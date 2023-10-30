import contextlib
import logging
import sys
import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

import aio_pika

if TYPE_CHECKING:
    import redis.asyncio

from .amqp import create_task_message
from .annotated_task import AnnotatedTask
from .backend import create_redis_connection_pool
from .config import DefaultConfig
from .result import AsyncResult as _AsyncResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CompleteTaskResources:
    context: Any
    redis_client_celery: Any


class Celery:
    def __init__(self) -> None:
        self.conf = DefaultConfig()
        self._tasks_registry: Dict[str, AnnotatedTask] = {}
        self._app_context: Any = None
        self._result_backend_connection_pool: Optional[
            "redis.asyncio.BlockingConnectionPool"
        ] = None
        self.rabbitmq_channel: Optional[aio_pika.RobustChannel] = None
        self._setup_app_context: Callable[
            [],
            contextlib.AbstractAsyncContextManager,
        ] = _setup_nothing

    def define_app_context(
        self,
        fn: Callable[[], contextlib.AbstractAsyncContextManager],
    ) -> None:
        self._setup_app_context = fn

    @property
    def context(self) -> Any:
        return self._app_context

    @property
    def result_backend(self) -> Optional["redis.asyncio.Redis"]:
        if self._result_backend_connection_pool is None:
            return None

        from redis.asyncio import Redis

        return Redis(connection_pool=self._result_backend_connection_pool)

    @contextlib.asynccontextmanager
    async def setup(self) -> AsyncIterator[None]:
        connection = await aio_pika.connect_robust(self.conf.broker_url)
        async with connection, connection.channel() as channel:
            self.rabbitmq_channel = channel
            try:
                if self.conf.result_backend is not None:
                    self._result_backend_connection_pool = create_redis_connection_pool(
                        url=self.conf.result_backend,
                        pool_size=self.conf.result_backend_connection_pool_size,
                    )
                async with self._setup_app_context() as context:
                    self._app_context = context
                    try:
                        yield
                    finally:
                        logger.warning("Shutting down application.")
            finally:
                if self._result_backend_connection_pool is not None:
                    await self._result_backend_connection_pool.disconnect()

    def task(self, *args, **opts):
        """Decorator to create a task class out of any callable."""

        def create_annotated_task(
            *,
            bind: bool = False,
            name: Optional[str] = None,
            ignore_result: Optional[bool] = None,
            max_retries: Optional[int] = None,
        ):
            def decorator(fn):
                if name is None:
                    task_name = _gen_task_name(fn.__name__, fn.__module__)
                else:
                    task_name = name
                annotated_task = AnnotatedTask(
                    fn=fn,
                    bind=bind,
                    ignore_result=ignore_result,
                    max_retries=max_retries,
                    task_name=task_name,
                    app=self,
                )
                self._tasks_registry[task_name] = annotated_task
                return annotated_task

            return decorator

        if len(args) == 1:
            if callable(args[0]):
                return create_annotated_task(**opts)(*args)
            raise TypeError("argument 1 to @task() must be a callable")
        if args:
            raise TypeError(
                f"@task() takes exactly 1 argument ({len(args) + len(opts)} given)",
            )
        return create_annotated_task(**opts)

    def _get_annotated_task(self, task_name: str) -> AnnotatedTask:
        return self._tasks_registry[task_name]

    def list_registered_task_names(self) -> List[str]:
        return sorted(self._tasks_registry)

    def AsyncResult(self, task_id: str) -> _AsyncResult:
        return _AsyncResult(task_id, app=self)

    async def send_task(  # noqa: PLR0913
        self,
        name: str,
        *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        countdown: Optional[int] = None,
        task_id: Optional[str] = None,
        priority: Optional[int] = None,
        queue: Optional[str] = None,
    ) -> _AsyncResult:
        task_id = task_id or str(uuid.uuid4())
        routing_key = queue or self.conf.task_default_queue
        logger.info("Sending task %s[%s] to queue %r ...", name, task_id, routing_key)
        await self._publish(
            create_task_message(
                task_id=task_id,
                task_name=name,
                args=args,
                kwargs=kwargs,
                priority=priority,
                countdown=countdown,
            ),
            routing_key=routing_key,
        )
        return self.AsyncResult(task_id)

    async def _publish(self, message: aio_pika.Message, routing_key: str) -> None:
        assert self.rabbitmq_channel is not None
        await self.rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=routing_key,
            timeout=60,
        )


@contextlib.asynccontextmanager
async def _setup_nothing() -> AsyncIterator[None]:
    yield None


def _gen_task_name(name: str, module_name: str) -> str:
    """Generate task name from name/module pair."""
    module_name = module_name or "__main__"
    module = sys.modules[module_name]
    if module is not None:
        module_name = module.__name__
    return ".".join(p for p in (module_name, name) if p)

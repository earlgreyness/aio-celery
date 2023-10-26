import contextlib
import sys
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Optional

import aio_pika

if TYPE_CHECKING:
    import redis.asyncio

from .amqp import create_task_message
from .annotated_task import AnnotatedTask
from .backend import create_redis_pool
from .config import DefaultConfig
from .result import AsyncResult


@dataclass(frozen=True)
class _CompleteTaskResources:
    context: Any
    redis_client_celery: Any


class Celery:
    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name
        self.conf = DefaultConfig()
        self._tasks_registry: dict[str, AnnotatedTask] = {}
        self._app_context: Any = None
        self._redis_pool_celery: Optional["redis.asyncio.BlockingConnectionPool"] = None
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

    @contextlib.asynccontextmanager
    async def setup(self) -> AsyncIterator[None]:
        connection = await aio_pika.connect_robust(self.conf.broker_url)
        async with connection, connection.channel() as channel:
            self.rabbitmq_channel = channel
            if self.conf.result_backend is not None:
                self._redis_pool_celery = create_redis_pool(
                    url=self.conf.result_backend,
                    pool_size=self.conf.redis_pool_size,
                )
            async with self._setup_app_context() as context:
                self._app_context = context
                yield

    @contextlib.asynccontextmanager
    async def _provide_task_resources(
        self,
    ) -> AsyncIterator[_CompleteTaskResources]:
        if self._redis_pool_celery is not None:
            import redis.asyncio

            async with redis.asyncio.Redis(
                connection_pool=self._redis_pool_celery,
            ) as redis_client:
                yield _CompleteTaskResources(self._app_context, redis_client)
        else:
            yield _CompleteTaskResources(self._app_context, None)

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
                    task_name = _gen_task_name(self, fn.__name__, fn.__module__)
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

    def list_registered_task_names(self) -> list[str]:
        return sorted(self._tasks_registry)

    def AsyncResult(self, task_id: str) -> AsyncResult:
        return AsyncResult(task_id, app=self)

    async def send_task(
        self,
        name: str,
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
        countdown: Optional[int] = None,
        task_id: Optional[str] = None,
        priority: Optional[int] = None,
        queue: Optional[str] = None,
    ) -> AsyncResult:
        task_id = task_id or str(uuid.uuid4())
        await self._publish(
            create_task_message(
                task_id=task_id,
                task_name=name,
                args=args,
                kwargs=kwargs,
                priority=priority,
                countdown=countdown,
            ),
            routing_key=queue or self.conf.task_default_queue,
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


def _gen_task_name(app: Celery, name: str, module_name: str) -> str:
    """Generate task name from name/module pair."""
    module_name = module_name or "__main__"
    module = sys.modules[module_name]
    if module is not None:
        module_name = module.__name__
    if module_name == "__main__" and app.name:
        return ".".join([app.name, name])
    return ".".join(p for p in (module_name, name) if p)

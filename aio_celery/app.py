from __future__ import annotations

import contextlib
import logging
import sys
import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
)

import aio_pika

from .amqp import create_task_message
from .annotated_task import AnnotatedTask
from .backend import create_redis_connection_pool
from .broker import Broker
from .config import DefaultConfig
from .context import CURRENT_ROOT_ID, CURRENT_TASK_ID
from .result import AsyncResult as _AsyncResult
from .utils import first_not_null

if TYPE_CHECKING:
    import redis.asyncio

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CompleteTaskResources:
    context: Any
    redis_client_celery: Any


class Celery:
    def __init__(self) -> None:
        self.conf = DefaultConfig()
        self._tasks_registry: dict[str, AnnotatedTask] = {}
        self._app_context: Any = None
        self._result_backend_connection_pool: (
            redis.asyncio.BlockingConnectionPool | None
        ) = None
        self._broker: Broker | None = None
        self._setup_app_context: Callable[
            [],
            contextlib.AbstractAsyncContextManager[Any],
        ] = _setup_nothing

    def define_app_context(
        self,
        fn: Callable[[], contextlib.AbstractAsyncContextManager[Any]],
    ) -> None:
        self._setup_app_context = fn

    @property
    def broker(self) -> Broker:
        if self._broker is None:
            msg = "This app has not been configured yet."
            raise RuntimeError(msg)
        return self._broker

    @broker.setter
    def broker(self, b: Broker) -> None:
        self._broker = b

    @property
    def context(self) -> Any:
        return self._app_context

    @property
    def result_backend(self) -> redis.asyncio.Redis[bytes] | None:
        if self._result_backend_connection_pool is None:
            return None

        from redis.asyncio import Redis

        return Redis(connection_pool=self._result_backend_connection_pool)

    @contextlib.asynccontextmanager
    async def setup(self) -> AsyncIterator[None]:
        connection = await aio_pika.connect_robust(self.conf.broker_url)
        async with connection, connection.channel() as channel:
            self.broker = Broker(
                rabbitmq_channel=channel,
                task_queue_max_priority=self.conf.task_queue_max_priority,
            )
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

    def task(  # noqa: PLR0913
        self,
        *args: Callable[..., Awaitable[Any]],
        bind: bool = False,
        name: str | None = None,
        ignore_result: bool | None = None,
        max_retries: int | None = 3,
        default_retry_delay: int = 180,
        queue: str | None = None,
        priority: int | None = None,
    ) -> AnnotatedTask | Callable[[Callable[..., Awaitable[Any]]], AnnotatedTask]:
        """Create a task class out of any callable."""

        def decorator(fn: Callable[..., Awaitable[Any]]) -> AnnotatedTask:
            if name is None:
                task_name = _gen_task_name(fn.__name__, fn.__module__)
            else:
                task_name = name
            annotated_task = AnnotatedTask(
                fn=fn,
                bind=bind,
                ignore_result=ignore_result,
                max_retries=max_retries,
                default_retry_delay=default_retry_delay,
                name=task_name,
                queue=queue,
                priority=priority,
                app=self,
            )
            self._tasks_registry[task_name] = annotated_task
            return annotated_task

        if not args:
            return decorator
        if len(args) == 1:
            func = args[0]
            if not callable(func):
                msg = "argument 1 to @task() must be a callable"
                raise TypeError(msg)
            return decorator(func)
        msg = "@task() takes exactly 1 argument"
        raise TypeError(msg)

    def get_annotated_task(self, task_name: str) -> AnnotatedTask:
        return self._tasks_registry[task_name]

    def list_registered_task_names(self) -> list[str]:
        return sorted(self._tasks_registry)

    def AsyncResult(self, task_id: str) -> _AsyncResult:  # noqa: N802
        return _AsyncResult(task_id, app=self)

    async def send_task(  # noqa: PLR0913
        self,
        name: str,
        *,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        countdown: float | None = None,
        task_id: str | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> _AsyncResult:
        if task_id is None:
            task_id = str(uuid.uuid4())
        await self.broker.publish_message(
            create_task_message(
                task_id=task_id,
                task_name=name,
                args=args,
                kwargs=kwargs,
                priority=priority,
                countdown=countdown,
                parent_id=CURRENT_TASK_ID.get(),
                root_id=CURRENT_ROOT_ID.get(),
            ),
            routing_key=first_not_null(queue, self.conf.task_default_queue),
        )
        return self.AsyncResult(task_id)


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

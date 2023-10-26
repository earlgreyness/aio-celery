import contextlib
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
    toolbox: Any
    redis_client_celery: Any


class Celery:
    def __init__(
        self,
        *,
        worker_context: Optional[
            Callable[[], contextlib.AbstractAsyncContextManager]
        ] = None,
        task_context: Optional[
            Callable[[Any], contextlib.AbstractAsyncContextManager]
        ] = None,
    ) -> None:
        self.conf = DefaultConfig()
        self._tasks_registry: dict[str, AnnotatedTask] = {}
        self._app_context = None
        self._redis_pool_celery: Optional["redis.asyncio.BlockingConnectionPool"] = None
        self.rabbitmq_channel: Optional[aio_pika.RobustChannel] = None
        self._initialize_worker_context = worker_context
        self._initialize_task_toolbox = task_context

    @contextlib.asynccontextmanager
    async def setup(self):
        connection = await aio_pika.connect_robust(self.conf.broker_url)
        async with connection, connection.channel() as channel:
            self.rabbitmq_channel = channel
            if self.conf.result_backend is not None:
                self._redis_pool_celery = create_redis_pool(
                    url=self.conf.result_backend,
                    pool_size=self.conf.redis_pool_size,
                )
            if self._initialize_worker_context is not None:
                async with self._initialize_worker_context() as context:
                    self._app_context = context
                    yield
            else:
                yield

    @contextlib.asynccontextmanager
    async def provide_complete_task_resources(
        self,
    ) -> AsyncIterator[_CompleteTaskResources]:
        if self._redis_pool_celery is not None:
            import redis.asyncio

            async with redis.asyncio.Redis(
                connection_pool=self._redis_pool_celery,
            ) as redis_client:
                if self._initialize_task_toolbox is not None:
                    async with self._initialize_task_toolbox(
                        self._app_context,
                    ) as toolbox:
                        yield _CompleteTaskResources(toolbox, redis_client)
                else:
                    yield _CompleteTaskResources({}, redis_client)
        elif self._initialize_task_toolbox is not None:
            async with self._initialize_task_toolbox(self._app_context) as toolbox:
                yield _CompleteTaskResources(toolbox, None)
        else:
            yield _CompleteTaskResources({}, None)

    def task(
        self,
        *,
        bind: bool = False,
        name: str,
        ignore_result: bool | None = None,
        max_retries: int | None = None,
    ):
        """Decorator to create a task class out of any callable."""
        def decorator(fn):
            annotated_task = AnnotatedTask(
                fn=fn,
                bind=bind,
                ignore_result=ignore_result,
                max_retries=max_retries,
                task_name=name,
                app=self,
            )
            self._tasks_registry[name] = annotated_task
            return annotated_task

        return decorator

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
            timeout=5,
        )

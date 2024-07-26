from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from .canvas import Signature
from .utils import first_not_null

if TYPE_CHECKING:
    from .app import Celery
    from .result import AsyncResult


@dataclass
class AnnotatedTask:
    fn: Callable[..., Awaitable[Any]]
    bind: bool
    ignore_result: bool | None
    max_retries: int | None
    default_retry_delay: int
    autoretry_for: tuple[type[Exception], ...]
    app: Celery
    name: str
    queue: str | None
    priority: int | None
    soft_time_limit: float | None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.bind:
            return self.fn(self, *args, **kwargs)
        return self.fn(*args, **kwargs)

    async def apply_async(self, **options: Any) -> AsyncResult:
        return await self.signature(**options).apply_async()

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return await self.apply_async(args=args, kwargs=kwargs)

    def signature(self, **options: Any) -> Signature:
        priority = first_not_null(options.get("priority"), self.priority)
        queue = first_not_null(options.get("queue"), self.queue)
        if priority is not None:
            options["priority"] = priority
        if queue is not None:
            options["queue"] = queue
        return Signature(self.name, **options)

    def s(self, *args: Any, **kwargs: Any) -> Signature:
        return self.signature(args=args, kwargs=kwargs)

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from .utils import first_not_null

if TYPE_CHECKING:
    from .app import Celery
    from .result import AsyncResult


@dataclass(frozen=True)
class AnnotatedTask:
    fn: Callable[..., Any]
    bind: bool
    ignore_result: bool | None
    max_retries: int | None
    default_retry_delay: int
    app: Celery
    name: str
    queue: str | None
    priority: int | None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.bind:
            return self.fn(None, *args, **kwargs)
        return self.fn(*args, **kwargs)

    async def apply_async(  # noqa: PLR0913
        self,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        *,
        task_id: str | None = None,
        countdown: float | None = None,
        priority: int | None = None,
        queue: str | None = None,
    ) -> AsyncResult:
        return await self.app.send_task(
            self.name,
            args=args,
            kwargs=kwargs,
            countdown=countdown,
            task_id=task_id,
            priority=first_not_null(
                priority,
                self.priority,
                self.app.conf.task_default_priority,
            ),
            queue=first_not_null(queue, self.queue),
        )

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return await self.apply_async(args=args, kwargs=kwargs)

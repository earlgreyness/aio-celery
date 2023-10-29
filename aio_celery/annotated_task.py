from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from .app import Celery
from .result import AsyncResult


@dataclass(frozen=True)
class AnnotatedTask:
    fn: Callable[..., Any]
    bind: bool
    ignore_result: Optional[bool]
    max_retries: Optional[int]
    app: "Celery"
    task_name: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.bind:
            return self.fn(None, *args, **kwargs)
        return self.fn(*args, **kwargs)

    async def apply_async(self, **options: Any) -> AsyncResult:
        options.setdefault("name", self.task_name)
        return await self.app.send_task(**options)

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return await self.apply_async(args=args, kwargs=kwargs)

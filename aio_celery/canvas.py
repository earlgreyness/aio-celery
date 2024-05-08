from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from ._state import get_current_app
from .utils import first_not_null

if TYPE_CHECKING:
    from .app import Celery
    from .result import AsyncResult


class Signature(dict[str, Any]):
    def __init__(  # noqa: PLR0913
        self,
        task: str,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        *,
        options: dict[str, Any] | None = None,
        app: Celery | None = None,
        **ex: Any,
    ) -> None:
        self._app = app
        super().__init__(
            task=task,
            args=tuple(args or ()),
            kwargs=kwargs or {},
            options=dict(options or {}, **ex),
            subtask_type=None,
            immutable=False,
        )

    @staticmethod
    def from_dict(d: dict[str, Any], *, app: Celery | None = None) -> Signature:
        return Signature(
            d["task"],
            args=d["args"],
            kwargs=d["kwargs"],
            options=d["options"],
            app=app,
        )

    def set(self, **options: Any) -> Signature:
        self["options"].update(options)
        return self

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
        sig: Signature = self
        chain_: list[dict[str, Any]] | None = None
        if self["task"] == "celery.chain":
            tasks = self["kwargs"]["tasks"]
            sig = tasks[0]
            chain_ = list(reversed(tasks[1:])) or None

        options = self["options"]

        return await (self._app or get_current_app()).send_task(
            sig["task"],
            args=first_not_null(args, sig["args"]),
            kwargs=first_not_null(kwargs, sig["kwargs"]),
            countdown=first_not_null(countdown, options.get("countdown")),
            task_id=first_not_null(task_id, options.get("task_id")),
            priority=first_not_null(priority, options.get("priority")),
            queue=first_not_null(queue, options.get("queue")),
            chain=chain_,
        )

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return await self.apply_async(args=args, kwargs=kwargs)


def signature(
    varies: str | Signature | dict[str, Any],
    *args: Any,
    **kwargs: Any,
) -> Signature:
    app = kwargs.get("app")
    if isinstance(varies, dict):
        if isinstance(varies, Signature):
            return copy.deepcopy(varies)
        return Signature.from_dict(varies, app=app)
    return Signature(varies, *args, **kwargs)


def chain(*tasks: Signature, **kwargs: Any) -> Signature:
    return Signature("celery.chain", args=(), kwargs={"tasks": tasks}, **kwargs)

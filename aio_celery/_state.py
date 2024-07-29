from __future__ import annotations

import dataclasses
import datetime
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import asyncio

    from .app import Celery


@dataclasses.dataclass
class RunningTask:
    asyncio_task: asyncio.Task[Any]
    task_id: str
    task_name: str
    state: Literal["SLEEPING", "SEMAPHORE", "RUNNING"]
    received: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    eta: str | None
    soft_time_limit: float | None
    retries: int
    started: str | None = None

    def serialize(self) -> dict[str, Any]:
        return {
            "name": self.task_name,
            "id": self.task_id,
            "state": self.state,
            "received": self.received,
            "started": self.started,
            "elapsed": (
                datetime.datetime.now().astimezone()
                - datetime.datetime.fromisoformat(self.started)
            ).total_seconds()
            if self.started is not None
            else None,
            "args": self.args,
            "kwargs": self.kwargs,
            "eta": self.eta,
            "soft_time_limit": self.soft_time_limit,
            "retries": self.retries,
            "stack": repr(self.asyncio_task.get_stack()),
        }


@dataclasses.dataclass
class _State:
    current_app: Celery | None = None
    running_tasks: dict[str, RunningTask] = dataclasses.field(default_factory=dict)


_STATE = _State()


def get_current_app() -> Celery:
    app = _STATE.current_app
    if app is None:
        msg = "No current app available"
        raise RuntimeError(msg)
    return app


def set_current_app(app: Celery | None) -> None:
    _STATE.current_app = app

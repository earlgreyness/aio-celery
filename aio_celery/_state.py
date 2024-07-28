from __future__ import annotations

import dataclasses
import datetime
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from .app import Celery


@dataclasses.dataclass
class RunningTask:
    task_id: str
    task_name: str

    received: str
    started: str | None = None

    args: tuple[Any, ...] | None = None
    kwargs: dict[str, Any] | None = None
    eta: str | None = None
    soft_time_limit: float | None = None

    state: Literal["SLEEPING", "SEMAPHORE", "RUNNING"] = "SLEEPING"

    def serialize(self) -> dict[str, Any]:
        now = datetime.datetime.now().astimezone()

        result = dataclasses.asdict(self)

        if self.started is not None:
            result["elapsed"] = (
                now - datetime.datetime.fromisoformat(self.started)
            ).total_seconds()
        else:
            result["elapsed"] = None

        return result


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

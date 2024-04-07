from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .app import Celery

_STATE: dict[str, Celery | None] = {
    "current_app": None,
}


def get_current_app() -> Celery:
    app = _STATE["current_app"]
    if app is None:
        msg = "No current app available"
        raise RuntimeError(msg)
    return app


def set_current_app(app: Celery | None) -> None:
    _STATE["current_app"] = app

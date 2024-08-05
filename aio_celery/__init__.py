from __future__ import annotations

from ._state import get_current_app as current_app
from .app import Celery, shared_task
from .canvas import chain, signature
from .task import Task

__version__ = "0.16.0"

__all__ = (
    "Celery",
    "Task",
    "chain",
    "current_app",
    "shared_task",
    "signature",
)

from __future__ import annotations

from contextvars import ContextVar

# Variables local to currently executing coroutine in the event loop
CURRENT_ROOT_ID: ContextVar[str | None] = ContextVar("current_root_id", default=None)
CURRENT_TASK_ID: ContextVar[str | None] = ContextVar("current_task_id", default=None)

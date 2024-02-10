from .app import Celery, shared_task
from .task import Task

__version__ = "0.8.2"

__all__ = (
    "Celery",
    "Task",
    "shared_task",
)

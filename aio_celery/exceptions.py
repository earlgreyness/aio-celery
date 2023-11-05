from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime

    from aio_pika import Message


class CeleryError(Exception):
    """Base class for all Celery errors."""


class MaxRetriesExceededError(CeleryError):
    """The tasks max restart limit has been exceeded."""


class TimeoutError(CeleryError):  # noqa: A001
    """The operation timed out."""


class Retry(CeleryError):  # noqa: N818
    def __init__(self, *, message: Message, delay: datetime.timedelta) -> None:
        self.message = message
        self._delay = delay
        super().__init__()

    def __str__(self) -> str:
        return f"{self.__class__.__name__} in {self._delay.total_seconds()}s"

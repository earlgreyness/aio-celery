from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, cast

from .exceptions import TimeoutError as CeleryTimeoutError

if TYPE_CHECKING:
    from .app import Celery


class AsyncResult:
    def __init__(
        self,
        id: str,  # noqa: A002
        *,
        app: Celery,
    ) -> None:
        self.id = id
        self._cache = None
        self.result_backend = app.result_backend

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.id}>"

    async def _get_task_meta(self) -> dict[str, Any]:
        if self.result_backend is None:
            msg = "No result backend is configured."
            raise RuntimeError(msg)
        if self._cache is None:
            value = await self.result_backend.get(f"celery-task-meta-{self.id}")
            if value is None:
                return {"result": None, "status": "PENDING"}
            self._cache = json.loads(value)
        return cast(Dict[str, Any], self._cache)

    @property
    async def result(self) -> Any:
        return (await self._get_task_meta())["result"]

    @property
    async def state(self) -> str:
        return str((await self._get_task_meta())["status"])

    async def get(
        self,
        timeout: float | None = None,
        interval: float = 0.5,
    ) -> Any:
        """Wait until task is ready, and return its result."""
        value = await self._get_task_meta()
        start = time.monotonic()
        while value == {"result": None, "status": "PENDING"}:
            await asyncio.sleep(interval)
            if timeout is not None and (time.monotonic() - start) > timeout:
                msg = "The operation timed out."
                raise CeleryTimeoutError(msg)
            value = await self._get_task_meta()
        return value["result"]

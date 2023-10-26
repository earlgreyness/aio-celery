import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .app import Celery


class AsyncResult:
    def __init__(self, id: str, *, app: "Celery") -> None:
        self.id = id
        self.task_id = id
        self._cache = None
        self._redis_pool = app._redis_pool_celery

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    async def _get_task_meta(self) -> dict[str, Any]:
        if self._redis_pool is None:
            raise RuntimeError("Result backend has not been enabled")
        if self._cache is None:
            import redis.asyncio

            async with redis.asyncio.Redis(connection_pool=self._redis_pool) as client:
                value = await client.get(f"celery-task-meta-{self.task_id}")
                if value is None:
                    return {"result": None, "status": "PENDING"}
                self._cache = json.loads(value)
        return self._cache

    @property
    async def result(self) -> Any:
        return (await self._get_task_meta())["result"]

    @property
    async def state(self) -> str:
        return (await self._get_task_meta())["status"]
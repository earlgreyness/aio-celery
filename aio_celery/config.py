from __future__ import annotations

import dataclasses
import datetime
from dataclasses import dataclass


@dataclass
class DefaultConfig:
    # Task execution settings
    task_ignore_result: bool = False
    task_soft_time_limit: int | None = None

    # Task result backend settings
    result_backend: str | None = None
    result_backend_connection_pool_size: int = 50
    result_expires: datetime.timedelta = datetime.timedelta(days=1)

    # Message Routing
    task_default_priority: int | None = None
    task_default_queue: str = "celery"
    task_queue_max_priority: int | None = None

    # Broker Settings
    broker_url: str = "amqp://guest:guest@localhost:5672//"

    # Worker
    worker_prefetch_multiplier: int = 4

    def update(self, **options: int | bool | str | datetime.timedelta | None) -> None:
        fields = {f.name for f in dataclasses.fields(self.__class__)}
        for k, v in options.items():
            if k not in fields:
                msg = f"Unknown configuration option: {k!r}"
                raise ValueError(msg)
            setattr(self, k, v)

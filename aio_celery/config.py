import dataclasses
import datetime
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class DefaultConfig:
    # Task execution settings
    task_ignore_result: bool = False
    task_soft_time_limit: Optional[int] = None

    # Task result backend settings
    result_backend: Optional[str] = None
    result_backend_connection_pool_size: int = 50
    result_expires: datetime.timedelta = datetime.timedelta(days=1)

    # Message Routing
    task_default_priority: int = 5
    task_default_queue: str = "celery"
    task_queue_max_priority: int = 5

    # Broker Settings
    broker_url: str = "amqp://guest:guest@localhost:5672//"

    def update(self, **options: Any) -> None:
        fields = {f.name for f in dataclasses.fields(self.__class__)}
        for k, v in options.items():
            if k not in fields:
                raise ValueError(f"Unknown configuration option: {k!r}")
            setattr(self, k, v)

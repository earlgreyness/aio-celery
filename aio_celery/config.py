from __future__ import annotations

import dataclasses
import datetime
from dataclasses import dataclass


@dataclass
class DefaultConfig:
    # Task execution settings
    task_ignore_result: bool = False
    task_soft_time_limit: float | None = None

    # Task result backend settings
    result_backend: str | None = None
    result_backend_connection_pool_size: int = 50
    result_expires: datetime.timedelta = datetime.timedelta(days=1)

    # Message Routing
    task_default_priority: int | None = None
    task_default_queue: str = "celery"
    task_queue_max_priority: int | None = None

    # Broker Settings
    broker_publish_timeout: float | None = None
    broker_url: str = "amqp://guest:guest@localhost:5672//"

    # Worker
    worker_consumer_tag_prefix: str = ""
    worker_prefetch_multiplier: int = 4
    # Raison d'être:
    # The asyncio.timeout() context manager is what transforms
    # the asyncio.CancelledError into a TimeoutError,
    # which means the TimeoutError can only be caught outside of the context manager.
    # (https://docs.python.org/3/library/asyncio-task.html#asyncio.timeout)
    worker_retry_task_on_asyncio_timeout_error: bool = False

    # Sentry SDK
    enable_sentry_sdk: bool = False

    # Inspection HTTP server
    inspection_http_server_is_enabled: bool = False
    inspection_http_server_host: str = "localhost"
    inspection_http_server_port: int = 1430

    # Intermittent Garbage Collection Behavior
    intermittent_gc_is_enabled: bool = False
    intermittent_gc_max_tasks_between_gc: int = 100
    intermittent_gc_max_interval_between_gc: datetime.timedelta = datetime.timedelta(
        hours=1,
    )

    def update(self, **options: int | bool | str | datetime.timedelta | None) -> None:
        fields = {f.name for f in dataclasses.fields(self.__class__)}
        for k, v in options.items():
            if k not in fields:
                msg = f"Unknown configuration option: {k!r}"
                raise ValueError(msg)
            setattr(self, k, v)

from __future__ import annotations

import copy
import dataclasses
import datetime
import json
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .amqp import create_task_message
from .exceptions import Retry
from .utils import first_not_null

if TYPE_CHECKING:
    from aio_pika import Message

    from .app import Celery
    from .request import Request

logger = logging.getLogger(__name__)


@dataclass
class Task:
    app: Celery
    request: Request
    _default_retry_delay: int = dataclasses.field(repr=False)

    @property
    def name(self) -> str:
        return self.request.task

    async def update_state(
        self,
        *,
        state: str,
        meta: dict[str, Any],
        _finalize: bool = False,
    ) -> None:
        result_backend = self.app.result_backend
        if result_backend is None:
            logger.debug("Result backend has not been enabled")
            return
        if _finalize:
            # do not update if result already in redis
            pass
        payload: dict[str, Any] = {
            "status": state,
            "result": meta,
            "traceback": None,
            "children": [],
            "date_done": None,
            "task_id": self.request.id,
        }
        if _finalize:
            payload["date_done"] = (
                datetime.datetime.utcnow().isoformat()  # noqa: DTZ003
            )
        if self.request.group is not None:
            payload["group_id"] = self.request.group
        if self.request.parent_id is not None:
            payload["parent_id"] = self.request.parent_id
        await result_backend.set(
            f"celery-task-meta-{self.request.id}",
            json.dumps(payload).encode(),
            ex=self.app.conf.result_expires,
        )

    def build_next_task_message(self, result: Any) -> tuple[Message | None, str]:
        if not self.request.chain:
            return None, ""
        new_chain = copy.deepcopy(self.request.chain)
        first = new_chain.pop()
        opts = first.get("options", {})
        new_task_id: str = first_not_null(opts.get("task_id"), str(uuid.uuid4()))
        new_priority: int = first_not_null(
            opts.get("priority"),
            self.app.conf.task_default_priority,
        )
        routing_key: str = first_not_null(
            opts.get("queue"),
            self.app.conf.task_default_queue,
        )
        reply_to: str = first_not_null(opts.get("reply_to"), "")
        return (
            create_task_message(
                task_id=new_task_id,
                task_name=first["task"],
                args=(result, *first["args"]),
                kwargs=first["kwargs"],
                priority=new_priority,
                parent_id=self.request.id,
                root_id=self.request.root_id,
                chain=new_chain,
                reply_to=reply_to,
            ),
            routing_key,
        )

    async def retry(self, *, countdown: float | None = None) -> None:
        delay = datetime.timedelta(
            seconds=countdown if countdown is not None else self._default_retry_delay,
        )
        raise Retry(
            message=self.request.build_retry_message(countdown=delay),
            delay=delay,
        )

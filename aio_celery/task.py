from __future__ import annotations

import copy
import dataclasses
import datetime
import json
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Tuple, cast

from .amqp import create_task_message
from .exceptions import Retry
from .utils import first_not_null

if TYPE_CHECKING:
    from aio_pika import IncomingMessage, Message

    from .annotated_task import AnnotatedTask
    from .app import Celery

logger = logging.getLogger(__name__)


@dataclass
class Task:
    message: IncomingMessage
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    options: dict[str, Any]
    chain: list[dict[str, Any]]

    app: Celery

    _default_retry_delay: int = dataclasses.field(repr=False)

    @property
    def task_id(self) -> str:
        return str(self.message.headers["id"])

    @property
    def name(self) -> str:
        return str(self.message.headers["task"])

    @property
    def eta(self) -> datetime.datetime | None:
        eta = self.message.headers["eta"]
        if eta is None:
            return None
        return datetime.datetime.fromisoformat(str(eta))

    @classmethod
    def from_raw_message(
        cls: type[Task],
        message: IncomingMessage,
        *,
        app: Celery,
        annotated_task: AnnotatedTask,
    ) -> Task:
        args, kwargs, options = json.loads(message.body)

        return cls(
            message=message,
            args=args,
            kwargs=kwargs,
            options=options,
            chain=options["chain"],
            app=app,
            _default_retry_delay=annotated_task.default_retry_delay,
        )

    def get_already_happened_retries(self) -> int:
        return cast(int, self.message.headers["retries"])

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
            "task_id": self.task_id,
        }
        if _finalize:
            payload["date_done"] = (
                datetime.datetime.utcnow().isoformat()  # noqa: DTZ003
            )
        if self.message.headers["group"] is not None:
            payload["group_id"] = self.message.headers["group"]
        if self.message.headers["parent_id"] is not None:
            payload["parent_id"] = self.message.headers["parent_id"]
        await result_backend.set(
            f"celery-task-meta-{self.task_id}",
            json.dumps(payload).encode(),
            ex=self.app.conf.result_expires,
        )

    def build_next_task_message(self, result: Any) -> tuple[Message | None, str]:
        if not self.chain:
            return None, ""
        new_chain = copy.deepcopy(self.chain)
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
        root_id = cast(Optional[str], self.message.headers["root_id"])
        return (
            create_task_message(
                task_id=new_task_id,
                task_name=first["task"],
                args=(result, *first["args"]),
                kwargs=first["kwargs"],
                priority=new_priority,
                parent_id=self.task_id,
                root_id=root_id,
                chain=new_chain,
                reply_to=reply_to,
            ),
            routing_key,
        )

    def _build_retry_message(
        self,
        *,
        countdown: datetime.timedelta | None,
    ) -> Message:
        message = copy.copy(self.message)
        if countdown is None:
            eta = None
        else:
            eta = (datetime.datetime.now().astimezone() + countdown).isoformat()
        message.headers.update(
            eta=eta,
            retries=self.get_already_happened_retries() + 1,
            parent_id=self.task_id,
        )
        return message

    async def retry(self, *, countdown: float | None = None) -> None:
        delay = datetime.timedelta(
            seconds=countdown if countdown is not None else self._default_retry_delay,
        )
        raise Retry(
            message=self._build_retry_message(countdown=delay),
            delay=delay,
        )

    @property
    def task_soft_time_limit(self) -> int | None:
        limits = cast(
            Tuple[Optional[int], Optional[int]],
            self.message.headers["timelimit"],
        )
        limit = limits[0]
        if limit is not None:
            return limit
        return self.app.conf.task_soft_time_limit

    @property
    def ignore_result(self) -> bool:
        return bool(self.message.headers["ignore_result"])

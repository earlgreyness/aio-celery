import copy
import dataclasses
import datetime
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from aio_pika import IncomingMessage, Message

from .amqp import create_task_message
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

    context: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def task_id(self) -> str:
        return str(self.message.headers["id"])

    @property
    def task_name(self) -> str:
        return str(self.message.headers["task"])

    @property
    def eta(self) -> Optional[datetime.datetime]:
        eta = self.message.headers["eta"]
        if eta is None:
            return None
        return datetime.datetime.fromisoformat(eta)

    @classmethod
    def from_raw_message(
        cls,
        message: IncomingMessage,
        *,
        app: Celery,
    ) -> "Task":
        args, kwargs, options = json.loads(message.body)
        callbacks, errbacks, chain, chord = (
            options["callbacks"],
            options["errbacks"],
            options["chain"],
            options["chord"],
        )
        assert callbacks is None
        assert errbacks is None
        assert chord is None
        return cls(
            message=message,
            args=args,
            kwargs=kwargs,
            options=options,
            chain=chain,
            app=app,
        )

    def get_already_happened_retries(self) -> int:
        retries = int(self.message.headers["retries"])
        assert retries >= 0
        return retries

    async def update_state(
        self,
        *,
        state: str,
        meta: dict[str, Any],
        _finalize: bool = False,
    ):
        result_backend = self.app.result_backend
        if result_backend is None:
            logger.debug("Result backend has not been enabled")
            return
        if _finalize:
            # do not update if result already in redis
            pass
        payload = {
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

    def _build_next_task_message(self, result: Any) -> tuple[Message, str]:
        assert self.chain
        new_chain = copy.deepcopy(self.chain)
        first = new_chain.pop()
        opts = first.get("options", {})
        new_task_id: str = opts.get("task_id") or str(uuid.uuid4())
        new_priority: int = opts.get("priority") or self.app.conf.task_default_priority
        routing_key: str = opts.get("queue") or self.app.conf.task_default_queue
        reply_to: str = opts.get("reply_to") or ""
        return (
            create_task_message(
                task_id=new_task_id,
                task_name=first["task"],
                args=(result, *first["args"]),
                kwargs=first["kwargs"],
                priority=new_priority,
                parent_id=self.task_id,
                chain=new_chain,
                reply_to=reply_to,
            ),
            routing_key,
        )

    def _build_retry_message(
        self,
        *,
        countdown: Optional[datetime.timedelta],
    ) -> Message:
        message = copy.copy(self.message)
        if countdown is None:
            eta = None
        else:
            eta = (datetime.datetime.now().astimezone() + countdown).isoformat()
        message.headers.update(
            eta=eta,
            retries=self.message.headers["retries"] + 1,
        )
        return message

    def retry(self, *, countdown: Optional[float] = None) -> None:
        if countdown is not None:
            countdown = datetime.timedelta(seconds=countdown)
        raise RetryRequested(message=self._build_retry_message(countdown=countdown))

    @property
    def task_soft_time_limit(self) -> Optional[int]:
        soft, _hard = self.message.headers["timelimit"]
        return soft or self.app.conf.task_soft_time_limit

    def _get_task_ignore_result(self) -> bool:
        return bool(self.message.headers["ignore_result"])


class RetryRequested(Exception):
    def __init__(self, *, message: Message) -> None:
        self.message = message
        super().__init__()

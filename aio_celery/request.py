from __future__ import annotations

import copy
import datetime
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from aio_pika import IncomingMessage, Message


@dataclass(frozen=True, kw_only=True)
class Request:
    message: IncomingMessage

    id: str  # noqa: A003
    task: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    retries: int
    eta: datetime.datetime | None
    parent_id: str | None
    group: str | None
    root_id: str | None
    ignore_result: bool
    timelimit: tuple[int | None, int | None]
    chain: list[dict[str, Any]]

    @classmethod
    def from_message(cls: type[Request], message: IncomingMessage) -> Request:
        headers = message.headers
        args, kwargs, options = json.loads(message.body)
        eta = headers["eta"]
        if eta is not None:
            eta = datetime.datetime.fromisoformat(str(eta))
        return cls(
            message=message,
            id=str(headers["id"]),
            args=args,
            kwargs=kwargs,
            task=cast(str, headers["task"]),
            retries=cast(int, headers["retries"]),
            eta=eta,
            parent_id=cast(Optional[str], headers["parent_id"]),
            group=cast(Optional[str], headers["group"]),
            root_id=cast(str, headers["root_id"]),
            ignore_result=bool(headers["ignore_result"]),
            timelimit=cast(tuple[Optional[int], Optional[int]], headers["timelimit"]),
            chain=options["chain"],
        )

    def build_retry_message(
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
            retries=self.retries + 1,
            parent_id=self.id,
        )
        return message

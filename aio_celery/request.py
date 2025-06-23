from __future__ import annotations

import copy
import datetime
import json
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Tuple, cast

if TYPE_CHECKING:
    from aio_pika import IncomingMessage, Message


@dataclass(frozen=True)
class Request:
    message: IncomingMessage

    id: str
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
        raw_eta = cast("Optional[str]", headers["eta"])
        eta: datetime.datetime | None
        if raw_eta is not None:
            if sys.version_info >= (3, 11):
                eta = datetime.datetime.fromisoformat(raw_eta)
            else:
                eta = datetime.datetime.strptime(raw_eta, "%Y-%m-%dT%H:%M:%S.%f%z")
            if eta.tzinfo is None or eta.tzinfo.utcoffset(eta) is None:
                msg = "eta must be a timezone aware datetime object"
                raise RuntimeError(msg)
        else:
            eta = None
        return cls(
            message=message,
            id=str(headers["id"]),
            args=args,
            kwargs=kwargs,
            task=cast("str", headers["task"]),
            retries=cast("int", headers["retries"]),
            eta=eta,
            parent_id=cast("Optional[str]", headers["parent_id"]),
            group=cast("Optional[str]", headers["group"]),
            root_id=cast("str", headers["root_id"]),
            ignore_result=bool(headers["ignore_result"]),
            timelimit=cast("Tuple[Optional[int], Optional[int]]", headers["timelimit"]),
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

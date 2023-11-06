from __future__ import annotations

import datetime
import json
from typing import Any

from aio_pika import DeliveryMode, Message


def create_task_message(  # noqa: PLR0913
    *,
    task_id: str,
    task_name: str,
    args: tuple[Any, ...] | None,
    kwargs: dict[str, Any] | None,
    priority: int | None,
    parent_id: str | None,
    root_id: str | None,
    chain: list[dict[str, Any]] | None = None,
    ignore_result: bool = False,
    countdown: float | None = None,
    reply_to: str = "",
) -> Message:
    args = args or ()
    kwargs = kwargs or {}
    if countdown is None:
        eta = None
    else:
        eta = (
            datetime.datetime.now().astimezone() + datetime.timedelta(seconds=countdown)
        ).isoformat()
    # https://docs.celeryq.dev/en/latest/internals/protocol.html
    headers: dict[str, Any] = {
        "argsrepr": repr(args),
        "eta": eta,
        "expires": None,
        "group": None,  # required
        "group_index": None,
        "id": task_id,  # required
        "ignore_result": ignore_result,
        "kwargsrepr": repr(kwargs),
        "lang": "py",  # required
        "origin": "unknown_pid@unknown_hostname",
        "parent_id": parent_id,  # required
        "retries": 0,
        "root_id": root_id if root_id is not None else task_id,  # required
        "shadow": None,
        "stamped_headers": None,
        "stamps": {},
        "task": task_name,  # required
        "timelimit": [None, None],
    }
    body: bytes = json.dumps(
        (
            args,
            kwargs,
            {
                "callbacks": None,
                "errbacks": None,
                "chain": chain,
                "chord": None,
            },
        ),
    ).encode()
    return Message(
        body=body,
        headers=headers,
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=DeliveryMode.PERSISTENT,
        priority=priority,
        correlation_id=task_id,
        reply_to=reply_to,  # optional
    )

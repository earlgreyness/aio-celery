import datetime
import json
from typing import Any, Optional

from aio_pika import DeliveryMode, Message


def create_task_message(
    *,
    task_id: str,
    task_name: str,
    args: tuple[Any, ...],
    kwargs: Any,
    priority: int,
    parent_id: Optional[str] = None,
    chain: Optional[list[dict[str, Any]]] = None,
    ignore_result: bool = False,
    countdown: Optional[int] = None,
) -> Message:
    kwargs = kwargs or {}
    if countdown is None:
        eta = None
    else:
        eta = (
            datetime.datetime.now().astimezone() + datetime.timedelta(seconds=countdown)
        ).isoformat()
    headers = {
        "argsrepr": repr(args),
        "eta": eta,
        "expires": None,
        "group": None,
        "group_index": None,
        "id": task_id,
        "ignore_result": ignore_result,
        "kwargsrepr": repr(kwargs),
        "lang": "py",
        "origin": "unknown_pid@unknown_hostname",
        "parent_id": parent_id,
        "retries": 0,
        "root_id": "",
        "shadow": None,
        "stamped_headers": None,
        "stamps": {},
        "task": task_name,
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
        reply_to="",
    )

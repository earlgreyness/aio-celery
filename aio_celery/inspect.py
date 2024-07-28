from __future__ import annotations

import asyncio
import email.utils
import json
import operator
import re
from typing import Any

from ._state import _STATE


def _get_running_tasks() -> list[dict[str, Any]]:
    return [t.serialize() for t in _STATE.running_tasks.values()]


def _collect_running_tasks_statistics() -> dict[str, Any]:
    running_tasks = sorted(
        _get_running_tasks(),
        key=operator.itemgetter("received"),
    )

    def s(t: asyncio.Task[Any]) -> tuple[int, str]:
        n = t.get_name()
        match = re.fullmatch(r"Task-(\d+)", n)
        if match:
            return int(match[1]), n
        return 0, n

    asyncio_tasks = sorted(asyncio.all_tasks(), key=s)

    celery_sleeping_tasks = [t for t in running_tasks if t["state"] == "SLEEPING"]
    celery_semaphore_tasks = [t for t in running_tasks if t["state"] == "SEMAPHORE"]
    celery_running_tasks = [t for t in running_tasks if t["state"] == "RUNNING"]

    for t in celery_running_tasks:
        del t["received"]
        del t["state"]
        del t["eta"]
        del t["started"]

    return {
        "stats": {
            "asyncio": len(asyncio_tasks),
            "sleeping": len(celery_sleeping_tasks),
            "semaphore": len(celery_semaphore_tasks),
            "running": len(celery_running_tasks),
        },
        "celery": celery_running_tasks,
        "asyncio": [repr(t) for t in asyncio_tasks],
    }


async def inspection_http_handle(
    reader: asyncio.StreamReader,  # noqa: ARG001
    writer: asyncio.StreamWriter,
) -> None:
    date: str = email.utils.formatdate(timeval=None, localtime=False, usegmt=True)

    content: bytes = (
        json.dumps(
            _collect_running_tasks_statistics(),
            ensure_ascii=False,
            indent=4,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf8")

    lines: list[bytes] = [
        b"HTTP/1.1 200 OK\r\n",
        b"Server: aio-celery\r\n",
        b"Date: " + date.encode("latin1") + b"\r\n",
        b"Content-Type: application/json; charset=utf-8\r\n",
        b"Content-Length: " + str(len(content)).encode("latin1") + b"\r\n",
        b"\r\n",
        content,
    ]

    for line in lines:
        writer.write(line)
    writer.write_eof()

    await writer.drain()

    writer.close()

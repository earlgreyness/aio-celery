import argparse
import asyncio
import asyncio.exceptions
import asyncio.timeouts
import contextlib
import datetime
import functools
import importlib
import logging
import os
import sys
import time
import urllib.parse
from typing import Any, List, Optional

import aiormq.exceptions
from aio_pika import IncomingMessage
from yarl import URL

from .annotated_task import AnnotatedTask
from .app import Celery
from .task import (
    RetryRequested,
    Task,
)

BANNER = """\
[config]
.> app:         {app}
.> transport:   {conninfo}
.> results:     {results}
.> concurrency: {concurrency}

[queues]
{queues}
"""

logger = logging.getLogger(__name__)


async def _sleep_if_necessary(task: Task) -> None:
    eta: Optional[datetime.datetime] = task.eta
    if eta is None:
        return
    now = datetime.datetime.now().astimezone()
    if eta > now:
        delta: datetime.timedelta = eta - now
        logger.info(
            "[%s] Sleeping for %s until task ETA %s ...",
            task.task_id,
            delta,
            eta,
        )
        await asyncio.sleep(delta.total_seconds())


async def _handle_task_result(
    task: Task,
    annotated_task: AnnotatedTask,
    result: Any,
) -> None:
    if (
        task.app.conf.task_ignore_result
        or annotated_task.ignore_result
        or task._get_task_ignore_result()
    ):
        return
    await task.update_state(state="SUCCESS", meta=result, _finalize=True)


async def _handle_task_retry(
    *,
    task: Task,
    annotated_task: AnnotatedTask,
    app: Celery,
    exc: RetryRequested,
    message: IncomingMessage,
) -> None:
    logger.info(
        "Task %s[%s] retry: %s",
        task.task_name,
        task.task_id,
        exc,
    )
    if (
        annotated_task.max_retries is not None
        and task.get_already_happened_retries() > annotated_task.max_retries
    ):
        raise RuntimeError(
            f"Max retries exceeded {annotated_task.max_retries=}",
        ) from exc
    await app.broker.publish_message(
        exc.message,
        routing_key=(message.routing_key or app.conf.task_default_queue),
    )


async def on_message_received(message: IncomingMessage, *, app: Celery) -> None:
    task_id = None
    task_name = None
    soft_time_limit = app.conf.task_soft_time_limit
    try:
        async with message.process(ignore_processed=True):
            task = Task.from_raw_message(message, app=app)
            task_id = task.task_id
            task_name = task.task_name
            logger.info("Task %s[%s] received", task.task_name, task.task_id)
            try:
                annotated_task = app._get_annotated_task(task.task_name)
            except KeyError:
                logger.exception(
                    "Received unregistered task of type %r\n"
                    "The message has been ignored and discarded.\n\n"
                    "Did you remember to import the module containing this task?\n"
                    "Or maybe you're using relative imports?\n\n"
                    "Please see\n"
                    "https://docs.celeryq.dev/en/latest/internals/protocol.html\n"
                    "for more information.\n\n"
                    "The full contents of the message body was:\n"
                    "%r (%db)\n\n"
                    "The full contents of the message headers:\n"
                    "%r\n\n"
                    "The delivery info for this task is:\n"
                    "%r",
                    task.task_name,
                    message.body,
                    len(message.body),
                    message.headers,
                    {
                        "consumer_tag": message.consumer_tag,
                        "delivery_tag": message.delivery_tag,
                        "redelivered": message.redelivered,
                        "exchange": message.exchange,
                        "routing_key": message.routing_key,
                    },
                )
                await message.reject()
                return

            await _sleep_if_necessary(task)

            timestamp_start = time.monotonic()
            args = (task, *task.args) if annotated_task.bind else task.args
            soft_time_limit = task.task_soft_time_limit
            try:
                async with asyncio.timeouts.timeout(soft_time_limit):
                    result = await annotated_task.fn(*args, **task.kwargs)
            except RetryRequested as exc:
                await _handle_task_retry(
                    task=task,
                    annotated_task=annotated_task,
                    app=app,
                    exc=exc,
                    message=message,
                )
            else:
                await _handle_task_result(
                    task=task,
                    annotated_task=annotated_task,
                    result=result,
                )
                if task.chain:
                    next_message, next_routing_key = task._build_next_task_message(
                        result=result,
                    )
                    await app.broker.publish_message(
                        next_message,
                        routing_key=next_routing_key,
                    )
                logger.info(
                    "Task %s[%s] succeeded in %.6fs: %r",
                    task.task_name,
                    task.task_id,
                    time.monotonic() - timestamp_start,
                    result,
                )
    except aiormq.exceptions.ChannelInvalidStateError:
        pass
    # This does not catch asyncio.exceptions.CancelledError since
    # the latter inherits directly from BaseException — not from Exception.
    # And that is okay for us. CancelledError is not meant to be caught.
    except Exception as err:
        if isinstance(err, asyncio.exceptions.TimeoutError):
            logger.warning(
                "Soft time limit (%ss) exceeded for %s[%s]",
                soft_time_limit,
                task_name,
                task_id,
            )
        logger.exception(
            "Task %s[%s] raised unexpected: %r",
            task_name,
            task_id,
            err,
        )


@contextlib.contextmanager
def _cwd_in_path():
    """Context adding the current working directory to sys.path."""
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            with contextlib.suppress(ValueError):
                sys.path.remove(cwd)


def _find_app_instance(locator: str) -> Celery:
    with _cwd_in_path():
        module_name, app_name = locator.rsplit(":", 1)
        module = importlib.import_module(module_name)
        app = getattr(module, app_name)
    if not isinstance(app, Celery):
        raise TypeError("app instance must inherit from Celery class")
    return app


def _print_intro(
    concurrency: int,
    queue_names: List[str],
    task_names: List[str],
    app: Celery,
) -> None:
    q = ".> " + "\n.> ".join(queue_names)
    t = "  . " + "\n  . ".join(task_names)

    def _normalize(u):
        if not u:
            return "disabled://"
        parts = urllib.parse.urlparse(u)
        if not parts.password:
            return u
        return str(URL(u).with_password("******"))

    banner = BANNER.format(
        app=repr(app),
        conninfo=_normalize(app.conf.broker_url),
        results=_normalize(app.conf.result_backend),
        concurrency=f"{concurrency} (asyncio)",
        queues=q,
    ).splitlines()
    print(
        "╔═══════════════════════╗\n"
        "║ AsyncIO Celery Worker ║\n"
        "╚═══════════════════════╝\n",
    )
    print("\n".join(banner) + "\n")
    print(f"[tasks]\n{t}\n")


async def run(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.getLevelName(args.loglevel),
        format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
    )
    logging.getLogger("aio_celery").setLevel(level=logging.getLevelName(args.loglevel))
    app = _find_app_instance(args.app)
    if not args.queues:
        queue_names = [app.conf.task_default_queue]
    else:
        queue_names = [q.strip() for q in args.queues.split(",")]

    _print_intro(
        args.concurrency,
        queue_names,
        app.list_registered_task_names(),
        app,
    )

    async with app.setup():
        await app.broker.set_qos(prefetch_count=args.concurrency)
        queues = [await app.broker.declare_queue(name) for name in queue_names]
        for queue in queues:
            await queue.consume(functools.partial(on_message_received, app=app))
        logger.info("Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()

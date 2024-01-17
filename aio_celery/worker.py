from __future__ import annotations

import asyncio
import asyncio.exceptions
import contextlib
import datetime
import functools
import importlib
import logging
import pathlib
import sys
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Awaitable, Iterator, Optional, cast

import aiormq.exceptions
from yarl import URL

from .app import Celery
from .context import CURRENT_ROOT_ID, CURRENT_TASK_ID
from .exceptions import MaxRetriesExceededError, Retry
from .request import Request
from .task import Task

if TYPE_CHECKING:
    import argparse

    from aio_pika import IncomingMessage

    from .annotated_task import AnnotatedTask

BANNER = """\
[config]
.> app:         {app}
.> transport:   {conninfo}
.> results:     {results}
.> concurrency: {concurrency}

[queues]
{queues}
"""

MAX_AMQP_PREFETCH_COUNT = 65535

logger = logging.getLogger(__name__)


async def _sleep_if_necessary(task: Task) -> None:
    eta: datetime.datetime | None = task.request.eta
    if eta is None:
        return
    now = datetime.datetime.now().astimezone()
    if eta > now:
        delta: datetime.timedelta = eta - now
        logger.info(
            "[%s] Sleeping for %s until task ETA %s ...",
            task.request.id,
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
        or task.request.ignore_result
    ):
        return
    await task.update_state(state="SUCCESS", meta=result, _finalize=True)


async def _handle_task_retry(
    *,
    task: Task,
    annotated_task: AnnotatedTask,
    app: Celery,
    exc: Retry,
    message: IncomingMessage,
) -> None:
    logger.info(
        "Task %s[%s] retry: %s",
        task.name,
        task.request.id,
        exc,
    )
    max_retries = annotated_task.max_retries
    if max_retries is not None and task.request.retries > max_retries:
        msg = (
            f"Can't retry {task.name}[{task.request.id}] args:{task.request.args}"
            f" kwargs:{task.request.kwargs} max_retries:{max_retries}"
        )
        raise MaxRetriesExceededError(msg)
    await app.broker.publish_message(
        exc.message,
        routing_key=(message.routing_key or app.conf.task_default_queue),
    )


async def on_message_received(
    message: IncomingMessage,
    *,
    app: Celery,
    semaphore: asyncio.Semaphore,
) -> None:
    task_id: str | None = None
    task_name: str | None = None
    try:
        async with message.process(ignore_processed=True):
            task_id = str(message.headers["id"])
            task_name = str(message.headers["task"])
            root_id = cast(Optional[str], message.headers["root_id"])
            logger.info("Task %s[%s] received", task_name, task_id)
            CURRENT_ROOT_ID.set(root_id)
            CURRENT_TASK_ID.set(task_id)
            try:
                annotated_task = app.get_annotated_task(task_name)
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
                    task_name,
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

            task = Task(
                app=app,
                request=Request.from_message(message),
                _default_retry_delay=annotated_task.default_retry_delay,
            )
            await _sleep_if_necessary(task)

            async with semaphore:
                timestamp_start = time.monotonic()
                args = (
                    (task, *task.request.args)
                    if annotated_task.bind
                    else task.request.args
                )
                soft_time_limit = (
                    task.request.timelimit[0] or app.conf.task_soft_time_limit
                )
                coro: Awaitable[Any] = annotated_task.fn(*args, **task.request.kwargs)
                try:
                    if soft_time_limit is None:
                        result = await coro
                    elif sys.version_info >= (3, 11):
                        async with asyncio.timeout(soft_time_limit):
                            result = await coro
                    else:
                        result = await asyncio.wait_for(coro, timeout=soft_time_limit)
                except Retry as exc:
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
                    next_message, next_routing_key = task.build_next_task_message(
                        result=result,
                    )
                    if next_message is not None:
                        await app.broker.publish_message(
                            next_message,
                            routing_key=next_routing_key,
                        )
                    logger.info(
                        "Task %s[%s] succeeded in %.6fs: %r",
                        task.name,
                        task.request.id,
                        time.monotonic() - timestamp_start,
                        result,
                    )
    except aiormq.exceptions.ChannelInvalidStateError:
        pass
    # This does not catch asyncio.exceptions.CancelledError since
    # the latter inherits directly from BaseException — not from Exception.
    # And that is okay for us. CancelledError is not meant to be caught.
    except Exception:
        logger.exception(
            "Task %s[%s] raised unexpected:",
            task_name,
            task_id,
        )
    finally:
        CURRENT_ROOT_ID.set(None)
        CURRENT_TASK_ID.set(None)


@contextlib.contextmanager
def _cwd_in_path() -> Iterator[None]:
    """Context adding the current working directory to sys.path."""
    cwd = str(pathlib.Path.cwd())
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield
        finally:
            with contextlib.suppress(ValueError):
                sys.path.remove(cwd)


def _find_app_instance(locator: str) -> Celery:
    with _cwd_in_path():
        module_name, app_name = locator.rsplit(":", 1)
        module = importlib.import_module(module_name)
        app = getattr(module, app_name)
    if not isinstance(app, Celery):
        msg = "app instance must inherit from Celery class"
        raise TypeError(msg)
    return app


def _print_intro(
    concurrency: int,
    queue_names: list[str],
    task_names: list[str],
    app: Celery,
) -> None:
    q = ".> " + "\n.> ".join(queue_names)
    t = "  . " + "\n  . ".join(task_names)

    def _normalize(u: str | None) -> str:
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
    print(  # noqa: T201
        "╔═══════════════════════╗\n"
        "║ AsyncIO Celery Worker ║\n"
        "╚═══════════════════════╝\n",
    )
    print("\n".join(banner) + "\n")  # noqa: T201
    print(f"[tasks]\n{t}\n")  # noqa: T201


async def run(args: argparse.Namespace) -> None:
    if args.configure_logging:
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
        semaphore = asyncio.Semaphore(args.concurrency)
        prefetch_count = min(
            app.conf.worker_prefetch_multiplier * args.concurrency,
            MAX_AMQP_PREFETCH_COUNT,
        )
        if prefetch_count > 0:
            await app.broker.set_qos(prefetch_count=prefetch_count)
        queues = [await app.broker.declare_queue(name) for name in queue_names]
        for queue in queues:
            await queue.consume(
                functools.partial(
                    on_message_received,
                    app=app,
                    semaphore=semaphore,
                ),
            )
        logger.info("Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()

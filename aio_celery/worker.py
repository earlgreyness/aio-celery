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

import aio_pika
import aiormq.exceptions
from aio_pika.abc import AbstractRobustChannel
from yarl import URL

from ._state import _STATE, RunningTask
from .app import Celery
from .context import CURRENT_ROOT_ID, CURRENT_TASK_ID
from .exceptions import MaxRetriesExceededError, Retry
from .inspect import inspection_http_handler
from .intermittent_gc import perform_gc_at_regular_intervals
from .request import Request
from .task import Task
from .utils import first_not_null

if TYPE_CHECKING:
    import argparse
    import types

    from aio_pika import IncomingMessage

    from .annotated_task import AnnotatedTask

sentry_sdk: types.ModuleType | None
try:
    sentry_sdk = importlib.import_module("sentry_sdk")
except ModuleNotFoundError:
    sentry_sdk = None

BANNER = """\
[config]
.> app:                 {app}
.> transport:           {conninfo}
.> results:             {results}
.> inspect http server: {inspect_http_server}
.> concurrency:         {concurrency}

[queues]
{queues}
"""

MAX_AMQP_PREFETCH_COUNT = 65535

logger = logging.getLogger(__name__)

_background_tasks = set()


def _iso_now() -> str:
    return datetime.datetime.now().astimezone().isoformat()


async def _sleep_if_necessary(task: Task) -> None:
    eta: datetime.datetime | None = task.request.eta
    if eta is None:
        return
    now = datetime.datetime.now().astimezone()
    if eta > now:
        delta: datetime.timedelta = eta - now
        logger.debug(
            "Task %s[%s] Sleeping for %s until task ETA %s ...",
            task.request.task,
            task.request.id,
            delta,
            eta,
        )
        await asyncio.sleep(delta.total_seconds())
        logger.debug(
            "Task %s[%s] Sleeping FINISHED",
            task.request.task,
            task.request.id,
        )


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
        pass
    else:
        await task.update_state(state="SUCCESS", meta=result, _finalize=True)
    next_message, next_routing_key = task.build_next_task_message(result=result)
    if next_message is not None:
        await task.app.broker.publish_message(
            next_message,
            routing_key=next_routing_key,
        )


async def _handle_task_retry(
    *,
    task: Task,
    annotated_task: AnnotatedTask,
    app: Celery,
    exc: Retry,
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
        routing_key=(task.request.message.routing_key or app.conf.task_default_queue),
    )


def _get_soft_time_limit(task: Task, annotated_task: AnnotatedTask) -> float | None:
    return cast(
        Optional[float],
        first_not_null(
            task.request.timelimit[0],
            annotated_task.soft_time_limit,
            task.app.conf.task_soft_time_limit,
        ),
    )


def _setup_sentry_context(task: Task, annotated_task: AnnotatedTask) -> None:
    if sentry_sdk is None:
        return
    sentry_sdk.set_tag("aio_celery_task_id", task.request.id)
    sentry_sdk.set_context(
        "aio-celery task",
        {
            "args": task.request.args,
            "kwargs": task.request.kwargs,
            "task_name": task.name,
            "soft_time_limit": _get_soft_time_limit(task, annotated_task),
        },
    )


async def _run_with_timeout(coro: Awaitable[Any], soft_time_limit: float | None) -> Any:
    if soft_time_limit is None:
        return await coro
    if sys.version_info >= (3, 11):
        # https://stackoverflow.com/a/47004339
        async with asyncio.timeout(soft_time_limit):
            return await coro
    return await asyncio.wait_for(coro, timeout=soft_time_limit)


def _log_unregistered_task(message: IncomingMessage) -> None:
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
        str(message.headers["task"]),
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


async def _execute_task(
    task: Task,
    annotated_task: AnnotatedTask,
    message: IncomingMessage,
) -> None:
    app = task.app
    args = (task, *task.request.args) if annotated_task.bind else task.request.args
    logger.debug(
        "Task %s[%s] started executing after acquiring semaphore",
        task.name,
        task.request.id,
    )
    timestamp_start = time.monotonic()
    try:
        try:
            result = await _run_with_timeout(
                annotated_task.fn(*args, **task.request.kwargs),
                _get_soft_time_limit(task, annotated_task),
            )
        except Retry:
            raise
        except Exception as exc:
            if isinstance(exc, annotated_task.autoretry_for) or (
                isinstance(exc, asyncio.TimeoutError)
                and app.conf.worker_retry_task_on_asyncio_timeout_error
            ):
                logger.warning(
                    "Task %s[%s] raised asyncio.TimeoutError, retrying...",
                    task.name,
                    task.request.id,
                )
                await task.retry()
            else:
                await message.reject()
            raise
    except Retry as exc:
        await _handle_task_retry(
            task=task,
            annotated_task=annotated_task,
            app=app,
            exc=exc,
        )
    else:
        await _handle_task_result(
            task=task,
            annotated_task=annotated_task,
            result=result,
        )
        logger.info(
            "Task %s[%s] succeeded in %.6fs: %r",
            task.name,
            task.request.id,
            time.monotonic() - timestamp_start,
            result,
        )


async def on_message_received(
    message: IncomingMessage,
    *,
    app: Celery,
    semaphore: asyncio.Semaphore,
    gc_is_paused: asyncio.Event,
) -> None:
    with contextlib.ExitStack() as stack:
        if app.conf.enable_sentry_sdk and sentry_sdk is not None:
            stack.enter_context(sentry_sdk.Hub(sentry_sdk.Hub.current))

        task_id = str(message.headers["id"])
        task_name = str(message.headers["task"])

        CURRENT_ROOT_ID.set(cast(Optional[str], message.headers["root_id"]))
        CURRENT_TASK_ID.set(task_id)

        logger.info("Task %s[%s] received", task_name, task_id)

        try:
            async with message.process(ignore_processed=True, requeue=True):
                try:
                    annotated_task = app.get_annotated_task(task_name)
                except KeyError:
                    _log_unregistered_task(message)
                    await message.reject()
                    return

                task = Task(
                    app=app,
                    request=Request.from_message(message),
                    _default_retry_delay=annotated_task.default_retry_delay,
                )

                running_task = RunningTask(
                    asyncio_task=asyncio.current_task(),  # type: ignore[arg-type]
                    task_id=task_id,
                    task_name=task_name,
                    received=_iso_now(),
                    state="SLEEPING",
                    args=task.request.args,
                    kwargs=task.request.kwargs,
                    eta=(
                        task.request.eta.isoformat()
                        if isinstance(task.request.eta, datetime.datetime)
                        else None
                    ),
                    soft_time_limit=_get_soft_time_limit(
                        task,
                        annotated_task,
                    ),
                    retries=task.request.retries,
                )
                _STATE.running_tasks[task_id] = running_task
                await asyncio.sleep(0)

                if app.conf.enable_sentry_sdk and sentry_sdk is not None:
                    _setup_sentry_context(task, annotated_task)

                logger.debug(
                    "Task %s[%s] args=%r kwargs=%r",
                    task.name,
                    task.request.id,
                    task.request.args,
                    task.request.kwargs,
                )

                await _sleep_if_necessary(task)
                running_task.state = "SEMAPHORE"

                async with semaphore:
                    running_task.state = "GC"
                    await gc_is_paused.wait()
                    running_task.state = "RUNNING"
                    running_task.started = _iso_now()
                    await _execute_task(task, annotated_task, message)
        except aiormq.exceptions.ChannelInvalidStateError:
            pass
        except Exception:
            # This does not catch asyncio.exceptions.CancelledError since
            # the latter inherits directly from BaseException — not from Exception.
            # And that is okay for us. CancelledError is not meant to be caught.
            logger.exception(
                "Task %s[%s] raised unexpected:",
                task_name,
                task_id,
            )
        finally:
            _STATE.amount_of_tasks_completed_after_last_gc_run += 1
            with contextlib.suppress(KeyError):
                del _STATE.running_tasks[task_id]
            await asyncio.sleep(0)
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

    if app.conf.inspection_http_server_is_enabled:
        inspect_http_server = (
            "http://"
            f"{app.conf.inspection_http_server_host}:"
            f"{app.conf.inspection_http_server_port}/"
        )
    else:
        inspect_http_server = "disabled://"

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
        inspect_http_server=inspect_http_server,
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

    gc_is_paused = asyncio.Event()
    gc_is_paused.set()
    if app.conf.intermittent_gc_is_enabled:
        gc_task = asyncio.create_task(
            perform_gc_at_regular_intervals(
                max_tasks_between_gc=app.conf.intermittent_gc_max_tasks_between_gc,
                max_interval_between_gc=app.conf.intermittent_gc_max_interval_between_gc,
                gc_is_paused=gc_is_paused,
            ),
        )
        _background_tasks.add(gc_task)
        gc_task.add_done_callback(_background_tasks.discard)

    async with app.setup():
        server = await asyncio.start_server(
            inspection_http_handler,
            host=app.conf.inspection_http_server_host,
            port=app.conf.inspection_http_server_port,
            start_serving=False,
        )

        semaphore = asyncio.Semaphore(args.concurrency)
        prefetch_count = min(
            app.conf.worker_prefetch_multiplier * args.concurrency,
            MAX_AMQP_PREFETCH_COUNT,
        )
        connection = await aio_pika.connect_robust(app.conf.broker_url)
        async with server, connection, connection.channel() as channel:
            if app.conf.inspection_http_server_is_enabled:
                await server.start_serving()
            if prefetch_count > 0:
                await channel.set_qos(
                    prefetch_count=prefetch_count,
                    timeout=app.conf.broker_publish_timeout,
                )
            queues = [
                await app.broker.declare_queue(
                    queue_name=name,
                    channel=cast(AbstractRobustChannel, channel),
                )
                for name in queue_names
            ]
            for queue in queues:
                await queue.consume(
                    functools.partial(
                        on_message_received,
                        app=app,
                        semaphore=semaphore,
                        gc_is_paused=gc_is_paused,
                    ),
                    timeout=app.conf.broker_publish_timeout,
                )
            logger.info("Waiting for messages. To exit press CTRL+C")
            await asyncio.Future()

import argparse
import asyncio
import asyncio.exceptions
import contextlib
import datetime
import functools
import importlib
import logging
import os
import sys
import urllib.parse
from typing import Any

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

LOGGER = logging.getLogger(__name__)


async def _sleep_if_necessary(task: Task) -> None:
    eta: datetime.datetime | None = task.eta
    if eta is None:
        return
    now = datetime.datetime.now().astimezone()
    if eta > now:
        LOGGER.info("[%s] Sleeping until %s...", task.task_id, eta)
        await asyncio.sleep((eta - now).total_seconds())


async def _handle_task_result(
    task: Task,
    annotated_task: AnnotatedTask,
    result: Any,
) -> None:
    LOGGER.info("[%s] Task result: %r", task.task_id, result)
    if (
        task.app.conf.task_ignore_result
        or annotated_task.ignore_result
        or task._get_task_ignore_result()
    ):
        return
    await task.update_state(state="SUCCESS", meta=result, _finalize=True)


async def on_message_received(message: IncomingMessage, *, app: Celery) -> None:
    task_id = None
    task_name = None
    task_args = None
    task_kwargs = None
    async with message.process():
        try:
            task = Task.from_raw_message(message, app=app)
            task_id = task.task_id
            task_name = task.task_name
            task_args = task.args
            task_kwargs = task.kwargs
            LOGGER.info(
                "[%s] Task received: %r, args=%r, kwargs=%r",
                task.task_id,
                task.task_name,
                task.args,
                task.kwargs,
            )
            annotated_task = app._get_annotated_task(task.task_name)
            await _sleep_if_necessary(task)
            try:
                async with app._provide_task_resources() as resources:
                    task.redis_client = resources.redis_client_celery
                    task.context = resources.context
                    args = (task, *task.args) if annotated_task.bind else task.args
                    async with asyncio.timeout(task.task_soft_time_limit):
                        result = await annotated_task.fn(*args, **task.kwargs)
            except RetryRequested as exc:
                if (
                    annotated_task.max_retries is not None
                    and task.get_already_happened_retries() > annotated_task.max_retries
                ):
                    raise RuntimeError(
                        f"Max retries exceeded {annotated_task.max_retries=}",
                    ) from exc
                await app._publish(exc.message, routing_key=message.routing_key)
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
                    await app._publish(next_message, routing_key=next_routing_key)
            LOGGER.info("[%s] Task completed", task.task_id)
        except Exception as err:
            LOGGER.exception(
                "[%s] Unexpected error happened: [%s] args=%r, kwargs=%r: repr=%r",
                task_id,
                task_name,
                task_args,
                task_kwargs,
                err,
            )
            raise


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
        return getattr(module, app_name)


def _print_intro(concurrency, queue_names, task_names, app):
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
        await app.rabbitmq_channel.set_qos(prefetch_count=args.concurrency)
        queues = [
            await app.rabbitmq_channel.declare_queue(
                name=name,
                durable=True,
                arguments={"x-max-priority": app.conf.task_queue_max_priority},
            )
            for name in queue_names
        ]
        for queue in queues:
            await queue.consume(
                functools.partial(on_message_received, app=app),
                timeout=None,  # this should be soft time limit
            )
        LOGGER.info(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()
from __future__ import annotations

import asyncio
import datetime
import gc
import logging
import time

from ._state import _STATE

logger = logging.getLogger(__name__)


async def perform_gc_at_regular_intervals(
    *,
    max_tasks_between_gc: int,
    max_interval_between_gc: datetime.timedelta,
    gc_is_paused: asyncio.Event,
) -> None:
    gc.disable()
    logger.warning("Automatic garbage collection disabled.")

    last_gc_time = datetime.datetime.now().astimezone()

    while True:
        await asyncio.sleep(0.5)
        if (
            _STATE.amount_of_tasks_completed_after_last_gc_run >= max_tasks_between_gc
            or (datetime.datetime.now().astimezone() - last_gc_time)
            > max_interval_between_gc
        ):
            gc_is_paused.clear()
            try:
                while True:
                    at_least_one_celery_task_is_running = any(
                        True
                        for t in _STATE.running_tasks.values()
                        if t.state == "RUNNING"
                    )
                    if not at_least_one_celery_task_is_running:
                        logger.debug("Collecting garbage manually...")
                        s = time.monotonic()
                        gc.collect()
                        logger.debug(
                            "Collecting garbage took %f seconds",
                            time.monotonic() - s,
                        )
                        last_gc_time = datetime.datetime.now().astimezone()
                        break
                    await asyncio.sleep(1)
            except Exception:
                gc.enable()
                raise
            finally:
                gc_is_paused.set()
                _STATE.amount_of_tasks_completed_after_last_gc_run = 0

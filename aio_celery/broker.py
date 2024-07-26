from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aio_pika
    from aio_pika.abc import (
        AbstractQueue,
        AbstractRobustChannel,
    )
    from pamqp.common import Arguments


class Broker:
    def __init__(
        self,
        *,
        broker_url: str,
        task_queue_max_priority: int | None,
        broker_publish_timeout: float | None,
        publishing_channel: AbstractRobustChannel,
    ) -> None:
        self._broker_url = broker_url
        self._task_queue_max_priority: int | None = task_queue_max_priority
        self._already_declared_queues: set[str] = set()
        self._broker_publish_timeout = broker_publish_timeout
        self._publishing_channel = publishing_channel

    async def publish_message(
        self,
        message: aio_pika.Message,
        *,
        routing_key: str,
    ) -> None:
        if routing_key not in self._already_declared_queues:
            await self.declare_queue(
                queue_name=routing_key,
                channel=self._publishing_channel,
            )

        await self._publishing_channel.default_exchange.publish(
            message,
            routing_key=routing_key,
            timeout=self._broker_publish_timeout,
        )

    async def declare_queue(
        self,
        *,
        queue_name: str,
        channel: AbstractRobustChannel,
    ) -> AbstractQueue:
        arguments: Arguments
        if self._task_queue_max_priority is not None:
            arguments = {"x-max-priority": self._task_queue_max_priority}
        else:
            arguments = None
        queue = await channel.declare_queue(
            name=queue_name,
            durable=True,
            arguments=arguments,
        )
        self._already_declared_queues.add(queue_name)
        return queue

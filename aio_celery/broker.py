from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aio_pika import Message
    from aio_pika.abc import AbstractChannel, AbstractQueue
    from pamqp.common import Arguments


class Broker:
    def __init__(
        self,
        *,
        rabbitmq_channel: AbstractChannel,
        task_queue_max_priority: int | None,
    ) -> None:
        self._rabbitmq_channel = rabbitmq_channel
        self._task_queue_max_priority: int | None = task_queue_max_priority
        self._already_declared_queues: set[str] = set()

    async def publish_message(
        self,
        message: Message,
        *,
        routing_key: str,
    ) -> None:
        if routing_key not in self._already_declared_queues:
            await self.declare_queue(routing_key)
            self._already_declared_queues.add(routing_key)
        await self._rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=routing_key,
            timeout=60,
        )

    async def declare_queue(self, queue_name: str) -> AbstractQueue:
        arguments: Arguments
        if self._task_queue_max_priority is not None:
            arguments = {"x-max-priority": self._task_queue_max_priority}
        else:
            arguments = None
        queue = await self._rabbitmq_channel.declare_queue(
            name=queue_name,
            durable=True,
            arguments=arguments,
        )
        self._already_declared_queues.add(queue_name)
        return queue

    async def set_qos(self, *, prefetch_count: int) -> None:
        await self._rabbitmq_channel.set_qos(prefetch_count=prefetch_count)

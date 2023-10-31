from typing import Set

from aio_pika import Message
from aio_pika.robust_channel import AbstractRobustChannel
from aio_pika.robust_queue import AbstractRobustQueue


class Broker:
    def __init__(
        self,
        *,
        rabbitmq_channel: AbstractRobustChannel,
        task_queue_max_priority: int,
    ) -> None:
        self._rabbitmq_channel = rabbitmq_channel
        self._task_queue_max_priority: int = task_queue_max_priority
        self._already_declared_queues: Set[str] = set()

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

    async def declare_queue(self, queue_name: str) -> AbstractRobustQueue:
        queue = await self._rabbitmq_channel.declare_queue(
            name=queue_name,
            durable=True,
            arguments={"x-max-priority": self._task_queue_max_priority},
        )
        self._already_declared_queues.add(queue_name)
        return queue

    async def set_qos(self, *, prefetch_count: int) -> None:
        await self._rabbitmq_channel.set_qos(prefetch_count=prefetch_count)

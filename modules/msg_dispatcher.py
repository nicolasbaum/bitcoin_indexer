import asyncio
from typing import Any, Dict

from loguru import logger


class Message:
    def __init__(self, queue_id: str, payload: Any):
        self.queue_id = queue_id
        self.payload = payload

    def __str__(self):
        return f"{self.queue_id}: {self.payload}"


class MessageDispatcher:
    """Reads messages from all queues and sends them to the DB queue."""

    def __init__(
        self, queues: Dict[str, asyncio.Queue], db_queue: asyncio.Queue[Message]
    ):
        self.msg_queues = queues
        self.db_queue = db_queue

    async def run(self):
        logger.info("MessageDispatcher started...")
        while True:
            try:
                elements = dict()
                for queue_name, queue in self.msg_queues.items():
                    elements[queue_name] = (
                        await queue.get() if not queue.empty() else None
                    )

                for queue_name, element in elements.items():
                    if element:
                        await self.db_queue.put(Message(queue_name, element))
                await asyncio.sleep(0.1)
                # for queue_name, queue in self.msg_queues.items():
                # logger.info(f"{queue_name} msgs: {queue.qsize()}")
            except asyncio.CancelledError:
                logger.info("MessageDispatcher cancelled.")
                raise
            except Exception as e:
                logger.error(f"MessageDispatcher error: {e}")

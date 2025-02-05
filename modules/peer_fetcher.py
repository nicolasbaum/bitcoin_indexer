import asyncio
from typing import Any, Mapping

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC


class PeerFetcher:
    """Fetches peer data and sends them to the queue."""

    def __init__(
        self,
        rpc: BitcoinRPC,
        db: AsyncIOMotorDatabase[Mapping[str, Any]],
        queue: asyncio.Queue,
    ):
        self.rpc = rpc
        self.db = db
        self.queue = queue

    async def run(self):
        logger.info("PeerFetcher started...")
        while True:
            try:
                peers = await self.rpc.call("getpeerinfo")
                if peers:
                    await self.queue.put(peers)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("PeerFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"PeerFetcher error: {e}")
                await asyncio.sleep(5)

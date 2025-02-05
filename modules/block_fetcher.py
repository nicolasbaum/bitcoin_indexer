import asyncio
from typing import Any, Mapping

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC


class BlockFetcher:
    """Fetches new blocks and sends them to the queue."""

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
        logger.info("BlockFetcher started...")
        while True:
            try:
                latest_block_height = await self.rpc.call("getblockcount")
                if latest_block_height is None:
                    await asyncio.sleep(0.1)
                    continue

                # Get the last processed block height from the system collection.
                last_stored = await self.db.system.find_one({"_id": "blocks"}) or {
                    "last_height": 0
                }
                start_height = last_stored["last_height"] + 1

                for height in range(start_height, latest_block_height + 1):
                    block_hash = await self.rpc.call("getblockhash", [height])
                    if block_hash is None:
                        continue
                    block_data = await self.rpc.call("getblock", [block_hash, 2])
                    if block_data:
                        await self.queue.put(block_data)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("BlockFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"BlockFetcher error: {e}")
                await asyncio.sleep(5)

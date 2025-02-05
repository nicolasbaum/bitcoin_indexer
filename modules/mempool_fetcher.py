import asyncio
from typing import Any, Mapping

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC


class MempoolFetcher:
    """Fetches mempool transactions and sends them to the queue."""

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
        logger.info("MempoolFetcher started...")
        while True:
            try:
                mempool_txids = await self.rpc.call("getrawmempool")
                if mempool_txids is None:
                    await asyncio.sleep(0.1)
                    continue

                last_stored = await self.db.system.find_one(
                    {"_id": "transactions"}
                ) or {"last_txid": None}
                last_txid = last_stored["last_txid"]

                for txid in mempool_txids[:50]:
                    if txid == last_txid:
                        break
                    tx_data = await self.rpc.call("getrawtransaction", [txid, True])
                    if tx_data:
                        await self.queue.put(tx_data)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("MempoolFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"MempoolFetcher error: {e}")
                await asyncio.sleep(5)

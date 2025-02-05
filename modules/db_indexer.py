import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Mapping

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC
from modules.msg_dispatcher import Message
from modules.tx_enricher import TxEnricher


class Indexer:
    """Handles writing data to MongoDB from the DB queue with logging."""

    def __init__(
        self,
        rpc: BitcoinRPC,
        db: AsyncIOMotorDatabase[Mapping[str, Any]],
        db_queue: asyncio.Queue,
    ):
        self.rpc = rpc
        self.db = db
        self.tx_enricher = TxEnricher(rpc, self.db)
        self.db_queue = db_queue

    async def run(self):
        while True:
            try:
                start_time = time.time_ns()
                msg: Message = await self.db_queue.get()

                if msg.queue_id == "blocks":
                    await self.db.blocks.update_one(
                        {"hash": msg.payload["hash"]},
                        {"$set": msg.payload},
                        upsert=True,
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.info(
                        f"🟢 Indexed block {msg.payload['height']} | "
                        f"{datetime.fromtimestamp(msg.payload['time'])} in {duration:.2f}ms"
                    )
                    await self.update_last_processed(
                        "blocks", {"last_height": msg.payload["height"]}
                    )

                    block_time = msg.payload.get("time")
                    block_hash = msg.payload["hash"]
                    block_height = msg.payload["height"]

                    for tx in msg.payload["tx"]:
                        enriched_tx = await self.tx_enricher.enrich_transaction(
                            tx,
                            block_time=block_time,
                            block_hash=block_hash,
                            block_height=block_height,
                        )
                        await self.db.transactions.update_one(
                            {"txid": enriched_tx["txid"]},
                            {"$set": enriched_tx},
                            upsert=True,
                        )
                        # Invalidate cache for the updated transaction.
                        await self.tx_enricher.invalidate_previous_transaction_cache(
                            enriched_tx["txid"]
                        )

                        for vout in enriched_tx.get("vout", []):
                            value = vout.get("value", 0.0)
                            addresses = vout.get("scriptPubKey", {}).get(
                                "addresses", []
                            )
                            for address in addresses:
                                await self.db.balances.update_one(
                                    {"address": address},
                                    {
                                        "$inc": {"balance": value},
                                        "$set": {
                                            "block_time": block_time,
                                            "block_height": block_height,
                                        },
                                    },
                                    upsert=True,
                                )

                        for vin in enriched_tx.get("vin", []):
                            value = vin.get("value", 0.0)
                            addresses = vin.get("addresses", [])
                            for address in addresses:
                                await self.db.balances.update_one(
                                    {"address": address},
                                    {
                                        "$inc": {"balance": -value},
                                        "$set": {
                                            "block_time": block_time,
                                            "block_height": block_height,
                                        },
                                    },
                                    upsert=True,
                                )

                    await self.update_last_processed(
                        "blocks", {"last_height": msg.payload["height"]}
                    )

                elif msg.queue_id == "transactions":
                    enriched_tx = await self.tx_enricher.enrich_transaction(msg.payload)
                    await self.db.transactions.update_one(
                        {"txid": enriched_tx["txid"]},
                        {"$set": enriched_tx},
                        upsert=True,
                    )
                    # Invalidate the cache for the updated transaction.
                    await self.tx_enricher.invalidate_previous_transaction_cache(
                        enriched_tx["txid"]
                    )
                    await self.update_last_processed(
                        "transactions", {"last_txid": msg.payload["txid"]}
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.debug(
                        f"🔵 New Transaction Indexed: TXID {msg.payload['txid']} in {duration:.2f}ms"
                    )

                elif msg.queue_id == "peers":
                    await self.db.peers.delete_many({})
                    await self.db.peers.insert_many(msg.payload)
                    await self.update_last_processed(
                        "peers", {"last_updated": datetime.now(timezone.utc)}
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.debug(
                        f"🟣 Peers Updated: {len(msg.payload)} connected peers in {duration:.2f}ms"
                    )
                    await self.update_last_processed(
                        "peers", {"last_updated": datetime.now(timezone.utc)}
                    )

                self.db_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Indexer cancelled.")
                raise
            except Exception as e:
                logger.error(f"❌ Indexer Error: {e}")

    async def update_last_processed(self, key: str, value: dict):
        """Updates the last processed document in the system collection."""
        await self.db.system.update_one({"_id": key}, {"$set": value}, upsert=True)
        logger.debug(f"✅ Updated system tracker: {key} -> {value}")

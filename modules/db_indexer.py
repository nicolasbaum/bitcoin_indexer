import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Mapping

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import UpdateOne

from modules.bitcoin_rpc import BitcoinRPC
from modules.msg_dispatcher import Message
from modules.tx_enricher import TxEnricher


class Indexer:
    """Handles writing data to MongoDB from the DB queue with logging."""

    def __init__(
        self,
        rpc: BitcoinRPC,
        db: AsyncIOMotorDatabase[Mapping[str, Any]],
        db_queue: asyncio.Queue[Message],
    ):
        self.rpc = rpc
        self.db = db
        self.tx_enricher = TxEnricher(rpc, db)
        self.db_queue = db_queue

        self.process_functions = {
            "blocks": self.process_block,
            "transactions": self.process_transaction,
            "peers": self.process_peers,
        }

        self._update_counters = defaultdict(int)

    async def run(self):
        logger.info("DB Indexer started...")
        while True:
            try:
                start_time = time.time_ns()
                msg: Message = await self.db_queue.get()

                process_function = self.process_functions.get(msg.queue_id)
                if not process_function:
                    logger.error(f"❌ Invalid queue ID: {msg.queue_id}")
                    continue

                await process_function(msg.payload)
                duration = (time.time_ns() - start_time) / 1e6
                logger.debug(
                    f"🟢 Processed db entry for {msg.queue_id} in {duration:.2f}ms"
                )
                self.db_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Indexer cancelled.")
                raise
            except Exception as e:
                logger.error(f"❌ Indexer Error: {e}")

    async def process_block(self, block_data: dict):
        """Process a block and its transactions with bulk operations."""
        try:
            # Update block document
            await self.db.blocks.update_one(
                {"hash": block_data["hash"]},
                {"$set": block_data},
                upsert=True,
            )

            # Process balances with bulk operations
            balance_updates = []
            for tx in block_data["tx"]:
                enriched_tx = await self.tx_enricher.enrich_transaction(
                    tx,
                    block_time=block_data["time"],
                    block_hash=block_data["hash"],
                    block_height=block_data["height"],
                )

                await self.db.transactions.update_one(
                    {"txid": enriched_tx["txid"]},
                    {"$set": enriched_tx},
                    upsert=True,
                )

                # Invalidate cache for the updated transaction
                await self.tx_enricher.invalidate_previous_transaction_cache(
                    enriched_tx["txid"]
                )

                # Process vouts for balance updates
                for vout in enriched_tx.get("vout", []):
                    value = vout.get("value", 0.0)
                    addresses = vout.get("scriptPubKey", {}).get("addresses", [])
                    for address in addresses:
                        balance_updates.append(
                            UpdateOne(
                                {"address": address},
                                {
                                    "$inc": {"balance": value},
                                    "$set": {
                                        "block_time": block_data["time"],
                                        "block_height": block_data["height"],
                                    },
                                },
                                upsert=True,
                            )
                        )

                # Process vins for balance updates
                for vin in enriched_tx.get("vin", []):
                    value = vin.get("value", 0.0)
                    addresses = vin.get("addresses", [])
                    for address in addresses:
                        balance_updates.append(
                            UpdateOne(
                                {"address": address},
                                {
                                    "$inc": {"balance": -value},
                                    "$set": {
                                        "block_time": block_data["time"],
                                        "block_height": block_data["height"],
                                    },
                                },
                                upsert=True,
                            )
                        )

            # Execute balance bulk update
            if balance_updates:
                _ = await self.db.balances.bulk_write(balance_updates, ordered=False)
                logger.debug(f"📊 Bulk updated {len(balance_updates)} balances")

            # Update last processed block height
            await self.update_last_processed(
                "blocks", {"last_height": block_data["height"]}
            )

        except Exception as e:
            logger.error(f"Error processing block {block_data['height']}: {e}")
            raise

    async def process_transaction(self, tx_data: dict):
        """Process a single transaction."""
        try:
            enriched_tx = await self.tx_enricher.enrich_transaction(tx_data)
            await self.db.transactions.update_one(
                {"txid": enriched_tx["txid"]},
                {"$set": enriched_tx},
                upsert=True,
            )
            await self.tx_enricher.invalidate_previous_transaction_cache(
                enriched_tx["txid"]
            )
            await self.update_last_processed(
                "transactions", {"last_txid": tx_data["txid"]}
            )
        except Exception as e:
            logger.error(f"Error processing transaction {tx_data['txid']}: {e}")
            raise

    async def process_peers(self, peers: list):
        """Process peer data with bulk operations."""
        try:
            # Clear existing peers
            await self.db.peers.delete_many({})
            # Bulk insert new peers
            await self.db.peers.insert_many(peers)
            await self.update_last_processed(
                "peers", {"last_updated": datetime.now(timezone.utc)}
            )
        except Exception as e:
            logger.error(f"Error processing peers: {e}")
            raise

    async def update_last_processed(self, key: str, value: dict):
        """Updates the last processed document in the system collection."""
        try:
            _ = await self.db.system.update_one(
                {"_id": key}, {"$set": value}, upsert=True
            )
            self._update_counters[key] = (self._update_counters[key] + 1) % 1000
            if self._update_counters[key] == 0:
                logger.info(f"✅ Updated system tracker: {key} -> {value}")
        except Exception as e:
            logger.error(f"Error updating system tracker for {key}: {e}")
            raise

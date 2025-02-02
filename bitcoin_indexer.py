import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Union

import aiohttp
import pymongo
from loguru import logger
from pymongo.errors import DuplicateKeyError

from db_initializer import setup_collections

# Load environment variables
RPC_USER = os.getenv("RPC_USER", "__cookie__")
RPC_PASSWORD = os.getenv("RPC_PASSWORD", "")
RPC_URL = os.getenv("RPC_URL", "http://umbrel.local:8332")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")

# MongoDB Connection
client = pymongo.MongoClient(MONGO_URL)
db = client["bitcoin_db"]

# Queues for async processing
block_queue = asyncio.Queue()
mempool_queue = asyncio.Queue()
peer_queue = asyncio.Queue()
db_queue = asyncio.Queue()

# Set up logging
if not logger._core.handlers:
    logger.add("logs/bitcoin_indexer.log", rotation="10MB", level="INFO")
logger.info("🚀 Bitcoin Indexer Started!")


def update_last_processed(key: str, value: dict):
    """Updates the last processed document in the system collection."""
    db.system.update_one({"_id": key}, {"$set": value}, upsert=True)
    logger.info(f"✅ Updated system tracker: {key} -> {value}")


class BitcoinRPC:
    """Handles RPC calls with automatic retries and error handling."""

    def __init__(self, user: str, password: str, url: str, max_retries=5):
        self.auth = aiohttp.BasicAuth(user, password)
        self.url = url
        self.max_retries = max_retries

    async def call(self, method: str, params=None) -> Any:
        """Generic function to make async RPC calls with retries."""
        params = params or []
        payload = json.dumps(
            {"jsonrpc": "1.0", "id": method, "method": method, "params": params}
        )

        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.url,
                        auth=self.auth,
                        data=payload,
                        headers={"content-type": "application/json"},
                        timeout=10,
                    ) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            return result.get("result")
                        else:
                            logger.error(f"RPC error {resp.status} on method {method}")
                            raise Exception(f"RPC error {resp.status}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"RPC call {method} failed (attempt {attempt + 1}): {e}")
                await asyncio.sleep(2**attempt)  # Exponential backoff

        logger.error(f"Max retries reached for RPC method {method}")
        return None


class BlockFetcher:
    """Fetches new blocks and sends them to the queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        """Continuously fetch new blocks and send to queue."""
        while True:
            latest_block_height = await self.rpc.call("getblockcount")
            if latest_block_height is None:
                await asyncio.sleep(10)
                continue

            last_stored = db.system.find_one({"_id": "blocks"}) or {"last_height": 0}
            start_height = last_stored["last_height"] + 1

            for height in range(start_height, latest_block_height + 1):
                block_hash = await self.rpc.call("getblockhash", [height])
                if block_hash is None:
                    continue

                # getblock with verbosity=2 => includes full tx data
                block_data = await self.rpc.call("getblock", [block_hash, 2])
                if block_data:
                    await block_queue.put(block_data)
                    update_last_processed("blocks", {"last_height": height})

            await asyncio.sleep(10)


class MempoolFetcher:
    """Fetches mempool transactions and sends them to queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            mempool_txids = await self.rpc.call("getrawmempool")
            if mempool_txids is None:
                await asyncio.sleep(5)
                continue

            last_stored = db.system.find_one({"_id": "transactions"}) or {
                "last_txid": None
            }
            last_txid = last_stored["last_txid"]

            # You can limit to e.g. 50 TXIDs
            for txid in mempool_txids[:50]:
                if txid == last_txid:
                    break

                tx_data = await self.rpc.call("getrawtransaction", [txid, True])
                if tx_data:
                    await mempool_queue.put(tx_data)
                    update_last_processed("transactions", {"last_txid": txid})

            await asyncio.sleep(5)


class PeerFetcher:
    """Fetches peer data and sends it to queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            peers = await self.rpc.call("getpeerinfo")
            if peers:
                await peer_queue.put(peers)
                update_last_processed(
                    "peers", {"last_updated": datetime.now(timezone.utc)}
                )

            await asyncio.sleep(30)


class MessageListener:
    """Reads messages from all queues and sends them to the DB queue."""

    async def run(self):
        while True:
            try:
                block = await block_queue.get() if not block_queue.empty() else None
                tx = await mempool_queue.get() if not mempool_queue.empty() else None
                peers = await peer_queue.get() if not peer_queue.empty() else None

                if block:
                    await db_queue.put(("blocks", block))
                if tx:
                    await db_queue.put(("transactions", tx))
                if peers:
                    await db_queue.put(("peers", peers))

                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"MessageListener error: {e}")


# --------------------------------------------------------------------
# Recursive removal of any "_id" fields to avoid immutable _id errors
# --------------------------------------------------------------------
def remove_id_recursively(obj: Union[dict, list]):
    """Recursively delete '_id' key from any nested dict/list structure."""
    if isinstance(obj, dict):
        # Remove _id if present
        obj.pop("_id", None)
        # Recurse into values
        for val in obj.values():
            remove_id_recursively(val)
    elif isinstance(obj, list):
        for item in obj:
            remove_id_recursively(item)


def enrich_transaction(
    tx: dict,
    block_time: int = None,
    block_hash: str = None,
    block_height: int = None,
) -> dict:
    """
    Enrich a transaction with addresses, fees, input totals, etc.
    Looks up the previous TX outputs to identify input addresses + amounts.
    """
    if block_hash is not None:
        tx["block_hash"] = block_hash
    if block_height is not None:
        tx["block_height"] = block_height
    if block_time is not None:
        tx["block_time"] = block_time  # Unix timestamp

    all_addresses = set()
    input_total = 0.0

    # Parse vin for addresses
    if "vin" in tx:
        for vin_item in tx["vin"]:
            vin_item.setdefault("addresses", [])
            prev_txid = vin_item.get("txid")
            if prev_txid:
                prev_index = vin_item.get("vout", -1)
                prev_tx_doc = db.transactions.find_one({"txid": prev_txid})
                if prev_tx_doc and "vout" in prev_tx_doc:
                    try:
                        spent_out = prev_tx_doc["vout"][prev_index]
                        addrs = spent_out.get("scriptPubKey", {}).get("addresses", [])
                        vin_item["addresses"] = addrs
                        input_total += spent_out.get("value", 0.0)
                        for a in addrs:
                            all_addresses.add(a)
                    except (IndexError, KeyError):
                        pass

    output_total = 0.0
    if "vout" in tx:
        for vout_item in tx["vout"]:
            val = vout_item.get("value", 0.0)
            output_total += val
            addrs = vout_item.get("scriptPubKey", {}).get("addresses", [])
            for a in addrs:
                all_addresses.add(a)

    tx["input_total"] = input_total
    tx["output_total"] = output_total
    tx["fee"] = round(input_total - output_total, 8)
    tx["all_addresses"] = list(all_addresses)

    return tx


class Indexer:
    """Handles writing data to MongoDB from the DB queue with logging"""

    async def run(self):
        while True:
            try:
                collection, data = await db_queue.get()

                if collection == "blocks":
                    # Remove any _id fields in the block doc
                    remove_id_recursively(data)

                    # Insert or replace block doc
                    try:
                        db.blocks.insert_one(data)
                    except DuplicateKeyError:
                        db.blocks.replace_one({"hash": data["hash"]}, data)

                    logger.info(
                        f"🟢 New Block Indexed: Height {data['height']} | Hash {data['hash']}"
                    )

                    # For each TX in the block, do the same
                    block_time = data.get("time")
                    block_hash = data["hash"]
                    block_height = data["height"]

                    for tx in data["tx"]:
                        enriched_tx = enrich_transaction(
                            tx,
                            block_time=block_time,
                            block_hash=block_hash,
                            block_height=block_height,
                        )
                        # Remove all _id from sub-docs
                        remove_id_recursively(enriched_tx)

                        db.transactions.update_one(
                            {"txid": enriched_tx["txid"]},
                            {"$set": enriched_tx},
                            upsert=True,
                        )

                    update_last_processed("blocks", {"last_height": data["height"]})

                elif collection == "transactions":
                    # Mempool TX
                    enriched_tx = enrich_transaction(data)
                    remove_id_recursively(enriched_tx)

                    # Try inserting; if there's a duplicate key, replace existing
                    try:
                        db.transactions.insert_one(enriched_tx)
                    except DuplicateKeyError:
                        db.transactions.replace_one(
                            {"txid": enriched_tx["txid"]}, enriched_tx
                        )

                    update_last_processed("transactions", {"last_txid": data["txid"]})
                    logger.info(f"🔵 New Transaction Indexed: TXID {data['txid']}")

                elif collection == "peers":
                    # For peers, we can simply remove & re-insert
                    db.peers.delete_many({})
                    db.peers.insert_many(data)
                    update_last_processed("peers", {"last_updated": datetime.utcnow()})
                    logger.info(f"🟣 Peers Updated: {len(data)} connected peers")

                db_queue.task_done()

            except Exception as e:
                logger.error(f"❌ Indexer Error: {e}")


async def main():
    setup_collections()
    rpc = BitcoinRPC(RPC_USER, RPC_PASSWORD, RPC_URL)

    block_fetcher = BlockFetcher(rpc)
    mempool_fetcher = MempoolFetcher(rpc)
    peer_fetcher = PeerFetcher(rpc)
    message_listener = MessageListener()
    indexer = Indexer()

    await asyncio.gather(
        block_fetcher.run(),
        mempool_fetcher.run(),
        peer_fetcher.run(),
        message_listener.run(),
        indexer.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())

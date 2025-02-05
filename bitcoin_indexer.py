import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from aiocache import Cache, cached  # <-- Import aiocache components
from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient  # Use Motor for async MongoDB

from db_initializer import setup_collections

load_dotenv()

# Load environment variables
RPC_USER = os.getenv("RPC_USER", "__cookie__")
RPC_PASSWORD = os.getenv("RPC_PASSWORD", "")
RPC_URL = os.getenv("RPC_URL", "http://umbrel.local:8332")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")

# MongoDB Connection using Motor (async)
client = AsyncIOMotorClient(MONGO_URL)
db = client["bitcoin_db"]

# Queues for async processing
block_queue = asyncio.Queue()
mempool_queue = asyncio.Queue()
peer_queue = asyncio.Queue()
db_queue = asyncio.Queue()

os.makedirs("logs", exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
logger.add(
    "logs/bitcoin_indexer.json",
    serialize=True,
    level="DEBUG",
    rotation="1 day",
    retention="7 days",
)
logger.info("🚀 Bitcoin Indexer Started!")


# --- Asynchronous helper to update system tracker ---
async def update_last_processed(key: str, value: dict):
    """Updates the last processed document in the system collection."""
    await db.system.update_one({"_id": key}, {"$set": value}, upsert=True)
    logger.debug(f"✅ Updated system tracker: {key} -> {value}")


# --- Helper functions for address derivation (unchanged) ---
def hash160(data: bytes) -> bytes:
    """Perform SHA256 followed by RIPEMD160 on the data."""
    sha = hashlib.sha256(data).digest()
    ripemd = hashlib.new("ripemd160", sha).digest()
    return ripemd


def base58_check_encode(payload: bytes) -> str:
    """Encode payload using Base58Check encoding."""
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    full_payload = payload + checksum
    num = int.from_bytes(full_payload, "big")
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    encoded = ""
    while num > 0:
        num, mod = divmod(num, 58)
        encoded = alphabet[mod] + encoded
    # Add '1' for each leading 0 byte in payload.
    n_leading = len(full_payload) - len(full_payload.lstrip(b"\x00"))
    return "1" * n_leading + encoded


def derive_address_from_pubkey(script_hex: str) -> Optional[str]:
    """
    Derives a P2PKH address from a P2PK script hex.
    Expects the script to be in the format:
      - Uncompressed: "41{65-byte pubkey}ac"
    Returns the Base58Check encoded address (using version byte 0x00) or None on error.
    """
    try:
        script_bytes = bytes.fromhex(script_hex)
        if script_bytes[-1] != 0xAC:
            return None
        if script_bytes[0] != 0x41 or len(script_bytes) != 67:
            return None
        pubkey = script_bytes[1:-1]
        h160 = hash160(pubkey)
        payload = b"\x00" + h160
        return base58_check_encode(payload)
    except Exception as e:
        logger.error(f"Error deriving address from pubkey: {e}")
        return None


# --- End helper functions ---


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
            start_time = time.time_ns()
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.url,
                        auth=self.auth,
                        data=payload,
                        headers={"content-type": "application/json"},
                        timeout=10,
                    ) as resp:
                        elapsed_time_ms = (time.time_ns() - start_time) / 1e6
                        if resp.status == 200:
                            result = await resp.json()
                            logger.debug(
                                f"RPC call {method} succeeded in {elapsed_time_ms:.2f}ms"
                            )
                            return result.get("result")
                        else:
                            logger.error(
                                f"RPC error {resp.status} on method {method} "
                                f"(attempt {attempt + 1}) after {elapsed_time_ms:.2f}ms"
                            )
                            raise Exception(f"RPC error {resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                elapsed_time_ms = (time.time_ns() - start_time) / 1e6
                logger.warning(
                    f"RPC call {method} failed (attempt {attempt + 1}) "
                    f"after {elapsed_time_ms:.2f}ms: {e}"
                )
                await asyncio.sleep(2**attempt)
        logger.error(f"Max retries reached for RPC method {method}")
        return None


class BlockFetcher:
    """Fetches new blocks and sends them to the queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            try:
                latest_block_height = await self.rpc.call("getblockcount")
                if latest_block_height is None:
                    await asyncio.sleep(0.1)
                    continue

                # Get the last processed block height from the system collection.
                last_stored = await db.system.find_one({"_id": "blocks"}) or {
                    "last_height": 0
                }
                start_height = last_stored["last_height"] + 1

                for height in range(start_height, latest_block_height + 1):
                    block_hash = await self.rpc.call("getblockhash", [height])
                    if block_hash is None:
                        continue
                    block_data = await self.rpc.call("getblock", [block_hash, 2])
                    if block_data:
                        await block_queue.put(block_data)
                        await update_last_processed("blocks", {"last_height": height})
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("BlockFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"BlockFetcher error: {e}")
                await asyncio.sleep(5)


class MempoolFetcher:
    """Fetches mempool transactions and sends them to the queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            try:
                mempool_txids = await self.rpc.call("getrawmempool")
                if mempool_txids is None:
                    await asyncio.sleep(0.1)
                    continue

                last_stored = await db.system.find_one({"_id": "transactions"}) or {
                    "last_txid": None
                }
                last_txid = last_stored["last_txid"]

                for txid in mempool_txids[:50]:
                    if txid == last_txid:
                        break
                    tx_data = await self.rpc.call("getrawtransaction", [txid, True])
                    if tx_data:
                        await mempool_queue.put(tx_data)
                        await update_last_processed("transactions", {"last_txid": txid})
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("MempoolFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"MempoolFetcher error: {e}")
                await asyncio.sleep(5)


class PeerFetcher:
    """Fetches peer data and sends them to the queue."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            try:
                peers = await self.rpc.call("getpeerinfo")
                if peers:
                    await peer_queue.put(peers)
                    await update_last_processed(
                        "peers", {"last_updated": datetime.now(timezone.utc)}
                    )
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info("PeerFetcher cancelled.")
                raise
            except Exception as e:
                logger.exception(f"PeerFetcher error: {e}")
                await asyncio.sleep(5)


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
            except asyncio.CancelledError:
                logger.info("MessageListener cancelled.")
                raise
            except Exception as e:
                logger.error(f"MessageListener error: {e}")


def previous_tx_key_builder(func, *args, **kwargs):
    """
    Custom key builder for get_previous_transaction.
    Assumes the first argument is the txid.
    """
    txid = args[0] if args else kwargs.get("txid")
    return f"previous_transaction:{txid}"


@cached(ttl=3600, cache=Cache.MEMORY, key_builder=previous_tx_key_builder)
async def get_previous_transaction(txid: str) -> Optional[dict]:
    """
    Asynchronously retrieves a previous transaction document by txid.
    The result is cached in-memory for 1 hour.
    """
    return await db.transactions.find_one({"txid": txid})


async def invalidate_previous_transaction_cache(txid: str):
    """
    Invalidate the cache entry for a given transaction ID.
    """
    key = previous_tx_key_builder(get_previous_transaction, txid)
    await get_previous_transaction.cache.delete(key)
    logger.debug(f"Cache invalidated for transaction {txid}")


async def enrich_transaction(
    tx: dict,
    block_time: int = None,
    block_hash: str = None,
    block_height: int = None,
    rpc: BitcoinRPC = None,
) -> dict:
    """
    Enrich a transaction with addresses, fees, and input/output totals.
    For each output (vout):
      - If the "addresses" field is missing or empty and a script hex is available:
         - If the script type is "pubkey", derive the P2PKH address manually.
         - Otherwise, attempt a 'decodescript' RPC call.
    Also, block information is added.
    """
    if block_hash is not None:
        tx["block_hash"] = block_hash
    if block_height is not None:
        tx["block_height"] = block_height
    if block_time is not None:
        tx["block_time"] = block_time

    all_addresses = set()
    input_total = 0.0

    start_time = time.time_ns()

    if "vin" in tx:
        for vin_item in tx["vin"]:
            vin_item.setdefault("addresses", [])
            prev_txid = vin_item.get("txid")
            if prev_txid:
                prev_index = vin_item.get("vout", -1)
                prev_tx_doc = await get_previous_transaction(prev_txid)
                if prev_tx_doc and "vout" in prev_tx_doc:
                    try:
                        spent_out = prev_tx_doc["vout"][prev_index]
                        addrs = spent_out.get("scriptPubKey", {}).get("addresses", [])
                        spk = spent_out.get("scriptPubKey", {})
                        if (
                            (not addrs or len(addrs) == 0)
                            and spk.get("type") == "pubkey"
                            and "hex" in spk
                        ):
                            derived = derive_address_from_pubkey(spk["hex"])
                            if derived:
                                addrs = [derived]
                                spk["addresses"] = addrs
                        vin_item["addresses"] = addrs
                        vin_item["value"] = spent_out.get("value", 0.0)
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
            spk = vout_item.get("scriptPubKey", {})
            addrs = spk.get("addresses", [])
            if (not addrs or len(addrs) == 0) and "hex" in spk and rpc is not None:
                if spk.get("type") == "pubkey":
                    derived = derive_address_from_pubkey(spk["hex"])
                    if derived:
                        addrs = [derived]
                        spk["addresses"] = addrs
                else:
                    decoded = await rpc.call("decodescript", [spk["hex"]])
                    if decoded and decoded.get("addresses"):
                        addrs = decoded["addresses"]
                        spk["addresses"] = addrs
            for a in addrs:
                all_addresses.add(a)

    tx["input_total"] = input_total
    tx["output_total"] = output_total
    tx["fee"] = round(input_total - output_total, 8)
    tx["all_addresses"] = list(all_addresses)

    duration = (time.time_ns() - start_time) / 1e6
    logger.debug(f"🥸 Enriched transaction in {duration:.2f}ms")
    return tx


class Indexer:
    """Handles writing data to MongoDB from the DB queue with logging."""

    def __init__(self, rpc: BitcoinRPC):
        self.rpc = rpc

    async def run(self):
        while True:
            try:
                start_time = time.time_ns()
                collection, data = await db_queue.get()

                if collection == "blocks":
                    await db.blocks.update_one(
                        {"hash": data["hash"]}, {"$set": data}, upsert=True
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.info(
                        f"🟢 Indexed block {data['height']} | "
                        f"{datetime.fromtimestamp(data['time'])} in {duration:.2f}ms"
                    )

                    block_time = data.get("time")
                    block_hash = data["hash"]
                    block_height = data["height"]

                    for tx in data["tx"]:
                        enriched_tx = await enrich_transaction(
                            tx,
                            block_time=block_time,
                            block_hash=block_hash,
                            block_height=block_height,
                            rpc=self.rpc,
                        )
                        await db.transactions.update_one(
                            {"txid": enriched_tx["txid"]},
                            {"$set": enriched_tx},
                            upsert=True,
                        )
                        # Invalidate cache for the updated transaction.
                        await invalidate_previous_transaction_cache(enriched_tx["txid"])

                        for vout in enriched_tx.get("vout", []):
                            value = vout.get("value", 0.0)
                            addresses = vout.get("scriptPubKey", {}).get(
                                "addresses", []
                            )
                            for address in addresses:
                                await db.balances.update_one(
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
                                await db.balances.update_one(
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

                    await update_last_processed(
                        "blocks", {"last_height": data["height"]}
                    )

                elif collection == "transactions":
                    enriched_tx = await enrich_transaction(data, rpc=self.rpc)
                    await db.transactions.update_one(
                        {"txid": enriched_tx["txid"]},
                        {"$set": enriched_tx},
                        upsert=True,
                    )
                    # Invalidate the cache for the updated transaction.
                    await invalidate_previous_transaction_cache(enriched_tx["txid"])
                    await update_last_processed(
                        "transactions", {"last_txid": data["txid"]}
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.debug(
                        f"🔵 New Transaction Indexed: TXID {data['txid']} in {duration:.2f}ms"
                    )

                elif collection == "peers":
                    await db.peers.delete_many({})
                    await db.peers.insert_many(data)
                    await update_last_processed(
                        "peers", {"last_updated": datetime.now(timezone.utc)}
                    )
                    duration = (time.time_ns() - start_time) / 1e6
                    logger.debug(
                        f"🟣 Peers Updated: {len(data)} connected peers in {duration:.2f}ms"
                    )

                db_queue.task_done()
            except asyncio.CancelledError:
                logger.info("Indexer cancelled.")
                raise
            except Exception as e:
                logger.error(f"❌ Indexer Error: {e}")


async def main():
    await setup_collections()
    rpc = BitcoinRPC(RPC_USER, RPC_PASSWORD, RPC_URL)

    block_fetcher = BlockFetcher(rpc)
    mempool_fetcher = MempoolFetcher(rpc)
    peer_fetcher = PeerFetcher(rpc)
    message_listener = MessageListener()
    indexer = Indexer(rpc)

    await asyncio.gather(
        block_fetcher.run(),
        mempool_fetcher.run(),
        peer_fetcher.run(),
        message_listener.run(),
        indexer.run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down Bitcoin Indexer...")
    except Exception as e:
        logger.error(f"❌ Unhandled exception in main: {e}")

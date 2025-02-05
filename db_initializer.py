import asyncio
import os

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING  # You can still import index helpers from pymongo

# Load MongoDB URI from environment variable
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB Connection using Motor
client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=200)
db = client["bitcoin_db"]


async def setup_collections():
    """
    Ensures all collections exist and are properly indexed.
    """
    logger.info(f"🔄 Connecting to MongoDB at {MONGO_URI}")
    logger.info("🔄 Initializing MongoDB collections and indexes...")

    coll_names = await db.list_collection_names()

    # Blocks Collection
    if "blocks" not in coll_names:
        await db.create_collection("blocks")
        logger.info("✅ Created 'blocks' collection.")
    await db.blocks.create_index([("height", ASCENDING)], unique=True)
    await db.blocks.create_index([("hash", ASCENDING)], unique=True)

    # Transactions Collection
    if "transactions" not in coll_names:
        await db.create_collection("transactions")
        logger.info("✅ Created 'transactions' collection.")
    await db.transactions.create_index([("txid", ASCENDING)], unique=True)
    await db.transactions.create_index([("vin.txid", ASCENDING)])
    await db.transactions.create_index([("vout.scriptPubKey.addresses", ASCENDING)])
    await db.transactions.create_index([("block_hash", ASCENDING)])
    await db.transactions.create_index([("block_height", ASCENDING)])
    await db.transactions.create_index([("all_addresses", ASCENDING)])

    # Mempool Collection
    if "mempool" not in coll_names:
        await db.create_collection("mempool")
        logger.info("✅ Created 'mempool' collection.")
    await db.mempool.create_index([("txid", ASCENDING)], unique=True)

    # Peers Collection
    if "peers" not in coll_names:
        await db.create_collection("peers")
        logger.info("✅ Created 'peers' collection.")
    await db.peers.create_index([("ip", ASCENDING)])

    # System Collection
    if "system" not in coll_names:
        await db.create_collection("system")
        logger.info("✅ Created 'system' collection.")
        await db.system.insert_many(
            [
                {"_id": "blocks", "last_height": 0},
                {"_id": "transactions", "last_txid": None},
                {"_id": "peers", "last_updated": None},
            ]
        )

    # Balances Collection
    if "balances" not in coll_names:
        await db.create_collection("balances")
        await db.balances.create_index([("address", ASCENDING)], unique=True)

    logger.info("🎉 MongoDB initialization complete!")


async def wait_for_mongo():
    retries = 5
    for attempt in range(retries):
        try:
            # server_info() is awaitable in Motor
            await client.server_info()
            logger.info("✅ MongoDB is ready.")
            return
        except Exception:
            logger.warning(f"🚧 MongoDB not ready. Retrying ({attempt+1}/{retries})...")
            await asyncio.sleep(5)
    raise Exception("❌ MongoDB failed to start.")


if __name__ == "__main__":

    async def init():
        await wait_for_mongo()
        await setup_collections()

    asyncio.run(init())

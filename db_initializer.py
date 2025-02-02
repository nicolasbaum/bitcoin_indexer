import os

from loguru import logger
from pymongo import ASCENDING, MongoClient

# Load MongoDB URI from environment variable
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB Connection
client = MongoClient(MONGO_URI)
db = client["bitcoin_db"]


def setup_collections():
    """
    Ensures all collections exist and are properly indexed.
    """
    logger.info(f"🔄 Connecting to MongoDB at {MONGO_URI}")
    logger.info("🔄 Initializing MongoDB collections and indexes...")

    # 🟢 Blocks Collection
    if "blocks" not in db.list_collection_names():
        db.create_collection("blocks")
        logger.info("✅ Created 'blocks' collection.")
    db.blocks.create_index([("height", ASCENDING)], unique=True)
    db.blocks.create_index([("hash", ASCENDING)], unique=True)

    # 🟢 Transactions Collection
    if "transactions" not in db.list_collection_names():
        db.create_collection("transactions")
        logger.info("✅ Created 'transactions' collection.")
    db.transactions.create_index([("txid", ASCENDING)], unique=True)

    # Index on input references
    db.transactions.create_index([("vin.txid", ASCENDING)])

    # Index on output addresses
    db.transactions.create_index([("vout.scriptPubKey.addresses", ASCENDING)])

    # Block references
    db.transactions.create_index([("block_hash", ASCENDING)])
    db.transactions.create_index([("block_height", ASCENDING)])

    # Optional index on aggregated addresses
    db.transactions.create_index([("all_addresses", ASCENDING)])

    # 🟢 Mempool Collection
    if "mempool" not in db.list_collection_names():
        db.create_collection("mempool")
        logger.info("✅ Created 'mempool' collection.")
    db.mempool.create_index([("txid", ASCENDING)], unique=True)

    # 🟢 Peers Collection
    if "peers" not in db.list_collection_names():
        db.create_collection("peers")
        logger.info("✅ Created 'peers' collection.")
    db.peers.create_index([("ip", ASCENDING)])

    # 🟢 System Collection (Tracking last processed items)
    if "system" not in db.list_collection_names():
        db.create_collection("system")
        logger.info("✅ Created 'system' collection.")

        # Initialize tracking documents for each fetcher
        db.system.insert_many(
            [
                {"_id": "blocks", "last_height": 0},
                {"_id": "transactions", "last_txid": None},
                {"_id": "peers", "last_updated": None},
            ]
        )

    logger.info("🎉 MongoDB initialization complete!")


if __name__ == "__main__":
    setup_collections()

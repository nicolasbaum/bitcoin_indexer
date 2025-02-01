from loguru import logger
from pymongo import ASCENDING, MongoClient

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["bitcoin_db"]


def setup_collections():
    """
    Ensures all collections exist and are properly indexed.
    """
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
    db.transactions.create_index([("vin.txid", ASCENDING)])  # Index inputs
    db.transactions.create_index(
        [("vout.scriptPubKey.addresses", ASCENDING)]
    )  # Index addresses

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

        # Initialize default tracking documents for each fetcher
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

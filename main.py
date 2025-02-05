import asyncio
import os
import sys

from aiocache import caches
from dotenv import load_dotenv
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from db_initializer import setup_collections
from modules.bitcoin_rpc import BitcoinRPC
from modules.block_fetcher import BlockFetcher
from modules.db_indexer import Indexer
from modules.mempool_fetcher import MempoolFetcher
from modules.msg_dispatcher import MessageDispatcher
from modules.peer_fetcher import PeerFetcher

load_dotenv()

caches.set_config(
    {
        "default": {
            "cache": "aiocache.RedisCache",
            "endpoint": "redis",  # The service name from docker-compose
            "port": 6379,
            "db": 0,
            "ttl": 3600,
            "serializer": {"class": "aiocache.serializers.JsonSerializer"},
        }
    }
)

os.makedirs("logs", exist_ok=True)
logger.remove()
logger.add(sys.stdout, format="{time} {level} {message}", level="INFO")
# logger.add(
#     "logs/bitcoin_indexer.json",
#     serialize=True,
#     level="DEBUG",
#     rotation="1 day",
#     retention="7 days",
# )
logger.info("🚀 Bitcoin Indexer Started!")


async def main():
    # Load environment variables
    RPC_USER = os.getenv("RPC_USER", "__cookie__")
    RPC_PASSWORD = os.getenv("RPC_PASSWORD", "")
    RPC_URL = os.getenv("RPC_URL", "http://umbrel.local:8332")
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

    # Queues for async processing
    block_queue = asyncio.Queue()
    mempool_queue = asyncio.Queue()
    peer_queue = asyncio.Queue()
    queues = {"blocks": block_queue, "transactions": mempool_queue, "peers": peer_queue}
    db_queue = asyncio.Queue()

    await setup_collections()
    rpc = BitcoinRPC(RPC_USER, RPC_PASSWORD, RPC_URL)
    client = AsyncIOMotorClient(MONGO_URI, maxPoolSize=200, minPoolSize=50)
    db = client["bitcoin_db"]

    block_fetcher = BlockFetcher(rpc, db, block_queue)
    mempool_fetcher = MempoolFetcher(rpc, db, mempool_queue)
    peer_fetcher = PeerFetcher(rpc, db, peer_queue)
    message_dispatcher = MessageDispatcher(queues, db_queue)
    indexer = Indexer(rpc, db, db_queue)

    await asyncio.gather(
        block_fetcher.run(),
        mempool_fetcher.run(),
        peer_fetcher.run(),
        message_dispatcher.run(),
        indexer.run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down Bitcoin Indexer...")
    except Exception as e:
        logger.error(f"❌ Unhandled exception in main: {e}")

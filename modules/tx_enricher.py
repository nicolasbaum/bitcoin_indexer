import os
import time
from typing import Any, Mapping, Optional

from aiocache import cached, caches
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC
from modules.utils import derive_address_from_pubkey

USE_REDIS = os.getenv("USE_REDIS", "false").lower() == "true"

if USE_REDIS:
    caches.set_config(
        {
            "default": {
                "cache": "aiocache.RedisCache",
                "endpoint": os.getenv("REDIS_HOST", "redis"),  # default service name
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "db": int(os.getenv("REDIS_DB", "0")),
                "ttl": 3600,
                "serializer": {"class": "aiocache.serializers.JsonSerializer"},
            }
        }
    )
else:
    caches.set_config(
        {
            "default": {
                "cache": "aiocache.SimpleMemoryCache",
                "ttl": 3600,
                "serializer": {"class": "aiocache.serializers.JsonSerializer"},
            }
        }
    )


class TxEnricher:
    def __init__(
        self,
        rpc: BitcoinRPC,
        db_client: AsyncIOMotorDatabase[Mapping[str, Any]],
    ):
        self.rpc = rpc
        self.db_client = db_client
        if USE_REDIS:
            logger.info("Using Redis for caching...")
        else:
            logger.info("Using in-memory cache...")

    @staticmethod
    def previous_tx_key_builder(_, *args, **kwargs):
        txid = args[1] if len(args) > 1 else kwargs.get("txid")
        key = f"previous_transaction:{txid}"
        return key

    @cached(ttl=3600, key_builder=previous_tx_key_builder, cache_none=True)
    async def get_previous_transaction(self, txid: str) -> Optional[dict]:
        return await self.db_client.transactions.find_one({"txid": txid})

    async def invalidate_previous_transaction_cache(self, txid: str):
        """
        Invalidate the cache entry for a given transaction ID.
        """
        # Note: When invalidating, we need to supply the same arguments as used during caching.
        key = self.previous_tx_key_builder(self.get_previous_transaction, self, txid)
        await self.get_previous_transaction.cache.delete(key)
        logger.debug(f"Cache invalidated for transaction {txid}")

    async def enrich_transaction(
        self,
        tx: dict,
        block_time: int = None,
        block_hash: str = None,
        block_height: int = None,
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
                    prev_tx_doc = await self.get_previous_transaction(prev_txid)
                    if prev_tx_doc and "vout" in prev_tx_doc:
                        try:
                            spent_out = prev_tx_doc["vout"][prev_index]
                            addrs = spent_out.get("scriptPubKey", {}).get(
                                "addresses", []
                            )
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
                if (
                    (not addrs or len(addrs) == 0)
                    and "hex" in spk
                    and self.rpc is not None
                ):
                    if spk.get("type") == "pubkey":
                        derived = derive_address_from_pubkey(spk["hex"])
                        if derived:
                            addrs = [derived]
                            spk["addresses"] = addrs
                    else:
                        decoded = await self.rpc.call("decodescript", [spk["hex"]])
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

# File: modules/tx_enricher.py
import os
import time
from typing import Any, Mapping, Optional

from aiocache import cached, caches
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorDatabase

from modules.bitcoin_rpc import BitcoinRPC
from modules.utils import (
    base58_check_encode,
    derive_address_from_pubkey,
)

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

    @cached(ttl=3600, key_builder=previous_tx_key_builder)
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
             - If it's a P2PK script, derive the P2PKH address manually.
             - If it's a P2SH script, derive the P2SH address manually.
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
                    if (
                        prev_tx_doc is None
                    ):  # Bugfix: Handle case where previous tx is not found
                        logger.warning(
                            f"Previous transaction {prev_txid} not found in database"
                            f" for vin enrichment in tx {tx['txid']}"
                        )
                        continue  # Skip to next vin_item
                    if "vout" in prev_tx_doc:
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
                        except (
                            IndexError,
                            KeyError,
                        ) as e:  # Bugfix: Improved error logging for vin processing
                            logger.warning(
                                f"Error enriching vin for txid={tx['txid']},"
                                f" prev_txid={prev_txid}, prev_index={prev_index}: {e}"
                            )

        output_total = 0.0
        if "vout" in tx:
            for vout_item in tx["vout"]:
                val = vout_item.get("value", 0.0)
                output_total += val
                spk = vout_item.get("scriptPubKey", {})
                addrs = spk.get("addresses", [])

                if (
                    not addrs and "hex" in spk and self.rpc is not None
                ):  # Check if addresses are missing AND we have script hex
                    script_hex = spk["hex"]
                    if (
                        script_hex.startswith("a9")
                        and script_hex.endswith("87")
                        and len(script_hex) == 50
                    ):  # Basic P2SH check
                        try:
                            script_hash_hex = script_hex[2:-2]
                            script_hash_bytes = bytes.fromhex(script_hash_hex)
                            p2sh_version_byte = (
                                b"\x05"  # Mainnet P2SH version byte (HARDCODED MAINNET)
                            )
                            payload = p2sh_version_byte + script_hash_bytes
                            p2sh_address = base58_check_encode(payload)
                            addrs = [p2sh_address]
                            spk["addresses"] = addrs
                            logger.debug(
                                f"Successfully derived P2SH address: {p2sh_address} "
                                f"from script_hex: {script_hex} in txid: {tx['txid']}"
                            )
                        except Exception as e_p2sh_derive:
                            logger.warning(
                                f"P2SH address derivation failed for script_hex: "
                                f"{script_hex} in txid: "
                                f"{tx['txid']}. Falling back to decodescript. "
                                f"Error: {e_p2sh_derive}"
                            )
                            decoded = await self.rpc.call(
                                "decodescript", [script_hex]
                            )  # Fallback to decodescript
                            if decoded and decoded.get(
                                "address"
                            ):  # <---- Corrected address check: decoded.get("address")
                                addrs = [
                                    decoded["address"]
                                ]  # <---- Extract legacy address from "address" key
                                spk["addresses"] = addrs
                            elif decoded and not decoded.get(
                                "address"
                            ):  # Warning if decodescript runs but still no legacy address
                                logger.warning(
                                    f"decodescript returned no *legacy* address for script hex: "
                                    f"{spk['hex']} in txid: {tx['txid']}, "
                                    f"script: {spk.get('asm')}. "
                                    f"Full decoded output (DEBUG level log) might have more info."
                                )
                                logger.debug(
                                    f"Decoded output: {decoded}"
                                )  # Full decoded output for debugging
                            elif not decoded:
                                logger.warning(
                                    f"decodescript call failed for script hex: "
                                    f"{spk['hex']} in txid: "
                                    f"{tx['txid']}, script: {spk.get('asm')}"
                                )
                        else:
                            pass  # P2SH derivation successful, addresses already set
                    else:  # Not P2SH, fallback to decodescript
                        decoded = await self.rpc.call("decodescript", [script_hex])
                        if decoded and decoded.get(
                            "address"
                        ):  # <---- Corrected address check: decoded.get("address")
                            addrs = [
                                decoded["address"]
                            ]  # <---- Extract legacy address from "address" key
                            spk["addresses"] = addrs
                        elif decoded and not decoded.get(
                            "address"
                        ):  # Warning if decodescript runs but no legacy address
                            logger.warning(
                                f"decodescript returned no *legacy* address for script hex: "
                                f"{spk['hex']} in txid: {tx['txid']}, script: {spk.get('asm')}. "
                                f"Full decoded output (DEBUG level log) might have more info."
                            )
                            logger.debug(
                                f"Decoded output: {decoded}"
                            )  # Full decoded output for debugging
                        elif not decoded:  # Warning for decodescript call failure
                            logger.warning(
                                f"decodescript call failed for script hex: "
                                f"{spk['hex']} in txid: {tx['txid']}, script: {spk.get('asm')}"
                            )

                for a in addrs:
                    all_addresses.add(a)

        tx["input_total"] = input_total
        tx["output_total"] = output_total
        tx["fee"] = round(input_total - output_total, 8)
        tx["all_addresses"] = list(all_addresses)

        duration = (time.time_ns() - start_time) / 1e6
        logger.debug(f"🥸 Enriched transaction in {duration:.2f}ms")
        return tx

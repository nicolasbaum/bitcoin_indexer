import asyncio
import json
import time
from typing import Any

import aiohttp
from loguru import logger


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

"""
fetcher/chainlink.py
====================
Fetch harga BTC/USD dari Chainlink Oracle via Polygon RPC.

Polymarket menggunakan Chainlink BTC/USD feed untuk settlement
setiap market 5-menit. Dengan menggunakan sumber yang sama,
beat price bot kita akan selaras dengan beat price Polymarket.

Cara kerja:
  - Panggil latestRoundData() pada Chainlink BTC/USD aggregator
    di Polygon Mainnet via public RPC (tanpa API key)
  - Decode ABI-encoded response untuk extract harga
  - Fallback ke multiple RPC jika satu gagal

Chainlink BTC/USD Aggregator (Polygon Mainnet):
  Address : 0xc907E116054Ad103354f2D350FD2514433D57F6f
  Decimals: 8
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from fetcher.hyperliquid_ws import MarketData

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Chainlink Config
# ─────────────────────────────────────────────
CHAINLINK_BTC_USD_POLYGON = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

# latestRoundData() function selector (keccak256 dari signature)
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# Public Polygon RPC (fallback list — diurutkan dari paling reliable)
POLYGON_RPCS = [
    "https://rpc.ankr.com/polygon",           # Ankr — paling stabil, gratis
    "https://polygon.llamarpc.com",            # LlamaRPC — reliable, no rate limit
    "https://polygon.drpc.org",                # dRPC — decentralized, stabil
    "https://1rpc.io/matic",                   # 1RPC — privacy-focused, reliable
    "https://polygon-rpc.com",                 # fallback lama
]

# Poll interval — Chainlink update BTC/USD setiap ~30 detik
CHAINLINK_POLL_INTERVAL = 10   # poll setiap 10 detik untuk data lebih fresh


# ─────────────────────────────────────────────
# Chainlink Fetcher
# ─────────────────────────────────────────────
class ChainlinkFetcher:
    """
    Polling harga BTC/USD dari Chainlink via Polygon JSON-RPC.
    Berjalan sebagai async loop di background.
    Tidak memerlukan API key atau library web3.
    """

    def __init__(self, market_data: MarketData, poll_interval: float = CHAINLINK_POLL_INTERVAL):
        self.data          = market_data
        self.poll_interval = poll_interval
        self._running      = False
        self._executor     = ThreadPoolExecutor(max_workers=1)
        self._last_price   = 0.0
        self._fail_count   = 0

    async def start(self):
        """Entry point — loop polling Chainlink setiap poll_interval detik."""
        self._running = True
        logger.info(f"[CHAINLINK] Starting BTC/USD polling every {self.poll_interval}s...")

        while self._running:
            try:
                loop  = asyncio.get_event_loop()
                price = await loop.run_in_executor(
                    self._executor, self._fetch_price
                )
                if price and price > 0:
                    self._last_price      = price
                    self._fail_count      = 0
                    self.data.chainlink_price = price
                    logger.debug(f"[CHAINLINK] BTC/USD: ${price:,.2f}")
                else:
                    self._fail_count += 1
                    if self._fail_count >= 3:
                        logger.warning(
                            f"[CHAINLINK] {self._fail_count} polls gagal berturut-turut. "
                            f"Fallback ke Hyperliquid price."
                        )

            except Exception as e:
                logger.error(f"[CHAINLINK] Polling error: {e}")

            await asyncio.sleep(self.poll_interval)

    def _fetch_price(self) -> float | None:
        """
        Query Chainlink BTC/USD aggregator via Polygon JSON-RPC.

        latestRoundData() mengembalikan:
          (uint80 roundId, int256 answer, uint256 startedAt,
           uint256 updatedAt, uint80 answeredInRound)

        ABI encoding: tiap field = 32 bytes (64 hex chars).
        answer = slot ke-2 (offset 64 hex chars dari start).
        Harga memiliki 8 desimal.
        """
        for rpc_url in POLYGON_RPCS:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "method" : "eth_call",
                    "params" : [
                        {
                            "to"  : CHAINLINK_BTC_USD_POLYGON,
                            "data": LATEST_ROUND_DATA_SELECTOR,
                        },
                        "latest",
                    ],
                    "id": 1,
                }

                resp = requests.post(rpc_url, json=payload, timeout=6)

                if resp.status_code != 200:
                    logger.debug(f"[CHAINLINK] {rpc_url} HTTP {resp.status_code}")
                    continue

                result = resp.json().get("result", "")
                if not result or result == "0x" or len(result) < 130:
                    logger.debug(f"[CHAINLINK] {rpc_url} response kosong atau terlalu pendek")
                    continue

                # Decode ABI response
                hex_data   = result[2:]          # hilangkan '0x'
                answer_hex = hex_data[64:128]    # slot ke-2 (int256 answer)
                answer     = int(answer_hex, 16)

                # Two's complement untuk nilai negatif (tidak terjadi pada BTC)
                if answer >= 2 ** 255:
                    answer -= 2 ** 256

                price = answer / 1e8   # 8 desimal

                if 10_000 < price < 1_000_000:   # sanity check: $10K–$1M
                    logger.debug(f"[CHAINLINK] ${price:,.2f} via {rpc_url}")
                    return price

            except requests.exceptions.Timeout:
                logger.debug(f"[CHAINLINK] {rpc_url} timeout")
                continue
            except Exception as e:
                logger.debug(f"[CHAINLINK] {rpc_url} error: {e}")
                continue

        return None

    @property
    def last_price(self) -> float:
        return self._last_price

    def stop(self):
        self._running = False
        self._executor.shutdown(wait=False)
        logger.info("[CHAINLINK] Stopped.")


# ─────────────────────────────────────────────
# Helper: ambil harga Chainlink sekali (sync)
# ─────────────────────────────────────────────
def fetch_chainlink_price_once() -> float | None:
    """
    Ambil harga Chainlink BTC/USD sekali saja (synchronous).
    Berguna untuk test atau inisialisasi awal.
    """
    fetcher = ChainlinkFetcher.__new__(ChainlinkFetcher)
    fetcher._executor = ThreadPoolExecutor(max_workers=1)
    return fetcher._fetch_price()


# ─────────────────────────────────────────────
# Test runner
# ─────────────────────────────────────────────
async def _test_run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from fetcher.hyperliquid_ws import MarketData
    data    = MarketData()
    fetcher = ChainlinkFetcher(data, poll_interval=15)

    print("=" * 60)
    print("CHAINLINK BTC/USD — TEST (3 polls)")
    print("=" * 60)

    task = asyncio.create_task(fetcher.start())

    for i in range(3):
        await asyncio.sleep(15)
        print(f"\n─── Poll #{i+1} ──────────────────────────────────────")
        print(f"  Chainlink BTC/USD : ${data.chainlink_price:,.2f}")

    fetcher.stop()
    task.cancel()
    print("\n✓ Test selesai.")


if __name__ == "__main__":
    asyncio.run(_test_run())

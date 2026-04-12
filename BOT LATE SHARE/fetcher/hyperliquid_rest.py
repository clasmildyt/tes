"""
fetcher/hyperliquid_rest.py
============================
Polling Open Interest BTC via Hyperliquid REST API setiap 5 detik.
OI dipakai sebagai filter kekuatan sinyal di Signal Engine.

Endpoint: POST https://api.hyperliquid.xyz/info
"""

import asyncio
import logging
import time
import requests
from concurrent.futures import ThreadPoolExecutor

from fetcher.hyperliquid_ws import MarketData

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Hyperliquid REST endpoint
# ─────────────────────────────────────────────
HL_REST_URL = "https://api.hyperliquid.xyz/info"

# Polling interval (detik)
OI_POLL_INTERVAL = 5


class HyperliquidRESTFetcher:
    """
    Polling OI (Open Interest) BTC via Hyperliquid REST API.
    Berjalan sebagai async loop di background.
    """

    def __init__(self, market_data: MarketData, poll_interval: float = OI_POLL_INTERVAL):
        self.data = market_data
        self.poll_interval = poll_interval
        self._running = False
        self._fail_count = 0
        self._executor = ThreadPoolExecutor(max_workers=1)

    async def start(self):
        """Entry point — loop polling OI setiap `poll_interval` detik."""
        self._running = True
        self._fail_count = 0
        logger.info(f"[REST] Starting OI polling every {self.poll_interval}s...")

        while self._running:
            try:
                # Jalankan requests (blocking) di thread terpisah agar tidak block event loop
                loop = asyncio.get_event_loop()
                oi = await loop.run_in_executor(self._executor, self._fetch_open_interest)
                if oi is not None:
                    self._fail_count = 0   # reset backoff saat berhasil
                    # Simpan OI sebelumnya sebelum diupdate
                    self.data.open_interest_prev = self.data.open_interest
                    self.data.open_interest = oi
                    self.data.open_interest_updated_at = time.time()

                    oi_change = oi - self.data.open_interest_prev
                    logger.debug(
                        f"[REST] OI: ${oi:,.0f} | "
                        f"Change: ${oi_change:+,.0f}"
                    )
                else:
                    self._fail_count += 1

            except Exception as e:
                logger.error(f"[REST] OI fetch error: {e}")
                self._fail_count += 1

            # Exponential backoff saat gagal: 5s → 10s → 20s → 40s → 60s (cap)
            if self._fail_count > 0:
                wait = min(self.poll_interval * (2 ** min(self._fail_count - 1, 3)), 60)
            else:
                wait = self.poll_interval
            await asyncio.sleep(wait)

    def _fetch_open_interest(self) -> float | None:
        """
        Fetch Open Interest BTC dari Hyperliquid REST API.
        Menggunakan requests (synchronous) untuk menghindari bug DNS aiohttp di Windows.

        Request body: {"type": "metaAndAssetCtxs"}
        Response: [meta_info, [asset_ctx_per_coin, ...]]

        asset_ctx untuk BTC berisi field 'openInterest' (dalam BTC).
        Kita convert ke USD dengan kalikan harga mark.
        """
        payload = {"type": "metaAndAssetCtxs"}

        try:
            resp = requests.post(HL_REST_URL, json=payload, timeout=5)

            if resp.status_code != 200:
                logger.warning(f"[REST] HTTP {resp.status_code}")
                return None

            data = resp.json()

            # Response format: [meta, [assetCtx0, assetCtx1, ...]]
            # meta["universe"] berisi list coin dengan index-nya
            meta = data[0]
            asset_ctxs = data[1]

            # Cari index BTC
            btc_index = None
            for i, coin_info in enumerate(meta.get("universe", [])):
                if coin_info.get("name") == "BTC":
                    btc_index = i
                    break

            if btc_index is None:
                logger.error("[REST] BTC not found in universe list")
                return None

            btc_ctx = asset_ctxs[btc_index]

            # openInterest dalam satuan BTC (contract)
            oi_btc = float(btc_ctx.get("openInterest", 0))

            # markPx = harga mark saat ini
            mark_px = float(btc_ctx.get("markPx", 0))

            # Convert ke USD
            oi_usd = oi_btc * mark_px

            # Update harga BTC di shared state jika WS belum dapat data
            if mark_px > 0 and self.data.btc_price == 0:
                self.data.btc_price = mark_px

            # Funding rate (sudah dalam format hourly, e.g. 0.0001 = 0.01%/jam)
            funding = float(btc_ctx.get("funding", 0))
            self.data.funding_rate = funding

            return oi_usd

        except requests.exceptions.Timeout:
            logger.warning("[REST] OI request timeout")
            return None

    def stop(self):
        """Hentikan polling loop."""
        self._running = False
        self._executor.shutdown(wait=False)
        logger.info("[REST] Stopping OI fetcher...")


# ─────────────────────────────────────────────
# Helper: cek apakah OI turun (posisi lemah tersapu)
# ─────────────────────────────────────────────
def is_oi_decreasing(market_data: MarketData) -> bool:
    """
    Return True jika OI turun dibanding polling sebelumnya.
    Ini menandakan posisi lemah sudah di-liquidate (tersapu).
    """
    if market_data.open_interest_prev == 0:
        return False  # Belum ada data pembanding
    return market_data.open_interest < market_data.open_interest_prev


def get_oi_change_pct(market_data: MarketData) -> float:
    """
    Hitung % perubahan OI antara dua polling terakhir.
    Negatif = OI turun.
    """
    if market_data.open_interest_prev == 0:
        return 0.0
    return (
        (market_data.open_interest - market_data.open_interest_prev)
        / market_data.open_interest_prev
        * 100
    )


# ─────────────────────────────────────────────
# Test runner
# ─────────────────────────────────────────────
async def _test_run():
    """
    Test mode: poll OI 5x dan tampilkan hasilnya.
    Jalankan: python -m fetcher.hyperliquid_rest
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    from fetcher.hyperliquid_ws import MarketData
    data = MarketData()
    fetcher = HyperliquidRESTFetcher(data, poll_interval=5)

    print("=" * 60)
    print("HYPERLIQUID REST — OI POLLING TEST (5 polls)")
    print("=" * 60)

    task = asyncio.create_task(fetcher.start())

    for i in range(5):
        await asyncio.sleep(5)
        oi_change_pct = get_oi_change_pct(data)
        oi_decreasing = is_oi_decreasing(data)

        print(f"\n─── Poll #{i+1} ──────────────────────────────────────")
        print(f"  Open Interest     : ${data.open_interest:,.0f}")
        print(f"  OI Prev           : ${data.open_interest_prev:,.0f}")
        print(f"  OI Change         : {oi_change_pct:+.3f}%")
        print(f"  OI Decreasing?    : {oi_decreasing}")
        print(f"  BTC Price (mark)  : ${data.btc_price:,.2f}")

    fetcher.stop()
    task.cancel()
    print("\n✓ Test selesai.")


if __name__ == "__main__":
    asyncio.run(_test_run())

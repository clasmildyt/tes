"""
fetcher/hyperliquid_ws.py
=========================
Koneksi WebSocket ke Hyperliquid untuk stream real-time:
  - Liquidation events BTC (dengan nilai USD)
  - CVD (Cumulative Volume Delta) dihitung dari trade stream

Data disimpan di shared MarketData object yang diupdate terus-menerus.
"""

import asyncio
import json
import time
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
import websockets

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Hyperliquid WebSocket endpoint
# ─────────────────────────────────────────────
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"

# ─────────────────────────────────────────────
# Shared data structure antar modul
# ─────────────────────────────────────────────
@dataclass
class LiquidationEvent:
    """Satu event liquidation BTC."""
    timestamp: float        # Unix timestamp
    side: str               # "LONG" atau "SHORT" (posisi yang di-liquidate)
    usd_value: float        # Nilai liquidation dalam USD
    price: float            # Harga BTC saat liquidation


@dataclass
class MarketData:
    """
    Shared state yang diisi oleh WebSocket fetcher dan REST fetcher.
    Dibaca oleh Signal Engine untuk evaluasi kondisi betting.
    """
    # --- Liquidation ---
    # Deque untuk menyimpan event liquidation 5 menit terakhir
    liquidations: deque = field(default_factory=lambda: deque(maxlen=500))

    # --- CVD (Cumulative Volume Delta) ---
    # List of (timestamp, delta) untuk 5 menit terakhir
    # delta = buy_volume - sell_volume per trade
    cvd_ticks: deque = field(default_factory=lambda: deque(maxlen=2000))
    cvd_cumulative: float = 0.0         # Running total CVD

    # --- Open Interest ---
    open_interest: float = 0.0          # OI BTC saat ini (dalam USD)
    open_interest_prev: float = 0.0     # OI sebelumnya (5 detik lalu)
    open_interest_updated_at: float = 0.0

    # --- BTC Price ---
    btc_price: float = 0.0

    # --- Funding Rate ---
    funding_rate: float = 0.0   # Hourly funding rate (dari REST API)

    # --- Chainlink Oracle Price ---
    chainlink_price: float = 0.0  # BTC/USD dari Chainlink (Polygon) — sama sumber dengan Polymarket

    # --- Connection status ---
    ws_connected: bool = False
    last_ws_message: float = 0.0


# ─────────────────────────────────────────────
# Hyperliquid WebSocket Client
# ─────────────────────────────────────────────
class HyperliquidWSFetcher:
    """
    Stream data liquidation dan trades dari Hyperliquid WebSocket.
    Subscribe ke dua channel:
      - 'trades' untuk hitung CVD
      - 'userFills' tidak tersedia publik, jadi liquidation dideteksi
        dari channel 'allMids' + 'trades' dengan flag isLiquidation
    """

    def __init__(self, market_data: MarketData):
        self.data = market_data
        self._running = False
        self._ws = None
        self._fail_count = 0

    async def start(self):
        """Entry point — jalankan koneksi WS dengan auto-reconnect + exponential backoff."""
        self._running = True
        while self._running:
            try:
                logger.info("[WS] Connecting to Hyperliquid WebSocket...")
                async with websockets.connect(
                    HL_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    open_timeout=15,
                ) as ws:
                    self._ws = ws
                    self.data.ws_connected = True
                    self._fail_count = 0   # reset backoff saat connect berhasil
                    logger.info("[WS] Connected!")

                    # Subscribe ke channel trades BTC
                    await self._subscribe(ws)

                    # Loop baca pesan
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        self._handle_message(raw_msg)

            except websockets.exceptions.ConnectionClosed as e:
                self._fail_count += 1
                logger.warning(f"[WS] Connection closed: {e}.")
            except Exception as e:
                self._fail_count += 1
                logger.error(f"[WS] Error: {e}.")
            finally:
                self.data.ws_connected = False
                if self._running:
                    # Exponential backoff: 3s → 6s → 12s → 24s → 48s → 60s (cap)
                    delay = min(3 * (2 ** max(self._fail_count - 1, 0)), 60)
                    logger.info(f"[WS] Reconnecting in {delay}s... (attempt #{self._fail_count})")
                    await asyncio.sleep(delay)

    async def _subscribe(self, ws):
        """Subscribe ke channel yang diperlukan di Hyperliquid."""
        # Channel trades: stream semua trade BTC termasuk liquidations
        sub_trades = {
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": "BTC"
            }
        }
        await ws.send(json.dumps(sub_trades))
        logger.info("[WS] Subscribed to BTC trades channel")

    def _handle_message(self, raw_msg: str):
        """Parse dan routing pesan WebSocket."""
        try:
            msg = json.loads(raw_msg)
            channel = msg.get("channel", "")

            if channel == "trades":
                self._process_trades(msg.get("data", []))

        except json.JSONDecodeError:
            logger.warning(f"[WS] Invalid JSON: {raw_msg[:100]}")
        except Exception as e:
            logger.error(f"[WS] Error processing message: {e}")

    def _process_trades(self, trades: list):
        """
        Proses setiap trade dari channel 'trades'.

        Hyperliquid mempunyai 2 jenis liquidation event:

        1. ADL (Auto-Deleveraging):
           - hash = "0x" + "0"*64  (zero hash)
           - users[0] = ADL engine address (0xa20ea526...)
           - Biasanya nilai kecil

        2. Batch Liquidation (cascading):
           - hash = real tx hash (non-zero)
           - Satu liquidation order fills ke banyak counterparty
           - Semua fills berbagi hash yang SAMA dalam satu WS message
           - users[1] (taker/aggressor) sama di semua fills cluster ini
           - Nilai total bisa sangat besar (ratusan ribu USD)

        Detection strategy:
          - Group semua trades by hash dalam satu message
          - Zero hash → ADL event langsung dicatat
          - Sama hash 2+ fills → batch liquidation/large order event
          - Single large trade (>$50K, unique hash) → catat juga
        """
        now = time.time()
        ZERO_HASH   = "0x" + "0" * 64
        # ADL engine address Hyperliquid (selalu muncul di zero-hash trades)
        ADL_ADDRESS = "0xa20ea526a2b2e2471a43cb981d613feeef27c9af"

        # ── Step 1: Update CVD dan harga untuk SEMUA trades ──
        for trade in trades:
            try:
                if trade.get("coin", "") != "BTC":
                    continue
                px  = float(trade.get("px", 0))
                sz  = float(trade.get("sz", 0))
                ts  = trade.get("time", now * 1000) / 1000
                usd = px * sz
                side = trade.get("side", "")

                if px > 0:
                    self.data.btc_price = px

                delta = usd if side == "B" else -usd
                self.data.cvd_cumulative += delta
                self.data.cvd_ticks.append((ts, delta, self.data.cvd_cumulative))
                self.data.last_ws_message = now

            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"[WS] CVD parse error: {e}")

        # ── Step 2: Deteksi liquidation ──
        # Group BTC trades by hash untuk deteksi batch liquidation
        hash_groups: dict = {}
        for trade in trades:
            try:
                if trade.get("coin", "") != "BTC":
                    continue
                h    = trade.get("hash", "")
                side = trade.get("side", "")
                px   = float(trade.get("px", 0))
                sz   = float(trade.get("sz", 0))
                ts   = trade.get("time", now * 1000) / 1000
                usd  = px * sz
                users = trade.get("users", [])

                if h not in hash_groups:
                    hash_groups[h] = {
                        "trades": [], "buy_usd": 0.0, "sell_usd": 0.0,
                        "ts": ts, "price": px, "users1": set()
                    }
                hash_groups[h]["trades"].append(trade)
                hash_groups[h]["price"] = px
                if side == "B":
                    hash_groups[h]["buy_usd"] += usd
                else:
                    hash_groups[h]["sell_usd"] += usd
                if len(users) > 1:
                    hash_groups[h]["users1"].add(users[1])

            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"[WS] Group parse error: {e}")

        # Evaluasi setiap hash group
        for h, grp in hash_groups.items():
            n_fills   = len(grp["trades"])
            buy_usd   = grp["buy_usd"]
            sell_usd  = grp["sell_usd"]
            total_usd = buy_usd + sell_usd
            ts        = grp["ts"]
            px        = grp["price"]
            is_zero   = (h == ZERO_HASH)

            # Cek apakah ini liquidation event:
            # 1. Zero hash (ADL event)
            # 2. Batch: 2+ fills berbagi hash yang sama (cascade liquidation)
            # 3. Single large trade (>$50K, kemungkinan forced close)
            is_liq = (
                is_zero
                or n_fills >= 2
                or total_usd >= 50_000
            )

            if not is_liq:
                continue

            # Tentukan arah liquidation
            # Dominan BUY → posisi SHORT yang di-liquidate (SHORT liq)
            # Dominan SELL → posisi LONG yang di-liquidate (LONG liq)
            if buy_usd >= sell_usd:
                liq_side = "SHORT"
                liq_usd  = buy_usd
            else:
                liq_side = "LONG"
                liq_usd  = sell_usd

            if liq_usd <= 0:
                continue

            event = LiquidationEvent(
                timestamp=ts,
                side=liq_side,
                usd_value=liq_usd,
                price=px,
            )
            self.data.liquidations.append(event)

            liq_type = "ADL" if is_zero else f"BATCH({n_fills})" if n_fills >= 2 else "LARGE"
            logger.info(
                f"[LIQ] {liq_type} | {liq_side} | "
                f"${liq_usd:,.0f} | "
                f"fills={n_fills} | "
                f"Price={px:,.1f}"
            )

    def stop(self):
        """Hentikan WebSocket loop."""
        self._running = False
        logger.info("[WS] Stopping WebSocket fetcher...")


# ─────────────────────────────────────────────
# Helper: query liquidation dalam window waktu
# ─────────────────────────────────────────────
def get_liquidations_in_window(
    market_data: MarketData,
    side: str,
    window_seconds: float = 30.0,
) -> tuple[float, int]:
    """
    Hitung total nilai liquidation untuk side tertentu dalam window detik terakhir.

    Args:
        market_data: shared MarketData object
        side: "LONG" atau "SHORT"
        window_seconds: berapa detik ke belakang yang dihitung

    Returns:
        (total_usd, count) — total nilai USD dan jumlah event
    """
    cutoff = time.time() - window_seconds
    total_usd = 0.0
    count = 0

    for event in market_data.liquidations:
        if event.timestamp >= cutoff and event.side == side:
            total_usd += event.usd_value
            count += 1

    return total_usd, count


def get_cvd_change(
    market_data: MarketData,
    window_seconds: float = 120.0,
) -> float:
    """
    Hitung perubahan CVD dalam window detik terakhir.

    Returns:
        CVD delta: positif = net buying pressure, negatif = net selling pressure
    """
    cutoff = time.time() - window_seconds
    delta_sum = 0.0

    for ts, delta, _cumulative in market_data.cvd_ticks:
        if ts >= cutoff:
            delta_sum += delta

    return delta_sum


# ─────────────────────────────────────────────
# Test runner (jalankan langsung untuk debug)
# ─────────────────────────────────────────────
async def _test_run():
    """
    Test mode: stream data selama 60 detik dan print ringkasannya.
    Jalankan: python -m fetcher.hyperliquid_ws
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    data = MarketData()
    fetcher = HyperliquidWSFetcher(data)

    print("=" * 60)
    print("HYPERLIQUID WS — TEST MODE (60 detik)")
    print("=" * 60)

    # Jalankan fetcher di background
    task = asyncio.create_task(fetcher.start())

    # Print summary setiap 10 detik
    for i in range(6):
        await asyncio.sleep(10)
        liq_short, short_count = get_liquidations_in_window(data, "SHORT", 30)
        liq_long, long_count = get_liquidations_in_window(data, "LONG", 30)
        cvd_2m = get_cvd_change(data, 120)

        print(f"\n─── Update #{i+1} ───────────────────────────────────")
        print(f"  BTC Price         : ${data.btc_price:,.2f}")
        print(f"  WS Connected      : {data.ws_connected}")
        print(f"  CVD (2min)        : ${cvd_2m:+,.0f}")
        print(f"  CVD (cumulative)  : ${data.cvd_cumulative:+,.0f}")
        print(f"  Total CVD ticks   : {len(data.cvd_ticks)}")
        print(f"  Liq SHORT (30s)   : ${liq_short:,.0f} ({short_count} events)")
        print(f"  Liq LONG  (30s)   : ${liq_long:,.0f}  ({long_count} events)")
        print(f"  Total Liq events  : {len(data.liquidations)}")
        print(f"  OI               : ${data.open_interest:,.0f}")

    fetcher.stop()
    task.cancel()
    print("\n✓ Test selesai.")


if __name__ == "__main__":
    asyncio.run(_test_run())

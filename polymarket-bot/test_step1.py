"""
test_step1.py
=============
Script verifikasi Step 1: jalankan WS + REST fetcher bersamaan
selama 90 detik dan tampilkan dashboard data real-time.

Jalankan: python test_step1.py
"""

import asyncio
import logging
import time
import os
import sys

# Tambahkan root folder ke path
sys.path.insert(0, os.path.dirname(__file__))

from fetcher.hyperliquid_ws import (
    MarketData,
    HyperliquidWSFetcher,
    get_liquidations_in_window,
    get_cvd_change,
)
from fetcher.hyperliquid_rest import (
    HyperliquidRESTFetcher,
    is_oi_decreasing,
    get_oi_change_pct,
)

# Setup logging — hanya tampilkan WARNING ke atas agar dashboard bersih
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Aktifkan INFO hanya untuk modul fetcher
logging.getLogger("fetcher.hyperliquid_ws").setLevel(logging.INFO)
logging.getLogger("fetcher.hyperliquid_rest").setLevel(logging.INFO)


def print_dashboard(data: MarketData, elapsed: int):
    """Cetak status dashboard di terminal."""
    liq_short_30s, short_count = get_liquidations_in_window(data, "SHORT", 30)
    liq_long_30s, long_count = get_liquidations_in_window(data, "LONG", 30)
    cvd_2m = get_cvd_change(data, 120)
    cvd_5m = get_cvd_change(data, 300)
    oi_change_pct = get_oi_change_pct(data)
    oi_down = is_oi_decreasing(data)

    # Evaluasi pra-sinyal sederhana
    short_liq_trigger = liq_short_30s >= 500_000
    long_liq_trigger = liq_long_30s >= 500_000
    cvd_up = cvd_2m > 0
    cvd_down = cvd_2m < 0

    up_possible = short_liq_trigger and cvd_up and oi_down
    down_possible = long_liq_trigger and cvd_down and oi_down

    pre_signal = "⬆  UP POSSIBLE" if up_possible else (
                 "⬇  DOWN POSSIBLE" if down_possible else "─  SKIP")

    print("\n" + "=" * 62)
    print(f"  HYPERLIQUID DATA DASHBOARD   (elapsed: {elapsed}s)")
    print("=" * 62)
    print(f"  BTC Price         : ${data.btc_price:>12,.2f}")
    print(f"  WS Connected      : {data.ws_connected}")
    print(f"  Last WS msg ago   : {time.time() - data.last_ws_message:.1f}s" if data.last_ws_message else "  Last WS msg       : -")
    print("─" * 62)
    print("  LIQUIDATIONS (30 detik terakhir)")
    print(f"    SHORT liq       : ${liq_short_30s:>12,.0f}  ({short_count} events)  {'✓ TRIGGER' if short_liq_trigger else ''}")
    print(f"    LONG  liq       : ${liq_long_30s:>12,.0f}  ({long_count} events)  {'✓ TRIGGER' if long_liq_trigger else ''}")
    print(f"    Total stored    : {len(data.liquidations)} events")
    print("─" * 62)
    print("  CVD (Cumulative Volume Delta)")
    print(f"    2-min CVD       : ${cvd_2m:>+13,.0f}  {'▲ BULLISH' if cvd_up else '▼ BEARISH'}")
    print(f"    5-min CVD       : ${cvd_5m:>+13,.0f}")
    print(f"    Cumulative CVD  : ${data.cvd_cumulative:>+13,.0f}")
    print(f"    CVD ticks stored: {len(data.cvd_ticks)}")
    print("─" * 62)
    print("  OPEN INTEREST")
    print(f"    Current OI      : ${data.open_interest:>12,.0f}")
    print(f"    Previous OI     : ${data.open_interest_prev:>12,.0f}")
    print(f"    OI Change       : {oi_change_pct:>+11.3f}%  {'▼ DECREASING ✓' if oi_down else '▲ INCREASING'}")
    print("─" * 62)
    print(f"  PRE-SIGNAL       :  {pre_signal}")
    print("=" * 62)


async def main():
    print("\n" + "=" * 62)
    print("  STEP 1 — DATA FETCHER TEST")
    print("  Streaming 90 detik dari Hyperliquid...")
    print("  Ctrl+C untuk stop lebih awal")
    print("=" * 62 + "\n")

    # Buat shared data object
    data = MarketData()

    # Buat fetcher
    ws_fetcher = HyperliquidWSFetcher(data)
    rest_fetcher = HyperliquidRESTFetcher(data, poll_interval=5)

    # Jalankan keduanya secara bersamaan (concurrent)
    ws_task = asyncio.create_task(ws_fetcher.start())
    rest_task = asyncio.create_task(rest_fetcher.start())

    start_time = time.time()
    dashboard_interval = 10  # Print dashboard setiap 10 detik
    total_duration = 90       # Total test duration

    try:
        while True:
            elapsed = int(time.time() - start_time)

            # Tunggu sampai WS terhubung (max 10 detik)
            if not data.ws_connected and elapsed < 10:
                print(f"\r  Menunggu koneksi WS... {elapsed}s", end="", flush=True)
                await asyncio.sleep(1)
                continue

            # Print dashboard
            print_dashboard(data, elapsed)

            # Stop setelah durasi test
            if elapsed >= total_duration:
                print(f"\n  Test selesai setelah {elapsed}s.")
                break

            await asyncio.sleep(dashboard_interval)

    except KeyboardInterrupt:
        print("\n\n  [!] Dihentikan manual (Ctrl+C)")

    finally:
        # Graceful shutdown
        ws_fetcher.stop()
        rest_fetcher.stop()
        ws_task.cancel()
        rest_task.cancel()

        # Ringkasan akhir
        print("\n" + "=" * 62)
        print("  RINGKASAN AKHIR")
        print("=" * 62)
        print(f"  Total liquidations  : {len(data.liquidations)}")
        print(f"  Total CVD ticks     : {len(data.cvd_ticks)}")
        print(f"  Final BTC Price     : ${data.btc_price:,.2f}")
        print(f"  Final OI            : ${data.open_interest:,.0f}")
        print(f"  Final CVD (cum)     : ${data.cvd_cumulative:+,.0f}")

        # Tampilkan 5 liquidation terakhir jika ada
        if data.liquidations:
            print("\n  5 Liquidation Terakhir:")
            recent = list(data.liquidations)[-5:]
            for liq in recent:
                print(
                    f"    [{liq.side:5s}] ${liq.usd_value:>12,.0f} "
                    f"@ ${liq.price:,.1f}"
                )

        print("=" * 62)
        print("  ✓ Step 1 DONE. Data fetcher berjalan dengan benar.")
        print("=" * 62 + "\n")


if __name__ == "__main__":
    asyncio.run(main())

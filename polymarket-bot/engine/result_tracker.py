"""
engine/result_tracker.py
========================
Melacak hasil setiap sinyal UP/DOWN setelah 5 menit.

Flow:
  1. Signal UP/DOWN masuk → simpan beat_price (referensi Polymarket)
  2. Saat window Polymarket berakhir → cek btc_price_exit
  3. Bandingkan exit vs BEAT PRICE → WIN / LOSS

Win condition (selaras Polymarket):
  UP   → btc_exit > beat_price  (harga lebih tinggi dari candle sebelumnya ✓)
  DOWN → btc_exit < beat_price  (harga lebih rendah dari candle sebelumnya ✓)

PnL simulation (realistic Polymarket):
  WIN  → +70% dari bet amount  (beli di ~0.59c, jual di $1.00)
  LOSS → -100% dari bet amount (lose seluruh modal yang dibet)
"""

import asyncio
import csv
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

from fetcher.hyperliquid_ws import MarketData

logger = logging.getLogger(__name__)

WINDOW_SECONDS   = 300   # 5 menit
WIN_PROFIT_RATE  = float(os.getenv("WIN_PROFIT_RATE", "0.70"))   # profit 70% saat WIN
LOSS_RATE        = float(os.getenv("LOSS_RATE",       "1.00"))   # loss 100% saat LOSS


@dataclass
class PendingResult:
    """Sinyal yang sedang menunggu hasil 5 menit ke depan."""
    signal_id:   str    # timestamp string, sebagai key unik
    direction:   str    # "UP" atau "DOWN"
    btc_entry:   float  # harga BTC saat sinyal dikirim
    beat_price:  float  # harga referensi Polymarket (close candle sebelumnya)
    window_id:   str    # ID window Polymarket ("HH:MM")
    window_end_ts: float  # unix timestamp kapan window berakhir (selaras :00/:05)
    entry_time:  float  # unix timestamp saat sinyal dikirim
    exit_time:   float  # unix timestamp evaluasi (= window_end_ts)
    amount_usdc: float  # ukuran bet simulasi
    odds:        float  # odds saat bet
    context: dict = None  # snapshot lengkap kondisi market saat sinyal


class ResultTracker:
    """
    Async tracker yang menunggu 5 menit lalu resolve setiap pending signal.
    Update file backtest_results.csv dengan kolom win/loss.
    """

    def __init__(self, market_data: MarketData, result_log: str = "logs/backtest_results.csv"):
        self.data      = market_data
        self.log_path  = Path(result_log)
        self._pending: list[PendingResult] = []
        self._running  = False

        # Stats real-time
        self.total    = 0
        self.wins     = 0
        self.losses   = 0
        self.pnl      = 0.0

        self._init_csv()

    def _init_csv(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_path.exists():
            with open(self.log_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    # ── Identitas ──
                    "signal_id", "direction",
                    "window_id",
                    # ── Harga & Outcome ──
                    "beat_price", "btc_entry", "btc_exit",
                    "price_vs_beat_at_signal",  # USD: entry - beat_price
                    "vs_beat_pct",              # % exit vs beat_price
                    "exit_margin_usd",          # USD: |exit - beat_price|
                    # ── Timing & Sesi ──
                    "entry_time", "exit_time",
                    "hour_of_day",              # 0-23 UTC
                    "day_of_week",              # Monday..Sunday
                    "session",                  # ASIA/LONDON/NEW_YORK/DEAD_ZONE
                    "window_elapsed_sec",        # detik ke berapa sinyal masuk
                    # ── Liquidation ──
                    "liq_short_usd", "liq_long_usd",
                    "dominance_ratio",
                    "cluster_short", "cluster_long",
                    "liq_pressure_trend",       # BUILDING/FADING/STABLE
                    # ── CVD & OI ──
                    "cvd_value",
                    "cvd_velocity",             # USD/detik rate of change CVD
                    "oi_change_pct",
                    # ── Trend (6 timeframe) ──
                    "trend_1m_dir", "trend_1m_pct",
                    "trend_3m_dir", "trend_3m_pct",
                    "trend_5m_dir", "trend_5m_pct",
                    "trend_10m_dir", "trend_10m_pct",
                    "trend_30m_dir", "trend_30m_pct",
                    "trend_1h_dir", "trend_1h_pct",
                    # ── Price Range Context ──
                    "price_vs_30m_high",        # USD: harga - 30m high
                    "price_vs_30m_low",         # USD: harga - 30m low
                    "price_vs_30m_range_pct",   # 0-100%: posisi dalam range
                    # ── Konfirmasi ──
                    "confirmation_delta_usd",   # $ harga bergerak saat confirm
                    # ── Candle Context ──
                    "candle_streak",            # "UP,DOWN,UP"
                    "candle_volatility_avg",    # Avg high-low $ per candle
                    # ── Market Extra ──
                    "funding_rate",             # Hourly funding rate
                    "signals_last_30min",       # Berapa sinyal dalam 30 menit terakhir
                    # ── Learning State ──
                    "threshold_mult_at_signal", # Multiplier saat sinyal
                    "win_streak_at_signal",
                    "loss_streak_at_signal",
                    # ── Bet ──
                    "amount_usdc", "odds",
                    # ── Hasil ──
                    "result", "pnl_usdc", "win_rate_running",
                ])
            logger.info(f"[TRACKER] Created: {self.log_path}")

    def add(
        self,
        direction:   str,
        btc_entry:   float,
        beat_price:  float,
        window_id:   str,
        window_end_ts: float,
        amount_usdc: float,
        odds:        float,
        on_resolved  = None,  # callback async(direction, is_win, btc_entry, btc_exit)
        context:     dict = None,  # snapshot lengkap kondisi market saat sinyal
    ) -> str:
        """
        Daftarkan sinyal baru untuk di-track hasilnya.
        Resolusi terjadi tepat saat window Polymarket berakhir (window_end_ts).

        Args:
            beat_price    : Harga referensi (close candle sebelumnya)
            window_id     : ID window Polymarket ("HH:MM")
            window_end_ts : Unix timestamp kapan window berakhir
        """
        now = time.time()
        signal_id = f"{int(now)}-{direction}-{window_id}"

        pending = PendingResult(
            signal_id     = signal_id,
            direction     = direction,
            btc_entry     = btc_entry,
            beat_price    = beat_price,
            window_id     = window_id,
            window_end_ts = window_end_ts,
            entry_time    = now,
            exit_time     = window_end_ts,  # resolve tepat saat window tutup
            amount_usdc   = amount_usdc,
            odds          = odds,
            context       = context or {},
        )
        pending._on_resolved = on_resolved

        self._pending.append(pending)
        remaining = max(0, window_end_ts - now)
        logger.info(
            f"[TRACKER] Tracking {direction} | Window: {window_id} | "
            f"Beat: ${beat_price:,.2f} | Entry: ${btc_entry:,.2f} | "
            f"Resolve in {remaining:.0f}s"
        )
        return signal_id

    async def run_loop(self):
        """Async loop: cek setiap 5 detik apakah ada pending yang sudah expired."""
        self._running = True
        while self._running:
            await asyncio.sleep(5)
            now = time.time()
            still_pending = []

            for p in self._pending:
                if now >= p.exit_time:
                    await self._resolve(p)
                else:
                    still_pending.append(p)

            self._pending = still_pending

    async def _resolve(self, p: PendingResult):
        """
        Evaluate hasil saat window Polymarket berakhir.

        Win condition (selaras Polymarket):
          UP   → btc_exit > beat_price  (harga tutup DI ATAS beat price)
          DOWN → btc_exit < beat_price  (harga tutup DI BAWAH beat price)
        """
        btc_exit = self.data.btc_price
        if btc_exit == 0:
            logger.warning(f"[TRACKER] BTC price = 0, skip resolve for {p.signal_id}")
            return

        # Perubahan vs beat price (referensi Polymarket)
        vs_beat_pct = (btc_exit - p.beat_price) / p.beat_price * 100 if p.beat_price > 0 else 0.0

        # Tentukan WIN / LOSS / FLAT berdasarkan BEAT PRICE (bukan entry price)
        if p.direction == "UP":
            if btc_exit > p.beat_price:
                result = "WIN"
            elif btc_exit < p.beat_price:
                result = "LOSS"
            else:
                result = "FLAT"
        else:  # DOWN
            if btc_exit < p.beat_price:
                result = "WIN"
            elif btc_exit > p.beat_price:
                result = "LOSS"
            else:
                result = "FLAT"

        # Hitung PnL simulasi (realistic Polymarket)
        # WIN  → +70% dari bet  (beli share di ~0.59c, dapat $1.00 saat menang)
        # LOSS → -100% dari bet (kehilangan seluruh modal yang dibet)
        if result == "WIN":
            pnl = p.amount_usdc * WIN_PROFIT_RATE   # contoh: $20 × 0.70 = +$14
            self.wins += 1
        elif result == "LOSS":
            pnl = -(p.amount_usdc * LOSS_RATE)      # contoh: $20 × 1.00 = -$20
            self.losses += 1
        else:
            pnl = 0.0

        self.total += 1
        self.pnl   += pnl
        win_rate    = self.wins / self.total * 100 if self.total > 0 else 0.0

        # Ambil context snapshot (default kosong jika tidak ada)
        ctx = p.context or {}

        # Hitung derived fields
        price_vs_beat_signal = round(p.btc_entry - p.beat_price, 2)
        exit_margin_usd      = round(abs(btc_exit - p.beat_price), 2)

        # Tulis ke CSV
        with open(self.log_path, "a", newline="") as f:
            csv.writer(f).writerow([
                # ── Identitas ──
                p.signal_id, p.direction, p.window_id,
                # ── Harga & Outcome ──
                round(p.beat_price, 2),
                round(p.btc_entry, 2),
                round(btc_exit, 2),
                price_vs_beat_signal,
                round(vs_beat_pct, 4),
                exit_margin_usd,
                # ── Timing & Sesi ──
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.entry_time)),
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.exit_time)),
                ctx.get("hour_of_day", ""),
                ctx.get("day_of_week", ""),
                ctx.get("session", ""),
                ctx.get("window_elapsed_sec", ""),
                # ── Liquidation ──
                ctx.get("liq_short_usd", ""),
                ctx.get("liq_long_usd", ""),
                ctx.get("dominance_ratio", ""),
                ctx.get("cluster_short", ""),
                ctx.get("cluster_long", ""),
                ctx.get("liq_pressure_trend", ""),
                # ── CVD & OI ──
                ctx.get("cvd_value", ""),
                ctx.get("cvd_velocity", ""),
                ctx.get("oi_change_pct", ""),
                # ── Trend ──
                ctx.get("trend_1m_dir", ""), ctx.get("trend_1m_pct", ""),
                ctx.get("trend_3m_dir", ""), ctx.get("trend_3m_pct", ""),
                ctx.get("trend_5m_dir", ""), ctx.get("trend_5m_pct", ""),
                ctx.get("trend_10m_dir", ""), ctx.get("trend_10m_pct", ""),
                ctx.get("trend_30m_dir", ""), ctx.get("trend_30m_pct", ""),
                ctx.get("trend_1h_dir", ""), ctx.get("trend_1h_pct", ""),
                # ── Price Range Context ──
                ctx.get("price_vs_30m_high", ""),
                ctx.get("price_vs_30m_low", ""),
                ctx.get("price_vs_30m_range_pct", ""),
                # ── Konfirmasi ──
                ctx.get("confirmation_delta_usd", ""),
                # ── Candle Context ──
                ctx.get("candle_streak", ""),
                ctx.get("candle_volatility_avg", ""),
                # ── Market Extra ──
                ctx.get("funding_rate", ""),
                ctx.get("signals_last_30min", ""),
                # ── Learning State ──
                ctx.get("threshold_mult_at_signal", ""),
                ctx.get("win_streak_at_signal", ""),
                ctx.get("loss_streak_at_signal", ""),
                # ── Bet ──
                round(p.amount_usdc, 2),
                round(p.odds, 4),
                # ── Hasil ──
                result,
                round(pnl, 4),
                round(win_rate, 1),
            ])

        logger.info(
            f"[TRACKER] RESOLVED {p.direction} → {result} | "
            f"Window: {p.window_id} | "
            f"Beat: ${p.beat_price:,.2f} | "
            f"Entry: ${p.btc_entry:,.2f} | Exit: ${btc_exit:,.2f} | "
            f"vs Beat: {vs_beat_pct:+.3f}% | "
            f"PnL: ${pnl:+.2f} | WR: {win_rate:.1f}%"
        )

        # Panggil callback on_resolved jika ada (untuk LearningEngine)
        cb = getattr(p, "_on_resolved", None)
        if cb is not None:
            try:
                await cb(p.direction, result == "WIN", p.btc_entry, btc_exit)
            except Exception as e:
                logger.error(f"[TRACKER] on_resolved callback error: {e}")

    def stop(self):
        self._running = False

    @property
    def win_rate(self) -> float:
        return self.wins / self.total * 100 if self.total > 0 else 0.0

    @property
    def stats(self) -> dict:
        return {
            "total"      : self.total,
            "wins"       : self.wins,
            "losses"     : self.losses,
            "pending"    : len(self._pending),
            "win_rate"   : round(self.win_rate, 1),
            "total_pnl"  : round(self.pnl, 2),
        }

"""
engine/signal.py
================
Signal Engine — evaluasi kondisi market dan return sinyal betting.

Logic (Net Dominance):
  UP      → SHORT liq >= threshold DAN lebih dominan dari LONG liq
             (ratio SHORT/LONG >= 1.5x) DAN CVD naik DAN OI turun
  DOWN    → LONG liq >= threshold DAN lebih dominan dari SHORT liq
             (ratio LONG/SHORT >= 1.5x) DAN CVD turun DAN OI turun
  CONFLICT→ Kedua sisi >= threshold tapi ratio < 1.5x → SKIP (terlalu seimbang)
  SKIP    → Kondisi tidak terpenuhi

Setiap evaluasi di-log ke logs/signals.csv untuk keperluan backtest.
"""

import csv
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from fetcher.hyperliquid_ws import MarketData, get_liquidations_in_window, get_cvd_change
from fetcher.hyperliquid_rest import is_oi_decreasing, get_oi_change_pct
from engine.learner import LearningEngine

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constants (default, bisa di-override via config)
# ─────────────────────────────────────────────
LIQ_WINDOW_SECONDS   = 30      # Window deteksi liquidation
CVD_WINDOW_SECONDS   = 120     # Window CVD (2 menit)
MIN_CVD_THRESHOLD    = 0       # CVD harus > 0 untuk UP, < 0 untuk DOWN
MIN_DOMINANCE_RATIO  = 1.5     # Sisi dominan harus minimal 1.5x lebih besar


class Signal(str, Enum):
    UP   = "UP"
    DOWN = "DOWN"
    SKIP = "SKIP"


@dataclass
class SignalResult:
    """Hasil evaluasi satu siklus signal engine."""
    signal: Signal
    timestamp: float
    btc_price: float

    # Liquidation
    liq_short_usd: float
    liq_long_usd: float
    liq_short_count: int
    liq_long_count: int

    # CVD
    cvd_2min: float

    # OI
    oi_current: float
    oi_prev: float
    oi_change_pct: float
    oi_decreasing: bool

    # Kondisi per syarat
    liq_trigger: bool
    cvd_ok: bool
    oi_ok: bool

    # Net Dominance info
    dominant_side: str        # "SHORT", "LONG", atau "CONFLICT"
    dominance_ratio: float    # ratio sisi dominan vs sisi lawan (0 jika tidak ada konflik)
    is_conflict: bool         # True jika kedua sisi >= threshold tapi ratio < 1.5x

    # Alasan dalam bentuk teks
    reason: str


# ─────────────────────────────────────────────
# CSV Logger
# ─────────────────────────────────────────────
CSV_HEADERS = [
    "timestamp", "datetime", "signal", "btc_price",
    "liq_short_usd", "liq_long_usd", "liq_short_count", "liq_long_count",
    "cvd_2min", "oi_current", "oi_prev", "oi_change_pct", "oi_decreasing",
    "liq_trigger", "cvd_ok", "oi_ok",
    "dominant_side", "dominance_ratio", "is_conflict",
    "reason",
]


class SignalLogger:
    """Tulis setiap SignalResult ke file CSV."""

    def __init__(self, log_path: str = "logs/signals.csv"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        # Buat header jika file belum ada
        if not self.log_path.exists():
            with open(self.log_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writeheader()
            logger.info(f"[LOG] Created signal log: {self.log_path}")

    def write(self, result: SignalResult):
        """Append satu baris ke CSV."""
        row = {
            "timestamp"       : result.timestamp,
            "datetime"        : datetime.fromtimestamp(result.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "signal"          : result.signal.value,
            "btc_price"       : round(result.btc_price, 2),
            "liq_short_usd"   : round(result.liq_short_usd, 2),
            "liq_long_usd"    : round(result.liq_long_usd, 2),
            "liq_short_count" : result.liq_short_count,
            "liq_long_count"  : result.liq_long_count,
            "cvd_2min"        : round(result.cvd_2min, 2),
            "oi_current"      : round(result.oi_current, 2),
            "oi_prev"         : round(result.oi_prev, 2),
            "oi_change_pct"   : round(result.oi_change_pct, 6),
            "oi_decreasing"   : result.oi_decreasing,
            "liq_trigger"     : result.liq_trigger,
            "cvd_ok"          : result.cvd_ok,
            "oi_ok"           : result.oi_ok,
            "dominant_side"   : result.dominant_side,
            "dominance_ratio" : round(result.dominance_ratio, 3),
            "is_conflict"     : result.is_conflict,
            "reason"          : result.reason,
        }
        with open(self.log_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writerow(row)


# ─────────────────────────────────────────────
# Signal Engine
# ─────────────────────────────────────────────
class SignalEngine:
    """
    Evaluasi kondisi market setiap `eval_interval` detik.
    Return signal UP / DOWN / SKIP beserta seluruh data pendukungnya.
    """

    def __init__(
        self,
        market_data: MarketData,
        liq_threshold_usd: float = 500_000,
        eval_interval: float = 5.0,
        log_path: str = "logs/signals.csv",
        log_only_actionable: bool = False,
        learning_engine: LearningEngine = None,
    ):
        """
        Args:
            market_data       : Shared state dari fetcher
            liq_threshold_usd : Minimum USD liquidation untuk trigger (default $500k)
            eval_interval     : Seberapa sering evaluasi dijalankan (detik)
            log_path          : Path ke file CSV log
            log_only_actionable: Jika True, hanya log UP/DOWN (bukan SKIP)
            learning_engine   : Learning engine untuk adaptive threshold & trend filter
        """
        self.data = market_data
        self.liq_threshold = liq_threshold_usd
        self.eval_interval = eval_interval
        self.logger = SignalLogger(log_path)
        self.log_only_actionable = log_only_actionable

        # Learning engine (opsional — jika None, pakai logika static)
        self.learner: LearningEngine = learning_engine

        self._running = False
        self._last_signal: SignalResult | None = None
        self._signal_callbacks: list = []

    def on_signal(self, callback):
        """Register callback yang dipanggil setiap ada sinyal UP/DOWN."""
        self._signal_callbacks.append(callback)

    def evaluate(self) -> SignalResult:
        """
        Jalankan satu siklus evaluasi dan return SignalResult.
        Fungsi ini bisa dipanggil manual atau lewat loop async.
        """
        now = time.time()

        # ── Kumpulkan semua data ──
        liq_short, short_count = get_liquidations_in_window(
            self.data, "SHORT", LIQ_WINDOW_SECONDS
        )
        liq_long, long_count = get_liquidations_in_window(
            self.data, "LONG", LIQ_WINDOW_SECONDS
        )
        cvd_2min    = get_cvd_change(self.data, CVD_WINDOW_SECONDS)
        oi_current  = self.data.open_interest
        oi_prev     = self.data.open_interest_prev
        oi_chg_pct  = get_oi_change_pct(self.data)
        oi_down     = is_oi_decreasing(self.data)
        btc_price   = self.data.btc_price

        # ── Update trend analyzer jika ada learning engine ──
        if self.learner:
            self.learner.trend.add_price(btc_price, now)
            self.learner.trend.add_cvd(cvd_2min, now)
            self.learner.trend.add_liq(liq_short, liq_long, now)

        # ── Gunakan adaptive threshold jika ada learning engine ──
        threshold_up   = self.learner.get_effective_threshold("UP")   if self.learner else self.liq_threshold
        threshold_down = self.learner.get_effective_threshold("DOWN") if self.learner else self.liq_threshold

        # ── Evaluasi kondisi dasar (dengan adaptive threshold) ──
        short_liq_trigger = liq_short >= threshold_up
        long_liq_trigger  = liq_long  >= threshold_down
        cvd_bullish       = cvd_2min > MIN_CVD_THRESHOLD
        cvd_bearish       = cvd_2min < -MIN_CVD_THRESHOLD

        # ── Net Dominance: hitung rasio kekuatan kedua sisi ──
        # Cegah division by zero — jika satu sisi nol, ratio = inf
        if liq_short > 0 and liq_long > 0:
            short_dominates = liq_short / liq_long   # ratio SHORT vs LONG
            long_dominates  = liq_long  / liq_short  # ratio LONG vs SHORT
        elif liq_short > 0:
            short_dominates = float("inf")
            long_dominates  = 0.0
        elif liq_long > 0:
            short_dominates = 0.0
            long_dominates  = float("inf")
        else:
            short_dominates = 0.0
            long_dominates  = 0.0

        # Cek apakah ada KONFLIK: kedua sisi >= threshold tapi terlalu seimbang
        both_triggered  = short_liq_trigger and long_liq_trigger
        is_conflict     = both_triggered and (
            short_dominates < MIN_DOMINANCE_RATIO and
            long_dominates  < MIN_DOMINANCE_RATIO
        )

        # Tentukan sisi dominan
        if liq_short >= liq_long:
            dominant_side  = "SHORT"
            dominance_ratio = short_dominates if liq_long > 0 else 0.0
        else:
            dominant_side  = "LONG"
            dominance_ratio = long_dominates if liq_short > 0 else 0.0

        # ── Tentukan sinyal berdasarkan Net Dominance ──
        signal      = Signal.SKIP
        liq_trigger = False
        cvd_ok      = False
        oi_ok       = False
        reason      = ""

        # ── Pre-check: Trend Filter & Cooldown (dari Learning Engine) ──
        # Harus dilakukan SEBELUM if-elif chain agar bisa dipakai di semua CASE
        up_blocked_reason   = ""
        down_blocked_reason = ""

        if self.learner:
            # Cek cooldown per direction
            up_allowed, up_cd_reason     = self.learner.is_direction_allowed("UP")
            down_allowed, down_cd_reason = self.learner.is_direction_allowed("DOWN")
            if not up_allowed:
                up_blocked_reason = up_cd_reason
            if not down_allowed:
                down_blocked_reason = down_cd_reason

            # Cek trend filter — blokir counter-trend
            if short_liq_trigger and not up_blocked_reason:
                is_ct, ct_reason = self.learner.trend.is_counter_trend("UP")
                if is_ct:
                    up_blocked_reason = ct_reason

            if long_liq_trigger and not down_blocked_reason:
                is_ct, ct_reason = self.learner.trend.is_counter_trend("DOWN")
                if is_ct:
                    down_blocked_reason = ct_reason

        # CASE 1: Konflik — kedua sisi kuat tapi terlalu seimbang → SKIP
        if is_conflict:
            signal      = Signal.SKIP
            liq_trigger = True
            cvd_ok      = cvd_bullish or cvd_bearish
            oi_ok       = oi_down
            reason = (
                f"SKIP (CONFLICT): SHORT ${liq_short:,.0f} vs LONG ${liq_long:,.0f} | "
                f"ratio {short_dominates:.2f}x / {long_dominates:.2f}x "
                f"< {MIN_DOMINANCE_RATIO}x minimum | "
                f"Market chaotic, jangan bet"
            )

        # CASE 2: SHORT dominan → sinyal UP
        elif short_liq_trigger and not long_liq_trigger and cvd_bullish and oi_down and not up_blocked_reason:
            signal      = Signal.UP
            liq_trigger = True
            cvd_ok      = True
            oi_ok       = True
            ratio_str   = f"{short_dominates:.1f}x" if liq_long > 0 else "solo"
            reason = (
                f"SHORT liq ${liq_short:,.0f} (dominan {ratio_str}) | "
                f"CVD +${cvd_2min:,.0f} (bullish) | "
                f"OI turun {oi_chg_pct:.3f}%"
            )

        # CASE 3: SHORT dominan (kedua terpicu tapi SHORT jauh lebih besar) → UP
        elif both_triggered and short_dominates >= MIN_DOMINANCE_RATIO and cvd_bullish and oi_down and not up_blocked_reason:
            signal      = Signal.UP
            liq_trigger = True
            cvd_ok      = True
            oi_ok       = True
            reason = (
                f"SHORT liq ${liq_short:,.0f} dominan {short_dominates:.1f}x "
                f"vs LONG ${liq_long:,.0f} | "
                f"CVD +${cvd_2min:,.0f} (bullish) | "
                f"OI turun {oi_chg_pct:.3f}%"
            )

        # CASE 4: LONG dominan → sinyal DOWN
        elif long_liq_trigger and not short_liq_trigger and cvd_bearish and oi_down and not down_blocked_reason:
            signal      = Signal.DOWN
            liq_trigger = True
            cvd_ok      = True
            oi_ok       = True
            ratio_str   = f"{long_dominates:.1f}x" if liq_short > 0 else "solo"
            reason = (
                f"LONG liq ${liq_long:,.0f} (dominan {ratio_str}) | "
                f"CVD -${abs(cvd_2min):,.0f} (bearish) | "
                f"OI turun {oi_chg_pct:.3f}%"
            )

        # CASE 5: LONG dominan (kedua terpicu tapi LONG jauh lebih besar) → DOWN
        elif both_triggered and long_dominates >= MIN_DOMINANCE_RATIO and cvd_bearish and oi_down and not down_blocked_reason:
            signal      = Signal.DOWN
            liq_trigger = True
            cvd_ok      = True
            oi_ok       = True
            reason = (
                f"LONG liq ${liq_long:,.0f} dominan {long_dominates:.1f}x "
                f"vs SHORT ${liq_short:,.0f} | "
                f"CVD -${abs(cvd_2min):,.0f} (bearish) | "
                f"OI turun {oi_chg_pct:.3f}%"
            )

        # CASE 6a: SKIP karena blocked (trend counter / cooldown)
        elif (short_liq_trigger and up_blocked_reason) or (long_liq_trigger and down_blocked_reason):
            signal      = Signal.SKIP
            liq_trigger = True
            cvd_ok      = cvd_bullish or cvd_bearish
            oi_ok       = oi_down
            block_msg   = up_blocked_reason or down_blocked_reason
            reason      = f"SKIP (BLOCKED): {block_msg}"
            logger.debug(f"[ENGINE] {reason}")

        # CASE 6b: SKIP biasa — kondisi tidak terpenuhi
        else:
            signal      = Signal.SKIP
            liq_trigger = short_liq_trigger or long_liq_trigger
            cvd_ok      = cvd_bullish or cvd_bearish
            oi_ok       = oi_down

            missing = []
            if not short_liq_trigger and not long_liq_trigger:
                missing.append(f"liq rendah (SHORT ${liq_short:,.0f} / LONG ${liq_long:,.0f})")
            if not (cvd_bullish or cvd_bearish):
                missing.append(f"CVD netral (${cvd_2min:,.0f})")
            if not oi_down:
                missing.append(f"OI tidak turun ({oi_chg_pct:+.3f}%)")

            if short_liq_trigger and not (cvd_bullish and oi_down):
                sub = []
                if not cvd_bullish: sub.append(f"CVD tidak bullish (${cvd_2min:,.0f})")
                if not oi_down:     sub.append(f"OI tidak turun ({oi_chg_pct:+.3f}%)")
                missing = [f"SHORT liq OK tapi: {', '.join(sub)}"]
            elif long_liq_trigger and not (cvd_bearish and oi_down):
                sub = []
                if not cvd_bearish: sub.append(f"CVD tidak bearish (${cvd_2min:,.0f})")
                if not oi_down:     sub.append(f"OI tidak turun ({oi_chg_pct:+.3f}%)")
                missing = [f"LONG liq OK tapi: {', '.join(sub)}"]

            reason = "SKIP: " + (", ".join(missing) if missing else "tidak ada trigger")

        result = SignalResult(
            signal          = signal,
            timestamp       = now,
            btc_price       = btc_price,
            liq_short_usd   = liq_short,
            liq_long_usd    = liq_long,
            liq_short_count = short_count,
            liq_long_count  = long_count,
            cvd_2min        = cvd_2min,
            oi_current      = oi_current,
            oi_prev         = oi_prev,
            oi_change_pct   = oi_chg_pct,
            oi_decreasing   = oi_down,
            liq_trigger     = liq_trigger,
            cvd_ok          = cvd_ok,
            oi_ok           = oi_ok,
            dominant_side   = dominant_side,
            dominance_ratio = dominance_ratio,
            is_conflict     = is_conflict,
            reason          = reason,
        )

        self._last_signal = result
        return result

    async def run_loop(self):
        """
        Async loop — evaluasi sinyal setiap eval_interval detik.
        Log ke CSV dan panggil callback jika UP/DOWN.
        """
        import asyncio
        self._running = True
        logger.info(f"[ENGINE] Signal engine started (interval: {self.eval_interval}s)")

        while self._running:
            try:
                # Tunggu sampai WS connected sebelum mulai evaluasi
                if not self.data.ws_connected:
                    await asyncio.sleep(1)
                    continue

                result = self.evaluate()

                # Log ke CSV
                should_log = (
                    not self.log_only_actionable
                    or result.signal != Signal.SKIP
                )
                if should_log:
                    self.logger.write(result)

                # Log ke terminal
                if result.signal != Signal.SKIP:
                    logger.warning(
                        f"[ENGINE] *** SIGNAL {result.signal.value} *** | "
                        f"BTC ${result.btc_price:,.0f} | {result.reason}"
                    )
                elif result.is_conflict:
                    # Conflict layak di-log sebagai warning agar mudah terlihat
                    logger.warning(
                        f"[ENGINE] ⚡ CONFLICT SKIP | "
                        f"SHORT ${result.liq_short_usd:,.0f} vs "
                        f"LONG ${result.liq_long_usd:,.0f} | "
                        f"ratio terlalu seimbang"
                    )
                else:
                    logger.debug(f"[ENGINE] {result.reason}")

                # Panggil callback (executor Step 3 akan listen di sini)
                if result.signal != Signal.SKIP:
                    for cb in self._signal_callbacks:
                        try:
                            await cb(result)
                        except Exception as e:
                            logger.error(f"[ENGINE] Callback error: {e}")

            except Exception as e:
                logger.error(f"[ENGINE] Evaluation error: {e}")

            await asyncio.sleep(self.eval_interval)

    def stop(self):
        self._running = False
        logger.info("[ENGINE] Signal engine stopped.")

    @property
    def last_signal(self) -> SignalResult | None:
        return self._last_signal

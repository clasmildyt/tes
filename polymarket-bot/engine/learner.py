"""
engine/learner.py
=================
Learning Engine — bot belajar dari hasil backtest secara real-time.

Fungsi utama:
  1. Track rolling win rate per direction (UP/DOWN)
  2. Adaptive threshold — naikkan threshold jika win rate turun
  3. Cooldown setelah consecutive losses
  4. Trend filter — cegah counter-trend betting
  5. Generate laporan belajar ke logs/learning_report.csv

Cara kerja:
  - Setelah setiap resolved signal → update statistik
  - Evaluasi "health" setiap 30 detik
  - Jika win rate < batas → terapkan penalti (threshold naik, cooldown)
  - Jika win rate recover → longgarkan kembali
"""

import csv
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Konfigurasi Learning Engine ──
ROLLING_WINDOW       = 15    # Evaluasi berdasarkan N signal terakhir
MIN_WIN_RATE_OK      = 0.50  # Di bawah ini → warning
MIN_WIN_RATE_DANGER  = 0.40  # Di bawah ini → naikkan threshold & cooldown
MAX_CONSECUTIVE_LOSS = 3     # Consecutive loss sebelum cooldown aktif
COOLDOWN_SECONDS     = 300   # 5 menit cooldown setelah consecutive loss
THRESHOLD_PENALTY    = 1.5   # Kalikan threshold jika win rate jelek
TREND_LOOKBACK_PTS   = 12    # Berapa price point untuk hitung trend (= 1 menit @ 5s interval)


@dataclass
class DirectionStats:
    """Statistik per arah (UP atau DOWN)."""
    direction: str
    results: deque = field(default_factory=lambda: deque(maxlen=15))  # True=WIN, False=LOSS
    consecutive_losses: int = 0
    cooldown_until: float = 0.0       # timestamp kapan cooldown berakhir
    threshold_multiplier: float = 1.0  # penalty multiplier untuk threshold
    total_win: int = 0
    total_loss: int = 0

    def record(self, is_win: bool):
        """Catat hasil bet."""
        self.results.append(is_win)
        if is_win:
            self.total_win += 1
            self.consecutive_losses = 0
        else:
            self.total_loss += 1
            self.consecutive_losses += 1

    @property
    def rolling_win_rate(self) -> float:
        """Win rate dalam rolling window terakhir."""
        if not self.results:
            return 1.0  # Default optimis jika belum ada data
        return sum(self.results) / len(self.results)

    @property
    def overall_win_rate(self) -> float:
        total = self.total_win + self.total_loss
        return self.total_win / total if total > 0 else 1.0

    @property
    def sample_size(self) -> int:
        return len(self.results)

    @property
    def in_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    @property
    def cooldown_remaining(self) -> float:
        remaining = self.cooldown_until - time.time()
        return max(0.0, remaining)


class TrendAnalyzer:
    """
    Analisis trend makro dari price history.
    Cegah betting counter-trend.
    Mendukung timeframe: 1m, 3m, 5m, 10m, 30m, 1h.
    """

    def __init__(self, lookback: int = TREND_LOOKBACK_PTS):
        self.lookback = lookback
        # maxlen=720 = 1 jam @ 5s interval
        self.price_history: deque = deque(maxlen=720)

        # CVD history untuk hitung velocity (rate of change per detik)
        self.cvd_history: deque = deque(maxlen=720)

        # Liq history untuk hitung pressure trend (building/fading)
        self.liq_history: deque = deque(maxlen=720)

    def add_price(self, price: float, ts: float = None):
        """Tambah price point baru."""
        self.price_history.append({
            "price": price,
            "ts": ts or time.time()
        })

    def add_cvd(self, cvd_val: float, ts: float = None):
        """Tambah CVD snapshot untuk hitung velocity."""
        self.cvd_history.append({
            "cvd": cvd_val,
            "ts": ts or time.time()
        })

    def add_liq(self, liq_short: float, liq_long: float, ts: float = None):
        """Tambah snapshot liquidation untuk deteksi pressure trend."""
        self.liq_history.append({
            "short": liq_short,
            "long":  liq_long,
            "ts":    ts or time.time()
        })

    def get_trend(self, seconds: int = 60) -> dict:
        """
        Hitung trend dalam N detik terakhir.

        Returns:
            {
              "direction": "UP" | "DOWN" | "SIDEWAYS",
              "change_pct": float,
              "change_usd": float,
              "data_points": int
            }
        """
        now = time.time()
        cutoff = now - seconds

        # Filter price points dalam window
        recent = [p for p in self.price_history if p["ts"] >= cutoff]

        if len(recent) < 2:
            return {"direction": "SIDEWAYS", "change_pct": 0.0, "change_usd": 0.0, "data_points": len(recent)}

        price_start = recent[0]["price"]
        price_end   = recent[-1]["price"]
        change_usd  = price_end - price_start
        change_pct  = (change_usd / price_start) * 100 if price_start > 0 else 0.0

        # Threshold: >0.05% = trending, <0.05% = sideways
        if change_pct > 0.05:
            direction = "UP"
        elif change_pct < -0.05:
            direction = "DOWN"
        else:
            direction = "SIDEWAYS"

        return {
            "direction": direction,
            "change_pct": change_pct,
            "change_usd": change_usd,
            "data_points": len(recent),
        }

    def get_multi_trend(self) -> dict:
        """Ambil trend di 6 timeframe sekaligus: 1m, 3m, 5m, 10m, 30m, 1h."""
        return {
            "1min":  self.get_trend(60),
            "3min":  self.get_trend(180),
            "5min":  self.get_trend(300),
            "10min": self.get_trend(600),
            "30min": self.get_trend(1800),
            "1hour": self.get_trend(3600),
        }

    def get_30m_range(self) -> dict:
        """
        Ambil high/low harga dalam 30 menit terakhir.
        Berguna untuk tahu posisi harga dalam range (dekat support/resistance).

        Returns:
            {"high": float, "low": float, "range_usd": float}
        """
        now    = time.time()
        cutoff = now - 1800  # 30 menit
        recent = [p["price"] for p in self.price_history if p["ts"] >= cutoff]
        if not recent:
            return {"high": 0.0, "low": 0.0, "range_usd": 0.0}
        high = max(recent)
        low  = min(recent)
        return {"high": high, "low": low, "range_usd": high - low}

    def get_cvd_velocity(self, seconds: int = 60) -> float:
        """
        Hitung rate of change CVD per detik dalam N detik terakhir.
        Positif = CVD naik cepat (bullish momentum kuat).
        Negatif = CVD turun cepat (bearish momentum kuat).

        Returns:
            float: USD per detik perubahan CVD
        """
        now    = time.time()
        cutoff = now - seconds
        recent = [c for c in self.cvd_history if c["ts"] >= cutoff]
        if len(recent) < 2:
            return 0.0
        delta_cvd = recent[-1]["cvd"] - recent[0]["cvd"]
        delta_t   = recent[-1]["ts"]  - recent[0]["ts"]
        if delta_t <= 0:
            return 0.0
        return delta_cvd / delta_t  # USD per detik

    def get_liq_pressure_trend(self, dominant: str, seconds: int = 60) -> str:
        """
        Cek apakah tekanan liquidasi sedang BUILDING, FADING, atau STABLE.

        Args:
            dominant: "SHORT" atau "LONG" — sisi mana yang kita pantau

        Returns:
            "BUILDING" | "FADING" | "STABLE"
        """
        now    = time.time()
        cutoff = now - seconds
        recent = [l for l in self.liq_history if l["ts"] >= cutoff]
        if len(recent) < 4:
            return "STABLE"

        # Bagi jadi dua bagian: paruh pertama vs paruh kedua
        mid   = len(recent) // 2
        key   = "short" if dominant == "SHORT" else "long"
        first_half  = sum(l[key] for l in recent[:mid]) / mid
        second_half = sum(l[key] for l in recent[mid:]) / (len(recent) - mid)

        if second_half > first_half * 1.3:
            return "BUILDING"
        elif second_half < first_half * 0.7:
            return "FADING"
        return "STABLE"

    def is_counter_trend(self, signal_direction: str) -> tuple[bool, str]:
        """
        Cek apakah sinyal berlawanan dengan trend yang ada.

        Returns:
            (is_counter: bool, reason: str)
        """
        trends = self.get_multi_trend()

        t1 = trends["1min"]["direction"]
        t3 = trends["3min"]["direction"]
        t5 = trends["5min"]["direction"]

        chg1 = trends["1min"]["change_pct"]
        chg3 = trends["3min"]["change_pct"]
        chg5 = trends["5min"]["change_pct"]

        # Kasus counter-trend: 2 dari 3 timeframe berlawanan dengan sinyal
        if signal_direction == "DOWN":
            # Bet DOWN saat trend UP = berbahaya
            bullish_count = sum([
                t1 == "UP",
                t3 == "UP",
                t5 == "UP",
            ])
            if bullish_count >= 2:
                reason = (
                    f"Counter-trend DOWN saat market UP: "
                    f"1min={chg1:+.3f}% | 3min={chg3:+.3f}% | 5min={chg5:+.3f}%"
                )
                return True, reason

        elif signal_direction == "UP":
            # Bet UP saat trend DOWN = berbahaya
            bearish_count = sum([
                t1 == "DOWN",
                t3 == "DOWN",
                t5 == "DOWN",
            ])
            if bearish_count >= 2:
                reason = (
                    f"Counter-trend UP saat market DOWN: "
                    f"1min={chg1:+.3f}% | 3min={chg3:+.3f}% | 5min={chg5:+.3f}%"
                )
                return True, reason

        return False, ""


class LearningEngine:
    """
    Engine pembelajaran adaptif.
    Memantau performa dan menyesuaikan parameter secara otomatis.
    """

    def __init__(
        self,
        base_threshold: float,
        log_path: str = "logs/learning_report.csv",
    ):
        self.base_threshold = base_threshold
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        # Statistik per arah
        self.stats = {
            "UP":   DirectionStats("UP"),
            "DOWN": DirectionStats("DOWN"),
        }

        # Trend analyzer
        self.trend = TrendAnalyzer()

        # Window tracking — sudah bet di window ini?
        self._current_window_id: Optional[str] = None
        self._bet_this_window: bool = False

        # CSV log
        self._init_log()

        logger.info(f"[LEARN] Learning engine started. Base threshold: ${base_threshold:,.0f}")

    def _init_log(self):
        """Buat CSV log jika belum ada."""
        if not self.log_path.exists():
            with open(self.log_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "datetime", "direction",
                    "rolling_wr_up", "rolling_wr_down",
                    "consec_loss_up", "consec_loss_down",
                    "threshold_mult_up", "threshold_mult_down",
                    "action", "reason"
                ])

    def record_result(self, direction: str, is_win: bool, btc_entry: float, btc_exit: float):
        """
        Catat hasil bet dan update statistik.
        Dipanggil oleh ResultTracker setelah 5 menit.
        """
        if direction not in self.stats:
            return

        stats = self.stats[direction]
        stats.record(is_win)

        result_str = "WIN" if is_win else "LOSS"
        logger.info(
            f"[LEARN] {direction} {result_str} | "
            f"Rolling WR: {stats.rolling_win_rate:.1%} ({stats.sample_size} samples) | "
            f"Consec loss: {stats.consecutive_losses}"
        )

        # Evaluasi dan sesuaikan parameter
        self._evaluate_and_adapt(direction)

    def _evaluate_and_adapt(self, direction: str):
        """
        Evaluasi performa dan sesuaikan threshold & cooldown.
        Dipanggil setiap kali ada result baru.
        """
        stats = self.stats[direction]

        if stats.sample_size < 3:
            return  # Butuh minimal 3 sample sebelum belajar

        wr = stats.rolling_win_rate
        action = "NO_CHANGE"
        reason = ""

        # ── PENALTI: win rate jelek ──
        if wr < MIN_WIN_RATE_DANGER and stats.sample_size >= 5:
            # Win rate di bawah 40% → threshold naik 2x dan cooldown
            new_mult = min(stats.threshold_multiplier * THRESHOLD_PENALTY, 4.0)  # max 4x
            if new_mult > stats.threshold_multiplier:
                stats.threshold_multiplier = new_mult
                action = "THRESHOLD_UP"
                reason = f"Win rate {wr:.1%} < {MIN_WIN_RATE_DANGER:.0%} threshold → mult {new_mult:.1f}x"
                logger.warning(f"[LEARN] ⚠ {direction} threshold naik ke {new_mult:.1f}x | {reason}")

        # ── COOLDOWN: consecutive losses ──
        if stats.consecutive_losses >= MAX_CONSECUTIVE_LOSS and not stats.in_cooldown:
            stats.cooldown_until = time.time() + COOLDOWN_SECONDS
            action = "COOLDOWN"
            reason = f"{stats.consecutive_losses} consecutive losses → cooldown {COOLDOWN_SECONDS//60}m"
            logger.warning(
                f"[LEARN] ⏸ {direction} cooldown {COOLDOWN_SECONDS//60} menit | "
                f"{stats.consecutive_losses} consecutive losses"
            )

        # ── RECOVERY: win rate pulih ──
        if wr >= 0.60 and stats.threshold_multiplier > 1.0 and stats.sample_size >= 8:
            old_mult = stats.threshold_multiplier
            stats.threshold_multiplier = max(1.0, stats.threshold_multiplier / THRESHOLD_PENALTY)
            action = "THRESHOLD_DOWN"
            reason = f"Win rate recover {wr:.1%} → threshold mult turun {old_mult:.1f}x → {stats.threshold_multiplier:.1f}x"
            logger.info(f"[LEARN] ✓ {direction} threshold turun ke {stats.threshold_multiplier:.1f}x | {reason}")

        # Log ke CSV
        self._write_log(direction, action, reason)

    def _write_log(self, direction: str, action: str, reason: str):
        """Tulis update ke learning_report.csv."""
        now = time.time()
        up_stats   = self.stats["UP"]
        down_stats = self.stats["DOWN"]
        with open(self.log_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now,
                datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
                direction,
                f"{up_stats.rolling_win_rate:.3f}",
                f"{down_stats.rolling_win_rate:.3f}",
                up_stats.consecutive_losses,
                down_stats.consecutive_losses,
                f"{up_stats.threshold_multiplier:.2f}",
                f"{down_stats.threshold_multiplier:.2f}",
                action,
                reason,
            ])

    def get_effective_threshold(self, direction: str) -> float:
        """
        Hitung threshold efektif setelah penalty/multiplier.
        Dipanggil oleh SignalEngine sebelum evaluasi.
        """
        mult = self.stats[direction].threshold_multiplier
        return self.base_threshold * mult

    def is_direction_allowed(self, direction: str) -> tuple[bool, str]:
        """
        Cek apakah direction boleh dieksekusi.
        Mempertimbangkan cooldown.

        Returns:
            (allowed: bool, reason: str)
        """
        stats = self.stats[direction]
        if stats.in_cooldown:
            mins = stats.cooldown_remaining / 60
            reason = (
                f"{direction} dalam cooldown {mins:.1f} menit lagi | "
                f"{stats.consecutive_losses} consecutive losses sebelumnya"
            )
            return False, reason
        return True, ""

    # ── Window tracking ──
    def check_and_set_window(self, window_id: str) -> tuple[bool, str]:
        """
        Cek apakah window ini sudah ada bet-nya.
        window_id bisa berupa condition_id Polymarket ATAU timestamp window 5 menit.

        Returns:
            (can_bet: bool, reason: str)
        """
        if self._current_window_id == window_id and self._bet_this_window:
            return False, f"Sudah ada bet di window {window_id}. Tunggu window berikutnya."
        return True, ""

    def mark_window_used(self, window_id: str):
        """Tandai window ini sudah ada bet."""
        self._current_window_id = window_id
        self._bet_this_window = True

    def advance_window(self, window_id: str):
        """Reset ketika window baru dimulai."""
        if self._current_window_id != window_id:
            self._current_window_id = window_id
            self._bet_this_window = False

    def get_status(self) -> dict:
        """Ringkasan status learning engine untuk dashboard."""
        up   = self.stats["UP"]
        down = self.stats["DOWN"]
        trends = self.trend.get_multi_trend()

        return {
            "up_rolling_wr":     up.rolling_win_rate,
            "up_total_win":      up.total_win,
            "up_total_loss":     up.total_loss,
            "up_consec_loss":    up.consecutive_losses,
            "up_threshold_mult": up.threshold_multiplier,
            "up_in_cooldown":    up.in_cooldown,
            "up_cooldown_rem":   up.cooldown_remaining,

            "down_rolling_wr":     down.rolling_win_rate,
            "down_total_win":      down.total_win,
            "down_total_loss":     down.total_loss,
            "down_consec_loss":    down.consecutive_losses,
            "down_threshold_mult": down.threshold_multiplier,
            "down_in_cooldown":    down.in_cooldown,
            "down_cooldown_rem":   down.cooldown_remaining,

            "trend_1min":  trends["1min"],
            "trend_3min":  trends["3min"],
            "trend_5min":  trends["5min"],
            "trend_10min": trends["10min"],
            "trend_30min": trends["30min"],
            "trend_1hour": trends["1hour"],
        }

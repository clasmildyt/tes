"""
fetcher/candle_tracker.py
=========================
Melacak 5-menit candle BTC yang selaras dengan jadwal Polymarket.

Jadwal window Polymarket (UTC+7 / WIB):
  17:00, 17:05, 17:10, 17:15, 17:20 ... (setiap 5 menit)

Beat Price:
  = Harga penutupan candle sebelumnya
  = Harga BTC tepat saat window baru dimulai
  Bot memprediksi apakah harga PENUTUPAN candle ini
  akan LEBIH TINGGI atau LEBIH RENDAH dari beat price.

Contoh:
  Window 17:05 - 17:10
    Beat price  = harga BTC tepat 17:05:00  (close candle 17:00-17:05)
    Prediksi UP = BTC akan tutup > beat price pada 17:10:00
    Prediksi DN = BTC akan tutup < beat price pada 17:10:00
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Candle:
    """Representasi satu candle 5 menit."""
    window_id:    str    # Format: "HH:MM" (e.g. "17:05")
    open_price:   float  # Harga pembukaan (= beat price window ini)
    high_price:   float  # Harga tertinggi dalam window
    low_price:    float  # Harga terendah dalam window
    close_price:  float  # Harga penutupan (= beat price window berikutnya)
    open_ts:      float  # Unix timestamp saat window dimulai
    close_ts:     float  # Unix timestamp saat window berakhir
    beat_price:   float  # = open_price (harga yang harus dikalahkan)

    @property
    def change_pct(self) -> float:
        """Perubahan harga dalam persen dari open ke close."""
        if self.open_price <= 0:
            return 0.0
        return (self.close_price - self.open_price) / self.open_price * 100

    @property
    def direction(self) -> str:
        """Arah candle: UP jika close > open, DOWN jika close < open."""
        if self.close_price > self.open_price:
            return "UP"
        elif self.close_price < self.open_price:
            return "DOWN"
        return "FLAT"


class CandleTracker:
    """
    Melacak candle 5 menit yang selaras dengan jadwal Polymarket.

    Cara kerja:
      - Cek setiap update apakah window baru sudah dimulai
      - Saat window berganti → simpan candle selesai, catat beat price baru
      - Menyediakan: beat_price, time_remaining, window_id, candle history
    """

    def __init__(self, history_size: int = 20):
        self.candle_history: deque = deque(maxlen=history_size)

        # State window saat ini
        self._current_window_id: str   = ""
        self._window_open_price: float = 0.0
        self._window_open_ts:    float = 0.0
        self._window_high:       float = 0.0
        self._window_low:        float = float("inf")
        self._last_price:        float = 0.0

        # Beat price = harga open window saat ini (close candle sebelumnya)
        self.beat_price:         float = 0.0
        self.beat_price_ts:      float = 0.0

        self._initialized = False

    # ──────────────────────────────────────────
    # Window helpers (static / class-level)
    # ──────────────────────────────────────────

    @staticmethod
    def get_window_id(dt: datetime = None) -> str:
        """
        Hitung window ID untuk waktu tertentu.
        Window ID = string "HH:MM" dibulatkan ke 5 menit ke bawah.

        Contoh:
          17:06:43 → "17:05"
          17:10:00 → "17:10"
          17:04:59 → "17:00"
        """
        if dt is None:
            dt = datetime.now()
        minute_block = (dt.minute // 5) * 5
        return dt.strftime(f"%H:{minute_block:02d}")

    @staticmethod
    def get_window_start(dt: datetime = None) -> datetime:
        """Kembalikan datetime tepat saat window dimulai."""
        if dt is None:
            dt = datetime.now()
        minute_block = (dt.minute // 5) * 5
        return dt.replace(minute=minute_block, second=0, microsecond=0)

    @staticmethod
    def get_window_end(dt: datetime = None) -> datetime:
        """Kembalikan datetime tepat saat window berakhir."""
        return CandleTracker.get_window_start(dt) + timedelta(minutes=5)

    @staticmethod
    def get_time_elapsed() -> int:
        """Detik yang sudah berjalan dalam window saat ini (0-300)."""
        now = datetime.now()
        ws  = CandleTracker.get_window_start(now)
        return int((now - ws).total_seconds())

    @staticmethod
    def get_time_remaining() -> int:
        """Detik tersisa dalam window saat ini (0-300)."""
        return max(0, 300 - CandleTracker.get_time_elapsed())

    @staticmethod
    def get_window_progress_pct() -> float:
        """Persentase window yang sudah berjalan (0.0 - 100.0)."""
        return CandleTracker.get_time_elapsed() / 300 * 100

    # ──────────────────────────────────────────
    # Update state
    # ──────────────────────────────────────────

    def override_beat_price(self, chainlink_price: float):
        """
        Override beat price window saat ini dengan harga Chainlink.
        Dipanggil setelah window baru terdeteksi untuk menyelaraskan
        beat price dengan oracle yang dipakai Polymarket.

        Args:
            chainlink_price: Harga BTC/USD dari Chainlink oracle
        """
        if chainlink_price > 0 and self._initialized:
            old = self.beat_price
            self.beat_price         = chainlink_price
            self._window_open_price = chainlink_price
            logger.info(
                f"[CANDLE] Beat price override Chainlink: "
                f"${old:,.2f} → ${chainlink_price:,.2f}"
            )

    def update(self, btc_price: float) -> Optional[Candle]:
        """
        Update dengan harga BTC terbaru.
        Dipanggil setiap kali ada harga baru (dari WS atau polling).

        Returns:
            Candle yang baru selesai jika window baru dimulai, None otherwise.
        """
        if btc_price <= 0:
            return None

        self._last_price = btc_price
        current_wid = self.get_window_id()
        now_ts = time.time()
        completed_candle = None

        if not self._initialized:
            # Inisialisasi pertama kali
            self._current_window_id = current_wid
            self._window_open_price = btc_price
            self._window_open_ts    = now_ts
            self._window_high       = btc_price
            self._window_low        = btc_price
            self.beat_price         = btc_price
            self.beat_price_ts      = now_ts
            self._initialized       = True
            logger.info(
                f"[CANDLE] Initialized | Window: {current_wid} | "
                f"Beat price: ${btc_price:,.2f}"
            )

        elif current_wid != self._current_window_id:
            # ── Window baru dimulai ──

            # Simpan candle yang baru selesai
            completed_candle = Candle(
                window_id   = self._current_window_id,
                open_price  = self._window_open_price,
                high_price  = self._window_high,
                low_price   = self._window_low,
                close_price = btc_price,   # close = harga pertama window baru
                open_ts     = self._window_open_ts,
                close_ts    = now_ts,
                beat_price  = self._window_open_price,
            )
            self.candle_history.append(completed_candle)

            logger.info(
                f"[CANDLE] ✓ Candle {completed_candle.window_id} selesai | "
                f"O:{completed_candle.open_price:,.2f} "
                f"H:{completed_candle.high_price:,.2f} "
                f"L:{completed_candle.low_price:,.2f} "
                f"C:{completed_candle.close_price:,.2f} | "
                f"{completed_candle.direction} {completed_candle.change_pct:+.3f}%"
            )

            # Update beat price untuk window baru
            old_beat = self.beat_price
            self.beat_price         = btc_price
            self.beat_price_ts      = now_ts
            self._current_window_id = current_wid
            self._window_open_price = btc_price
            self._window_open_ts    = now_ts
            self._window_high       = btc_price
            self._window_low        = btc_price

            logger.info(
                f"[CANDLE] New window: {current_wid} | "
                f"Beat price: ${self.beat_price:,.2f} "
                f"(prev: ${old_beat:,.2f})"
            )

        else:
            # Update candle yang sedang berjalan
            self._window_high = max(self._window_high, btc_price)
            self._window_low  = min(self._window_low,  btc_price)

        return completed_candle

    # ──────────────────────────────────────────
    # Info helpers
    # ──────────────────────────────────────────

    @property
    def current_window_id(self) -> str:
        return self._current_window_id

    @property
    def current_open(self) -> float:
        return self._window_open_price

    @property
    def current_high(self) -> float:
        return self._window_high

    @property
    def current_low(self) -> float:
        return self._window_low

    @property
    def current_price(self) -> float:
        return self._last_price

    @property
    def vs_beat_pct(self) -> float:
        """Selisih harga saat ini vs beat price dalam persen."""
        if self.beat_price <= 0:
            return 0.0
        return (self._last_price - self.beat_price) / self.beat_price * 100

    @property
    def vs_beat_usd(self) -> float:
        """Selisih harga saat ini vs beat price dalam USD."""
        return self._last_price - self.beat_price

    @property
    def current_direction(self) -> str:
        """Arah sementara candle saat ini vs beat price."""
        if self.vs_beat_usd > 0:
            return "UP"
        elif self.vs_beat_usd < 0:
            return "DOWN"
        return "FLAT"

    def get_last_n_candles(self, n: int = 5) -> list:
        """Ambil N candle terakhir yang sudah selesai."""
        history = list(self.candle_history)
        return history[-n:] if len(history) >= n else history

    def get_candle_streak(self, n: int = 3) -> str:
        """
        Ambil arah N candle terakhir sebagai string.
        Contoh: "UP,DOWN,UP" atau "DOWN,DOWN,DOWN"

        Returns:
            str: Arah candle dipisah koma, atau "" jika belum cukup data.
        """
        candles = self.get_last_n_candles(n)
        if not candles:
            return ""
        return ",".join(c.direction for c in candles)

    def get_volatility_avg(self, n: int = 5) -> float:
        """
        Hitung rata-rata range (high - low) dari N candle terakhir dalam USD.
        Dipakai sebagai ukuran volatilitas mikro market saat ini.

        Returns:
            float: Rata-rata range candle dalam USD, 0.0 jika belum cukup data.
        """
        candles = self.get_last_n_candles(n)
        if not candles:
            return 0.0
        ranges = [c.high_price - c.low_price for c in candles if c.high_price > 0]
        return sum(ranges) / len(ranges) if ranges else 0.0

    def get_status(self) -> dict:
        """Ringkasan lengkap untuk dashboard."""
        return {
            "window_id":        self._current_window_id,
            "beat_price":       self.beat_price,
            "current_price":    self._last_price,
            "current_high":     self._window_high,
            "current_low":      self._window_low,
            "vs_beat_pct":      self.vs_beat_pct,
            "vs_beat_usd":      self.vs_beat_usd,
            "current_direction":self.current_direction,
            "time_elapsed":     self.get_time_elapsed(),
            "time_remaining":   self.get_time_remaining(),
            "progress_pct":     self.get_window_progress_pct(),
            "candles_recorded": len(self.candle_history),
        }

"""
engine/blocked_tracker.py
==========================
Melacak sinyal yang diblokir oleh filter (F2/F3/F4) dan
menentukan apakah sinyal tersebut "would have won" setelah
window berakhir.

Berguna untuk analisis kualitas filter:
  - Filter F4D memblokir 20 sinyal
    → 17 would_have_won = NO  (85% tepat → filter valid ✅)
    → 3  would_have_won = YES (15% salah → acceptable)

Output: logs/blocked_signals.csv
"""

import asyncio
import csv
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from fetcher.hyperliquid_ws import MarketData

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data structure
# ─────────────────────────────────────────────
@dataclass
class BlockedSignal:
    """Satu sinyal yang diblokir oleh filter."""
    signal_id:      str    # unique ID
    direction:      str    # "UP" atau "DOWN"
    beat_price:     float  # harga referensi Polymarket saat sinyal
    btc_at_block:   float  # harga BTC saat sinyal diblokir
    window_id:      str    # ID window Polymarket ("HH:MM")
    window_end_ts:  float  # unix timestamp kapan window berakhir
    blocked_by:     str    # "F2" / "F3" / "F4A" / "F4B" / "F4C" / "F4D"
    blocked_reason: str    # teks lengkap alasan
    block_time:     float  # unix timestamp saat diblokir
    context:        dict   = field(default_factory=dict)


# ─────────────────────────────────────────────
# Blocked Signal Tracker
# ─────────────────────────────────────────────
class BlockedTracker:
    """
    Menerima blocked signal, menunggu window berakhir,
    lalu resolve (would_have_won?) dan tulis ke CSV.
    """

    def __init__(
        self,
        market_data: MarketData,
        log_path: str = "logs/blocked_signals.csv",
        brain=None,   # AdaptiveBrain instance (opsional)
    ):
        self.data     = market_data
        self.log_path = Path(log_path)
        self.brain    = brain   # akan dipanggil saat would_win=YES
        self._pending: dict[str, BlockedSignal] = {}
        self._running = False
        self._stats   = {"total": 0, "would_win": 0, "would_loss": 0}
        self._init_csv()

    def _init_csv(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_path.exists():
            with open(self.log_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    # ── Identitas ──
                    "signal_id", "direction", "window_id",
                    # ── Harga ──
                    "beat_price", "btc_at_block", "btc_exit",
                    "price_vs_beat_at_block",  # btc_at_block - beat_price
                    "exit_vs_beat_usd",        # btc_exit - beat_price
                    # ── Filter ──
                    "blocked_by", "blocked_reason",
                    # ── Waktu ──
                    "block_time", "window_end_time",
                    # ── Hasil Hipotetis ──
                    "would_have_won",  # YES / NO
                    # ── Context (jika tersedia, khususnya F4) ──
                    "session", "hour_of_day", "day_of_week",
                    "window_elapsed_sec",
                    "liq_short_usd", "liq_long_usd", "dominance_ratio",
                    "cvd_value", "cvd_velocity", "oi_change_pct",
                    "trend_1m_dir", "trend_1m_pct",
                    "trend_3m_dir", "trend_3m_pct",
                    "trend_5m_dir", "trend_5m_pct",
                    "trend_10m_dir", "trend_10m_pct",
                    "trend_30m_dir", "trend_30m_pct",
                    "trend_1h_dir", "trend_1h_pct",
                    "price_vs_30m_range_pct",
                    "confirmation_delta_usd",
                    "candle_streak",
                    "funding_rate",
                ])
            logger.info(f"[BLOCKED] Created: {self.log_path}")

    def add(
        self,
        direction:      str,
        beat_price:     float,
        btc_at_block:   float,
        window_id:      str,
        window_end_ts:  float,
        blocked_by:     str,
        blocked_reason: str,
        context:        dict = None,
    ):
        """
        Daftarkan sinyal yang baru diblokir.
        Akan di-resolve otomatis saat window berakhir.

        Dedup: satu window hanya perlu satu blocked signal per direction+filter.
        Kalau sudah ada pending dengan window_id + direction + blocked_by yang sama → skip.
        """
        for existing in self._pending.values():
            if (existing.window_id == window_id
                    and existing.direction == direction
                    and existing.blocked_by == blocked_by):
                logger.debug(
                    f"[BLOCKED] Dedup skip {direction} {window_id} {blocked_by} "
                    f"(sudah ada pending)"
                )
                return

        sig_id = f"{int(time.time())}-{direction}-{window_id}-{blocked_by}"

        blocked = BlockedSignal(
            signal_id      = sig_id,
            direction      = direction,
            beat_price     = beat_price,
            btc_at_block   = btc_at_block,
            window_id      = window_id,
            window_end_ts  = window_end_ts,
            blocked_by     = blocked_by,
            blocked_reason = blocked_reason,
            block_time     = time.time(),
            context        = context or {},
        )
        self._pending[sig_id] = blocked
        logger.info(
            f"[BLOCKED] +{direction} {window_id} oleh {blocked_by} | "
            f"{blocked_reason[:60]}"
        )

    async def run_loop(self):
        """Loop utama: resolve blocked signals saat window berakhir."""
        self._running = True
        while self._running:
            now = time.time()
            to_resolve = [
                b for b in list(self._pending.values())
                if now >= b.window_end_ts + 2   # +2s buffer
            ]
            for b in to_resolve:
                await self._resolve(b)
            await asyncio.sleep(2)

    async def _resolve(self, b: BlockedSignal):
        """Tentukan apakah sinyal yang diblokir seharusnya menang."""
        btc_exit = self.data.btc_price
        if btc_exit <= 0:
            return   # data belum tersedia, tunggu dulu

        # Tentukan would_have_won berdasarkan beat price
        if b.direction == "UP":
            would_win = btc_exit > b.beat_price
        else:
            would_win = btc_exit < b.beat_price

        # Update stats
        self._stats["total"] += 1
        if would_win:
            self._stats["would_win"] += 1
        else:
            self._stats["would_loss"] += 1

        result_str = "YES" if would_win else "NO"
        ctx = b.context

        with open(self.log_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                # Identitas
                b.signal_id, b.direction, b.window_id,
                # Harga
                round(b.beat_price, 2),
                round(b.btc_at_block, 2),
                round(btc_exit, 2),
                round(b.btc_at_block - b.beat_price, 2),
                round(btc_exit - b.beat_price, 2),
                # Filter
                b.blocked_by,
                b.blocked_reason[:100],
                # Waktu
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(b.block_time)),
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(b.window_end_ts)),
                # Hasil
                result_str,
                # Context
                ctx.get("session", ""),
                ctx.get("hour_of_day", ""),
                ctx.get("day_of_week", ""),
                ctx.get("window_elapsed_sec", ""),
                ctx.get("liq_short_usd", ""),
                ctx.get("liq_long_usd", ""),
                ctx.get("dominance_ratio", ""),
                ctx.get("cvd_value", ""),
                ctx.get("cvd_velocity", ""),
                ctx.get("oi_change_pct", ""),
                ctx.get("trend_1m_dir", ""), ctx.get("trend_1m_pct", ""),
                ctx.get("trend_3m_dir", ""), ctx.get("trend_3m_pct", ""),
                ctx.get("trend_5m_dir", ""), ctx.get("trend_5m_pct", ""),
                ctx.get("trend_10m_dir", ""), ctx.get("trend_10m_pct", ""),
                ctx.get("trend_30m_dir", ""), ctx.get("trend_30m_pct", ""),
                ctx.get("trend_1h_dir", ""), ctx.get("trend_1h_pct", ""),
                ctx.get("price_vs_30m_range_pct", ""),
                ctx.get("confirmation_delta_usd", ""),
                ctx.get("candle_streak", ""),
                ctx.get("funding_rate", ""),
            ])

        icon = "✅" if would_win else "❌"
        logger.info(
            f"[BLOCKED] {icon} Resolved {b.direction} {b.window_id} "
            f"({b.blocked_by}) → would_have_won={result_str} "
            f"exit=${btc_exit:,.0f} beat=${b.beat_price:,.0f}"
        )

        # Notifikasi Adaptive Brain jika would_win=YES
        if would_win and self.brain is not None:
            ctx = dict(b.context)
            ctx["window_id"] = b.window_id
            try:
                self.brain.notify_would_win(
                    signal_id  = b.signal_id,
                    direction  = b.direction,
                    blocked_by = b.blocked_by,
                    context    = ctx,
                )
            except Exception as e:
                logger.debug(f"[BLOCKED] Brain notify error: {e}")

        del self._pending[b.signal_id]

    @property
    def stats(self) -> dict:
        """Statistik blocked signals untuk dashboard."""
        total = self._stats["total"]
        win   = self._stats["would_win"]
        loss  = self._stats["would_loss"]
        acc   = (loss / total * 100) if total > 0 else 0.0  # filter accuracy
        return {
            "total"        : total,
            "would_win"    : win,
            "would_loss"   : loss,
            "filter_acc_pct": round(acc, 1),   # % yang benar diblokir (would_loss)
            "pending"      : len(self._pending),
        }

    def stop(self):
        self._running = False
        logger.info("[BLOCKED] Stopped.")

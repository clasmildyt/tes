"""
risk/manager.py
===============
Risk Manager — filter semua bet sebelum diteruskan ke executor.

Rules:
  1. Max 1 bet per window 5 menit (satu condition_id = satu bet)
  2. Max bet = % dari total balance (default 10%)
  3. Max 5 bet per jam (rolling 60 menit)
  4. Daily stop loss: jika loss >= threshold → tandai hit (main.py akan switch ke MANUAL)
  5. Min odds = 0 (disabled) — semua odds diterima
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Structs
# ─────────────────────────────────────────────
class BlockReason(str, Enum):
    ALLOWED            = "ALLOWED"
    DUPLICATE_WINDOW   = "DUPLICATE_WINDOW"   # sudah bet di window ini
    MAX_BET_PER_HOUR   = "MAX_BET_PER_HOUR"   # sudah 5 bet dalam 1 jam
    DAILY_STOP_LOSS    = "DAILY_STOP_LOSS"     # loss harian >= threshold → switch MANUAL
    ZERO_BALANCE       = "ZERO_BALANCE"        # balance nol


@dataclass
class RiskDecision:
    """Hasil evaluasi risk manager untuk satu request bet."""
    allowed: bool
    reason: BlockReason
    bet_amount: float       # jumlah USDC yang disetujui (bisa lebih kecil dari request)
    message: str            # pesan log yang informatif


@dataclass
class BetRecord:
    """Catatan bet yang sudah dilakukan (untuk tracking)."""
    timestamp: float
    condition_id: str       # ID window Polymarket
    direction: str
    amount_usdc: float
    odds: float
    pnl: float = 0.0        # diisi setelah market settled (opsional)


# ─────────────────────────────────────────────
# Risk Manager
# ─────────────────────────────────────────────
class RiskManager:
    """
    Semua bet harus melewati RiskManager.check() sebelum dikirim ke executor.
    Jika check() return allowed=False, executor tidak boleh place bet.
    """

    def __init__(
        self,
        max_bet_pct: float   = 0.10,    # 10% dari balance per bet
        max_bets_per_hour: int = 5,      # max 5 bet per jam
        daily_stop_loss: float = 0.50,   # switch MANUAL jika loss >= 50% balance awal hari
        min_odds: float        = 0.0,    # 0 = disabled (semua odds diterima)
    ):
        self.max_bet_pct       = max_bet_pct
        self.max_bets_per_hour = max_bets_per_hour
        self.daily_stop_loss   = daily_stop_loss
        self.min_odds          = min_odds

        # ── State tracking ──
        # Set condition_id yang sudah dibet hari ini
        self._bet_windows: set[str]  = set()

        # Deque timestamp bet dalam 60 menit terakhir (untuk rate limit)
        self._hourly_bets: deque     = deque()

        # Tracking balance & PnL harian
        self._balance_start_of_day: float = 0.0   # set saat pertama kali dapat balance
        self._daily_pnl: float            = 0.0   # diupdate manual via record_pnl()
        self._today: date                 = date.today()

        # History semua bet (untuk audit)
        self._bet_history: list[BetRecord] = []

        logger.info(
            f"[RISK] Initialized: max_bet={max_bet_pct*100:.0f}% | "
            f"max/hour={max_bets_per_hour} | "
            f"daily_stop={daily_stop_loss*100:.0f}% | "
            f"min_odds={'disabled' if min_odds == 0 else min_odds}"
        )

    # ─────────────────────────────────────────
    # Main check — panggil ini sebelum bet
    # ─────────────────────────────────────────
    def check(
        self,
        condition_id: str,
        direction: str,
        odds: float,
        current_balance: float,
    ) -> RiskDecision:
        """
        Evaluasi apakah bet boleh dilakukan.

        Args:
            condition_id    : ID market/window dari Polymarket (unik per 5 menit)
            direction       : "UP" atau "DOWN"
            odds            : harga token (0-1)
            current_balance : saldo USDC saat ini

        Returns:
            RiskDecision dengan allowed=True/False dan jumlah bet yang disetujui
        """
        self._reset_daily_state_if_new_day()
        self._cleanup_hourly_window()

        # Inisialisasi balance awal hari jika belum
        if self._balance_start_of_day == 0 and current_balance > 0:
            self._balance_start_of_day = current_balance
            logger.info(f"[RISK] Balance awal hari: ${current_balance:.2f}")

        # ── Rule 1: Balance tidak boleh nol ──
        if current_balance <= 0:
            return RiskDecision(
                allowed    = False,
                reason     = BlockReason.ZERO_BALANCE,
                bet_amount = 0.0,
                message    = "Balance = $0, tidak bisa bet",
            )

        # ── Rule 2: Daily stop loss ──
        if self._balance_start_of_day > 0:
            loss_pct = -self._daily_pnl / self._balance_start_of_day
            if loss_pct >= self.daily_stop_loss:
                return RiskDecision(
                    allowed    = False,
                    reason     = BlockReason.DAILY_STOP_LOSS,
                    bet_amount = 0.0,
                    message    = (
                        f"Daily stop loss tercapai: "
                        f"loss ${-self._daily_pnl:.2f} "
                        f"({loss_pct*100:.1f}% dari balance awal ${self._balance_start_of_day:.2f})"
                    ),
                )

        # ── Rule 3: Max bet per jam ──
        if len(self._hourly_bets) >= self.max_bets_per_hour:
            oldest = self._hourly_bets[0]
            wait   = 3600 - (time.time() - oldest)
            return RiskDecision(
                allowed    = False,
                reason     = BlockReason.MAX_BET_PER_HOUR,
                bet_amount = 0.0,
                message    = (
                    f"Rate limit: sudah {len(self._hourly_bets)} bet dalam 1 jam. "
                    f"Tunggu {wait/60:.1f} menit."
                ),
            )

        # ── Rule 4: Satu bet per window (condition_id) ──
        if condition_id in self._bet_windows:
            return RiskDecision(
                allowed    = False,
                reason     = BlockReason.DUPLICATE_WINDOW,
                bet_amount = 0.0,
                message    = (
                    f"Sudah ada bet di window ini "
                    f"(condition_id: {condition_id[:16]}...). "
                    f"Tunggu window 5 menit berikutnya."
                ),
            )

        # ── Hitung bet amount (2% dari balance) ──
        bet_amount = round(current_balance * self.max_bet_pct, 2)
        bet_amount = max(bet_amount, 0.01)  # minimum $0.01

        return RiskDecision(
            allowed    = True,
            reason     = BlockReason.ALLOWED,
            bet_amount = bet_amount,
            message    = (
                f"OK | ${bet_amount:.2f} ({self.max_bet_pct*100:.0f}% x ${current_balance:.2f}) | "
                f"Bets jam ini: {len(self._hourly_bets)}/{self.max_bets_per_hour} | "
                f"Windows hari ini: {len(self._bet_windows)}"
            ),
        )

    # ─────────────────────────────────────────
    # Catat bet yang sudah dieksekusi
    # ─────────────────────────────────────────
    def record_bet(
        self,
        condition_id: str,
        direction: str,
        amount_usdc: float,
        odds: float,
    ):
        """
        Dipanggil SETELAH bet berhasil dieksekusi.
        Update semua state tracking.
        """
        now = time.time()

        # Tandai window ini sudah digunakan
        self._bet_windows.add(condition_id)

        # Tambah ke rolling hourly counter
        self._hourly_bets.append(now)

        # Simpan ke history
        self._bet_history.append(BetRecord(
            timestamp    = now,
            condition_id = condition_id,
            direction    = direction,
            amount_usdc  = amount_usdc,
            odds         = odds,
        ))

        logger.info(
            f"[RISK] Bet recorded: {direction} ${amount_usdc:.2f} @ {odds:.3f} | "
            f"Window locked: {condition_id[:16]}... | "
            f"Hourly: {len(self._hourly_bets)}/{self.max_bets_per_hour} | "
            f"Windows today: {len(self._bet_windows)}"
        )

    def record_pnl(self, pnl: float):
        """
        Update PnL harian setelah market settled.
        pnl positif = profit, negatif = loss.
        """
        self._daily_pnl += pnl
        loss_pct = -self._daily_pnl / self._balance_start_of_day * 100 if self._balance_start_of_day else 0
        logger.info(
            f"[RISK] PnL updated: ${pnl:+.2f} | "
            f"Daily PnL: ${self._daily_pnl:+.2f} | "
            f"vs stop loss: {loss_pct:.1f}%"
        )

    # ─────────────────────────────────────────
    # Status & info
    # ─────────────────────────────────────────
    def get_status(self, current_balance: float) -> dict:
        """Return ringkasan status risk manager untuk dashboard."""
        self._cleanup_hourly_window()
        loss_pct = (
            -self._daily_pnl / self._balance_start_of_day * 100
            if self._balance_start_of_day > 0 else 0.0
        )
        stop_loss_budget = (
            self._balance_start_of_day * self.daily_stop_loss + self._daily_pnl
            if self._balance_start_of_day > 0 else 0.0
        )
        return {
            "balance"            : current_balance,
            "balance_start_day"  : self._balance_start_of_day,
            "daily_pnl"          : self._daily_pnl,
            "daily_loss_pct"     : loss_pct,
            "stop_loss_pct"      : self.daily_stop_loss * 100,
            "stop_loss_budget"   : stop_loss_budget,   # berapa lagi bisa loss sebelum stop
            "bets_this_hour"     : len(self._hourly_bets),
            "max_bets_per_hour"  : self.max_bets_per_hour,
            "windows_today"      : len(self._bet_windows),
            "next_bet_size"      : round(current_balance * self.max_bet_pct, 2),
            "trading_allowed"    : loss_pct < self.daily_stop_loss * 100,
        }

    def is_window_used(self, condition_id: str) -> bool:
        """Return True jika window ini sudah ada betnya."""
        return condition_id in self._bet_windows

    # ─────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────
    def _cleanup_hourly_window(self):
        """Hapus bet yang sudah lebih dari 60 menit dari hourly counter."""
        cutoff = time.time() - 3600
        while self._hourly_bets and self._hourly_bets[0] < cutoff:
            self._hourly_bets.popleft()

    def _reset_daily_state_if_new_day(self):
        """Reset state harian jika sudah ganti hari."""
        today = date.today()
        if today != self._today:
            logger.info(
                f"[RISK] New day detected. Reset daily state. "
                f"Previous PnL: ${self._daily_pnl:+.2f}"
            )
            self._today                = today
            self._bet_windows          = set()
            self._daily_pnl            = 0.0
            self._balance_start_of_day = 0.0

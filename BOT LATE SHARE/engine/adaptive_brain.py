"""
engine/adaptive_brain.py
========================
Adaptive Brain — memantau blocked signals (would win) dan
secara otomatis mengganti profil strategi bot berdasarkan kondisi market.

Konsep:
  1. BlockedTracker memberi tahu Brain setiap sinyal "would win" di-resolve
  2. Brain kumpulkan pola: filter mana + regime apa yang blokir winning signals
  3. Setelah 3 would-win dari pola yang sama → switch profil
  4. Cooldown 2 window (10 menit) setelah switch
  5. Evaluasi profil baru setelah 4 bet resolved
  6. Catat semua keputusan ke logs/brain_report.csv
  7. Catat hasil evaluasi ke logs/brain_profile_results.csv

Profil yang tersedia:
  STANDARD        — baseline, balanced
  FAST_TREND      — F3 longgar, untuk market trending kuat
  CONSERVATIVE    — F3 ketat, untuk market noisy/choppy
  MOMENTUM_HUNTER — selektif, hanya dominance sangat tinggi
"""

import csv
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Profil Strategi
# ─────────────────────────────────────────────
STRATEGY_PROFILES: dict = {
    "STANDARD": {
        "CONFIRM_MIN_MOVE"    : 10.0,
        "MOMENTUM_MAX_AGAINST": 60.0,
        "MIN_WINDOW_REMAINING": 120,
        "F4D_MACRO_RATIO_MIN" : 6.0,
        "description"         : "Baseline — balanced untuk kondisi normal",
    },
    "FAST_TREND": {
        "CONFIRM_MIN_MOVE"    : 5.0,
        "MOMENTUM_MAX_AGAINST": 80.0,
        "MIN_WINDOW_REMAINING": 120,
        "F4D_MACRO_RATIO_MIN" : 4.0,
        "description"         : "Market trending kuat — F3 longgar, momentum toleran",
    },
    "CONSERVATIVE": {
        "CONFIRM_MIN_MOVE"    : 15.0,
        "MOMENTUM_MAX_AGAINST": 40.0,
        "MIN_WINDOW_REMAINING": 150,
        "F4D_MACRO_RATIO_MIN" : 8.0,
        "description"         : "Market noisy/choppy — filter ketat, hindari false signal",
    },
    "MOMENTUM_HUNTER": {
        "CONFIRM_MIN_MOVE"    : 7.0,
        "MOMENTUM_MAX_AGAINST": 70.0,
        "MIN_WINDOW_REMAINING": 120,
        "F4D_MACRO_RATIO_MIN" : 8.0,
        "description"         : "Khusus dominance >8x — selektif tapi agresif",
    },
}


def _get_regime(cvd_velocity: float) -> str:
    """Klasifikasi regime market dari CVD velocity."""
    abs_vel = abs(cvd_velocity)
    if abs_vel > 50_000:
        return "FAST"
    elif abs_vel > 15_000:
        return "NORMAL"
    else:
        return "CHOPPY"


def _select_profile(blocked_by: str, regime: str, dominance_ratio: float) -> str:
    """
    Pilih profil terbaik berdasarkan pola yang terdeteksi.

    Logic:
      F3 diblokir (regime apapun) → FAST_TREND (F3=$5, lebih toleran)
          - FAST/NORMAL: gerakan besar tapi konfirmasi terlalu lama
          - CHOPPY     : gerakan kecil sehingga $10 terlalu besar untuk dicapai
      F2 diblokir                 → FAST_TREND (momentum terlalu ketat)
      F4D diblokir + dom tinggi   → MOMENTUM_HUNTER
      F4A/B/C diblokir            → CONSERVATIVE (tambah selektivitas)
      Lainnya                     → STANDARD
    """
    if blocked_by == "F3":
        # F3 memblokir sinyal yang would-win di SEMUA regime:
        # → turunkan threshold konfirmasi ke $5 (FAST_TREND)
        return "FAST_TREND"
    if blocked_by == "F2":
        return "FAST_TREND"
    if blocked_by == "F4D" and dominance_ratio > 6.0:
        return "MOMENTUM_HUNTER"
    if blocked_by in ("F4A", "F4B", "F4C"):
        return "CONSERVATIVE"   # filter advanced terlalu agresif → jadikan lebih ketat tapi
                                 # biarkan entry kalau konfirmasi kuat
    return "STANDARD"


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────
@dataclass
class WouldWinEvent:
    """Satu kejadian blocked signal yang would have won."""
    signal_id      : str
    direction      : str
    blocked_by     : str
    regime         : str
    dominance_ratio: float
    session        : str
    window_id      : str
    timestamp      : float = field(default_factory=time.time)


@dataclass
class BrainDecision:
    """Satu keputusan brain: ganti profil."""
    from_profile       : str
    to_profile         : str
    reason             : str
    pattern_blocked_by : str
    pattern_regime     : str
    window_id          : str
    timestamp          : float = field(default_factory=time.time)
    result             : str   = "─"    # "WIN" / "LOSS" / "─" (belum cukup data)
    bets_after         : int   = 0


# ─────────────────────────────────────────────
# Adaptive Brain
# ─────────────────────────────────────────────
class AdaptiveBrain:
    """
    Memantau blocked signals (would win) dan mengganti profil strategi otomatis.

    Integrasi:
        brain = AdaptiveBrain()

        # Di BlockedTracker._resolve() saat would_win=YES:
        brain.notify_would_win(signal_id, direction, blocked_by, context)

        # Di handler._on_resolved() setiap bet selesai:
        brain.notify_bet_resolved(is_win)

        # Di _confirm_and_bet() baca parameter aktif:
        threshold = brain.get_param("CONFIRM_MIN_MOVE")
    """

    WOULD_WIN_TRIGGER = 3      # min would-win dari pola sama → switch
    COOLDOWN_WINDOWS  = 2      # window cooldown setelah switch
    EVAL_AFTER_BETS   = 4      # evaluasi profil setelah N bet resolved
    WINDOW_DURATION   = 300    # detik per window (5 menit)
    RECENT_WINDOW_SEC = 1800   # analisis 30 menit terakhir

    def __init__(self, log_dir: str = "logs"):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # State profil aktif
        self._active_profile    = "STANDARD"
        self._previous_profile  = "STANDARD"   # untuk revert
        self._profile_since     = time.time()
        self._profile_window    = ""
        self._cooldown_until    = 0.0

        # Buffer would-win events (max 50 entry)
        self._would_win_buffer: deque = deque(maxlen=50)

        # Counter evaluasi profil
        self._bets_since_switch   = 0
        self._wins_since_switch   = 0
        self._losses_since_switch = 0

        # Early revert state
        self._early_revert_triggered: bool = False

        # Riwayat keputusan (max 8 terakhir untuk display)
        self._history: deque               = deque(maxlen=8)
        self._current_decision: Optional[BrainDecision] = None

        # CSV paths
        self._report_path  = self._log_dir / "brain_report.csv"
        self._results_path = self._log_dir / "brain_profile_results.csv"
        self._init_csvs()

        logger.info(
            f"[BRAIN] Adaptive Brain initialized | "
            f"Profil: {self._active_profile} | "
            f"Trigger: {self.WOULD_WIN_TRIGGER} would-win | "
            f"Cooldown: {self.COOLDOWN_WINDOWS} window | "
            f"Eval: {self.EVAL_AFTER_BETS} bet"
        )

    # ─────────────────────────────────────────────
    # CSV Init
    # ─────────────────────────────────────────────
    def _init_csvs(self):
        if not self._report_path.exists():
            with open(self._report_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "timestamp", "from_profile", "to_profile",
                    "reason", "pattern_blocked_by", "pattern_regime",
                    "would_win_count", "window_id",
                ])

        if not self._results_path.exists():
            with open(self._results_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "timestamp", "profile", "bets_evaluated",
                    "wins", "losses", "win_rate_pct",
                    "verdict",  # EFFECTIVE / NEUTRAL / INEFFECTIVE
                    "action",   # KEEP / LOG_FOR_REVIEW
                ])

    # ─────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────
    def get_param(self, key: str, fallback=None):
        """Baca parameter dari profil aktif."""
        return STRATEGY_PROFILES[self._active_profile].get(key, fallback)

    @property
    def active_profile(self) -> str:
        return self._active_profile

    @property
    def profile_description(self) -> str:
        return STRATEGY_PROFILES[self._active_profile]["description"]

    @property
    def profile_since_str(self) -> str:
        return time.strftime("%H:%M", time.localtime(self._profile_since))

    def notify_would_win(
        self,
        signal_id : str,
        direction : str,
        blocked_by: str,
        context   : dict,
    ):
        """
        Dipanggil BlockedTracker setiap blocked signal would have won.
        Brain analisis apakah perlu switch profil.
        """
        cvd_vel   = float(context.get("cvd_velocity", 0) or 0)
        dom_ratio = float(context.get("dominance_ratio", 1) or 1)
        session   = context.get("session", "UNKNOWN")
        window_id = context.get("window_id", "")

        regime = _get_regime(cvd_vel)

        event = WouldWinEvent(
            signal_id       = signal_id,
            direction       = direction,
            blocked_by      = blocked_by,
            regime          = regime,
            dominance_ratio = dom_ratio,
            session         = session,
            window_id       = window_id,
        )
        self._would_win_buffer.append(event)

        logger.info(
            f"[BRAIN] +WouldWin: {direction} | Filter: {blocked_by} | "
            f"Regime: {regime} | DOM: {dom_ratio:.1f}x | "
            f"Buffer: {len(self._would_win_buffer)}"
        )

        self._evaluate_pattern()

    def notify_bet_resolved(self, is_win: bool):
        """
        Dipanggil setiap bet resolved.
        Digunakan untuk evaluasi efektivitas profil aktif.
        """
        self._bets_since_switch += 1
        if is_win:
            self._wins_since_switch += 1
        else:
            self._losses_since_switch += 1

        bets = self._bets_since_switch
        wins = self._wins_since_switch
        wr   = (wins / bets * 100) if bets > 0 else 0.0

        logger.debug(
            f"[BRAIN] Bet resolved ({'WIN' if is_win else 'LOSS'}) | "
            f"Profil: {self._active_profile} | "
            f"Progress eval: {bets}/{self.EVAL_AFTER_BETS} | WR: {wr:.0f}%"
        )

        # ── Early revert: jika WR < 30% setelah minimal 2 bet ──
        if bets >= 2 and wr < 30.0 and self._active_profile != "STANDARD":
            logger.warning(
                f"[BRAIN] ⚡ EARLY REVERT triggered | "
                f"WR {wr:.0f}% setelah {bets} bet (threshold <30%) | "
                f"Revert {self._active_profile} → STANDARD"
            )
            self._early_revert_triggered = True
            self._revert_to_previous()
            return

        if bets >= self.EVAL_AFTER_BETS:
            self._evaluate_profile_effectiveness()

    # ─────────────────────────────────────────────
    # Pattern Analysis
    # ─────────────────────────────────────────────
    def _evaluate_pattern(self):
        """
        Analisis buffer would-win dalam 30 menit terakhir.
        Jika ada pola (≥3 dari filter+regime sama) → switch profil.
        """
        if self._is_on_cooldown():
            logger.debug(
                f"[BRAIN] Cooldown aktif, sisa "
                f"{self.cooldown_remaining}s — skip pattern eval"
            )
            return

        now    = time.time()
        cutoff = now - self.RECENT_WINDOW_SEC

        # Hanya event dalam 30 menit terakhir
        recent = [e for e in self._would_win_buffer if e.timestamp >= cutoff]

        if len(recent) < self.WOULD_WIN_TRIGGER:
            return

        # Hitung frekuensi tiap kombinasi (blocked_by, regime)
        pattern_counts: dict = {}
        for e in recent:
            key = (e.blocked_by, e.regime)
            pattern_counts.setdefault(key, []).append(e)

        # Cari pola yang memenuhi threshold
        for (blocked_by, regime), events in sorted(
            pattern_counts.items(), key=lambda x: -len(x[1])
        ):
            if len(events) >= self.WOULD_WIN_TRIGGER:
                avg_dom        = sum(e.dominance_ratio for e in events) / len(events)
                target_profile = _select_profile(blocked_by, regime, avg_dom)

                if target_profile == self._active_profile:
                    logger.debug(
                        f"[BRAIN] Pola ({blocked_by}/{regime}) terdeteksi "
                        f"tapi sudah di profil {target_profile} — skip"
                    )
                    return

                reason = (
                    f"{len(events)} would-win diblokir {blocked_by} "
                    f"di regime {regime} (avg dom {avg_dom:.1f}x)"
                )
                self._switch_profile(
                    target_profile, reason, blocked_by, regime, len(events)
                )
                return  # satu switch per evaluasi

    def _switch_profile(
        self,
        new_profile : str,
        reason      : str,
        blocked_by  : str,
        regime      : str,
        count       : int,
    ):
        """Ganti profil aktif dan catat keputusan."""
        from fetcher.candle_tracker import CandleTracker

        old_profile = self._active_profile
        window_id   = CandleTracker.get_window_id()

        decision = BrainDecision(
            from_profile       = old_profile,
            to_profile         = new_profile,
            reason             = reason,
            pattern_blocked_by = blocked_by,
            pattern_regime     = regime,
            window_id          = window_id,
        )

        # Update state
        self._previous_profile     = old_profile   # simpan untuk kemungkinan revert
        self._active_profile       = new_profile
        self._profile_since        = time.time()
        self._profile_window       = window_id
        self._cooldown_until       = time.time() + (self.COOLDOWN_WINDOWS * self.WINDOW_DURATION)
        self._bets_since_switch    = 0
        self._wins_since_switch    = 0
        self._losses_since_switch  = 0
        self._early_revert_triggered = False
        self._current_decision     = decision

        # Simpan ke history & kosongkan buffer agar pola lama tidak re-trigger
        self._history.appendleft(decision)
        self._would_win_buffer.clear()

        logger.warning(
            f"[BRAIN] 🔄 SWITCH PROFIL: {old_profile} → {new_profile} | "
            f"Alasan: {reason} | Cooldown: {self.COOLDOWN_WINDOWS} window"
        )

        # Simpan ke brain_report.csv
        with open(self._report_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                old_profile, new_profile, reason,
                blocked_by, regime, count, window_id,
            ])

    # ─────────────────────────────────────────────
    # Profile Revert Helper
    # ─────────────────────────────────────────────
    def _revert_to_previous(self):
        """
        Revert profil aktif ke profil sebelumnya (atau STANDARD jika tidak ada).
        Digunakan saat evaluasi menunjukkan profil tidak efektif.
        """
        from fetcher.candle_tracker import CandleTracker

        bets   = self._bets_since_switch
        wins   = self._wins_since_switch
        losses = self._losses_since_switch
        wr     = (wins / bets * 100) if bets > 0 else 0.0

        old_profile    = self._active_profile
        revert_target  = self._previous_profile if self._previous_profile != old_profile else "STANDARD"
        window_id      = CandleTracker.get_window_id()

        logger.warning(
            f"[BRAIN] ↩ REVERT PROFIL: {old_profile} → {revert_target} | "
            f"WR {wr:.0f}% ({wins}W/{losses}L after {bets} bet) | "
            f"{'Early revert (<30% after 2 bet)' if self._early_revert_triggered else 'Normal eval revert (<45%)'}"
        )

        verdict = "INEFFECTIVE"
        action  = "REVERT"
        result  = "LOSS"

        # Update result di decision terkini
        if self._current_decision:
            self._current_decision.result     = result
            self._current_decision.bets_after = bets

        # Simpan ke brain_profile_results.csv
        with open(self._results_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                old_profile, bets, wins, losses,
                round(wr, 1), verdict, action,
            ])

        # Perform revert
        self._previous_profile    = old_profile
        self._active_profile      = revert_target
        self._profile_since       = time.time()
        self._profile_window      = window_id
        self._cooldown_until      = time.time() + (self.COOLDOWN_WINDOWS * self.WINDOW_DURATION)
        self._bets_since_switch   = 0
        self._wins_since_switch   = 0
        self._losses_since_switch = 0
        self._early_revert_triggered = False
        self._would_win_buffer.clear()

        # Catat ke brain_report.csv sebagai keputusan revert
        with open(self._report_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                old_profile, revert_target,
                f"REVERT: WR {wr:.0f}% ({wins}W/{losses}L/{bets} bet)",
                "EVAL_REVERT", "─", bets, window_id,
            ])

    # ─────────────────────────────────────────────
    # Profile Effectiveness Evaluation
    # ─────────────────────────────────────────────
    def _evaluate_profile_effectiveness(self):
        """
        Evaluasi apakah profil aktif efektif setelah EVAL_AFTER_BETS bet.
        Simpan laporan ke brain_profile_results.csv.

        Threshold baru:
          WR >= 55% → KEEP, extend eval 4 bet lagi
          WR 45-54% → KEEP tapi neutral (tidak extend)
          WR < 45%  → REVERT ke profil sebelumnya (atau STANDARD)
        """
        bets   = self._bets_since_switch
        wins   = self._wins_since_switch
        losses = self._losses_since_switch
        wr     = (wins / bets * 100) if bets > 0 else 0.0

        if wr >= 55.0:
            verdict = "EFFECTIVE"
            action  = "KEEP"
            result  = "WIN"
            logger.info(
                f"[BRAIN] ✅ Profil {self._active_profile} EFEKTIF | "
                f"WR {wr:.0f}% ({wins}W/{losses}L) — extend eval {self.EVAL_AFTER_BETS} bet lagi"
            )
            # Update result di decision terkini
            if self._current_decision:
                self._current_decision.result     = result
                self._current_decision.bets_after = bets

            # Simpan ke brain_profile_results.csv
            with open(self._results_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    self._active_profile, bets, wins, losses,
                    round(wr, 1), verdict, action,
                ])

            # Reset counter untuk siklus evaluasi berikutnya (extend 4 bet lagi)
            self._bets_since_switch   = 0
            self._wins_since_switch   = 0
            self._losses_since_switch = 0

        elif wr >= 45.0:
            verdict = "NEUTRAL"
            action  = "KEEP"
            result  = "─"
            logger.info(
                f"[BRAIN] ➡ Profil {self._active_profile} NEUTRAL | "
                f"WR {wr:.0f}% — pantau terus (tidak extend)"
            )
            # Update result di decision terkini
            if self._current_decision:
                self._current_decision.result     = result
                self._current_decision.bets_after = bets

            # Simpan ke brain_profile_results.csv
            with open(self._results_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    self._active_profile, bets, wins, losses,
                    round(wr, 1), verdict, action,
                ])

            # Reset counter — tidak extend, evaluasi siklus baru dari awal
            self._bets_since_switch   = 0
            self._wins_since_switch   = 0
            self._losses_since_switch = 0

        else:
            # WR < 45% → REVERT
            self._revert_to_previous()

    # ─────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────
    def _is_on_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    @property
    def cooldown_remaining(self) -> int:
        return max(0, int(self._cooldown_until - time.time()))

    def _dominant_pattern_count(self) -> tuple:
        """
        Hitung pola (blocked_by, regime) yang paling sering muncul
        dalam 30 menit terakhir.

        Returns: (count, blocked_by, regime)
        """
        now    = time.time()
        cutoff = now - self.RECENT_WINDOW_SEC
        recent = [e for e in self._would_win_buffer if e.timestamp >= cutoff]

        if not recent:
            return (0, "-", "-")

        pattern_counts: dict = {}
        for e in recent:
            key = (e.blocked_by, e.regime)
            pattern_counts[key] = pattern_counts.get(key, 0) + 1

        # Pola yang paling dominan
        best_key = max(pattern_counts, key=lambda k: pattern_counts[k])
        return (pattern_counts[best_key], best_key[0], best_key[1])

    @property
    def stats(self) -> dict:
        """Semua data yang dibutuhkan dashboard."""
        bets   = self._bets_since_switch
        wins   = self._wins_since_switch
        losses = self._losses_since_switch
        wr     = (wins / bets * 100) if bets > 0 else 0.0

        dom_count, dom_filter, dom_regime = self._dominant_pattern_count()

        return {
            "active_profile"         : self._active_profile,
            "previous_profile"       : self._previous_profile,
            "description"            : self.profile_description,
            "since"                  : self.profile_since_str,
            "cooldown_remaining"     : self.cooldown_remaining,
            "bets_evaluated"         : bets,
            "eval_target"            : self.EVAL_AFTER_BETS,
            "wins_since_switch"      : wins,
            "losses_since_switch"    : losses,
            "wr_since_switch"        : round(wr, 1),
            "early_revert_triggered" : self._early_revert_triggered,
            # Pola terkuat (bukan total buffer)
            "dominant_pattern_count" : dom_count,
            "dominant_filter"        : dom_filter,
            "dominant_regime"        : dom_regime,
            "trigger_threshold"      : self.WOULD_WIN_TRIGGER,
            "history"                : list(self._history),
            "params"                 : {
                k: v for k, v in STRATEGY_PROFILES[self._active_profile].items()
                if k != "description"
            },
        }

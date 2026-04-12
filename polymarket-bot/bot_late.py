"""
bot_late.py
===========
Late Entry Speed Bot — completely different strategy from main.py.

Concept:
  Enter ONLY in the last 90 seconds of a 5-minute Polymarket window (t=210-300s).
  No 12s confirmation delay — all filter checks are INSTANT (<1s).
  Goal: "steal the start" by entering before Polymarket participants react to the
  liquidation signal.

Filter pipeline (ALL INSTANT — no delays):
  F1: Time check        — elapsed >= 210s AND remaining >= 20s
  F2: Beat distance     — |price - beat| >= $40 in signal direction
  F3: Dual liq check    — liq_recent(3s) >= $15k AND liq_sustained(30s) >= $50k
  F4: CVD alignment     — cvd_2min < -25k for DOWN, cvd_2min > +25k for UP
  F5: Odds display      — info only, NO FILTER (odds ditampilkan untuk monitoring)

Run:  python bot_late.py
Stop: Ctrl+C
"""

import asyncio
import logging
import os
import sys
import time

if sys.platform == "win32":
    import msvcrt
    import winsound
else:
    msvcrt   = None
    winsound = None
from collections import deque
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()


# ─────────────────────────────────────────────
# Sound notifications (Windows winsound)
# ─────────────────────────────────────────────
def _beep(freq: int, duration: int):
    try:
        if winsound:
            winsound.Beep(freq, duration)
    except Exception:
        pass


async def play_sound(event: str):
    loop = asyncio.get_event_loop()
    if event == "signal":
        await loop.run_in_executor(None, _beep, 600, 120)
        await loop.run_in_executor(None, _beep, 900, 180)
    elif event == "win":
        await loop.run_in_executor(None, _beep, 600, 100)
        await loop.run_in_executor(None, _beep, 800, 100)
        await loop.run_in_executor(None, _beep, 1050, 250)
    elif event == "loss":
        await loop.run_in_executor(None, _beep, 500, 200)
        await loop.run_in_executor(None, _beep, 300, 350)
    elif event == "blocked":
        await loop.run_in_executor(None, _beep, 440, 100)
    elif event == "warning":
        await loop.run_in_executor(None, _beep, 880, 200)
        await loop.run_in_executor(None, _beep, 440, 200)
        await loop.run_in_executor(None, _beep, 880, 200)
        await loop.run_in_executor(None, _beep, 440, 400)


# ── Config ──
DRY_RUN               = os.getenv("DRY_RUN", "true").lower() == "true"
BET_AMOUNT            = float(os.getenv("LATE_BET_AMOUNT", "2.0"))   # flat $2
MIN_ODDS              = float(os.getenv("MIN_ODDS", "0.45"))
MAX_ODDS              = float(os.getenv("LATE_MAX_ODDS", "0.95"))
LIQ_THRESHOLD         = float(os.getenv("LIQUIDATION_THRESHOLD", "200000"))
CLAIM_CASH_THRESHOLD  = float(os.getenv("CLAIM_CASH_THRESHOLD", "4.0"))   # auto-claim saat cash < $X
CLAIM_CHECK_INTERVAL  = int(os.getenv("CLAIM_CHECK_INTERVAL", "90"))       # cek setiap N detik
ENTRY_WINDOW_START    = 210      # only enter at t>=210s (last 90s)
MIN_WINDOW_REMAINING  = 10       # skip if <10s left
MIN_BEAT_DISTANCE     = 40.0     # price must be $40+ from beat in signal direction
LIQ_RECENT_WINDOW     = 3        # seconds for recent liq check (sama dengan backtest)
LIQ_RECENT_MIN_USD    = 15_000   # $15k in 3s = active cascade
LIQ_SUSTAINED_WINDOW  = 30       # seconds for sustained liq check
LIQ_SUSTAINED_MIN_USD = 50_000   # $50k in 30s = sustained pressure
CVD_MIN_THRESHOLD     = 25_000   # |CVD_2min| >= $25k in signal direction
ODDS_POLL_INTERVAL    = 3        # aggressive polling interval (seconds) in last 100s
MAX_BETS_PER_HOUR     = 8
DAILY_STOP_LOSS_PCT   = 0.20     # 20% daily stop loss

# ── Session transition blackout (UTC) ──
# Format: (start_hour, start_min, end_hour, end_min)
SESSION_BLOCK_ENABLED = os.getenv("SESSION_BLOCK", "true").lower() == "true"
SESSION_BLOCKS = [
    ( 7, 55,  9,  5),   # Asian → London open
    (12, 55, 14,  5),   # London → NY open  ← paling berbahaya
    (20, 55, 22,  5),   # NY close
]

# ── Logging ──
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/late_live.log", encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logging.getLogger("engine.signal").setLevel(logging.ERROR)
logging.getLogger("engine.result_tracker").setLevel(logging.INFO)
logging.getLogger("executor.polymarket").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

from fetcher.hyperliquid_ws import MarketData, HyperliquidWSFetcher, get_liquidations_in_window, get_cvd_change
from fetcher.hyperliquid_rest import HyperliquidRESTFetcher
from fetcher.candle_tracker import CandleTracker
from engine.signal import SignalEngine, Signal, SignalResult
from engine.result_tracker import ResultTracker
from engine.blocked_tracker import BlockedTracker
from executor.polymarket import PolymarketExecutor
from utils.colors import C, green, red, yellow, cyan, magenta, white, dim, bold, signed_color, ok_or_err


# ─────────────────────────────────────────────
# Session Transition Guard
# ─────────────────────────────────────────────
def _in_session_block() -> tuple:
    """
    Returns (is_blocked: bool, label: str) based on current UTC time.
    Checks against SESSION_BLOCKS windows.
    """
    if not SESSION_BLOCK_ENABLED:
        return False, ""
    now   = datetime.now(timezone.utc)
    total = now.hour * 60 + now.minute
    for (h0, m0, h1, m1) in SESSION_BLOCKS:
        start = h0 * 60 + m0
        end   = h1 * 60 + m1
        if start <= total < end:
            return True, f"{h0:02d}:{m0:02d}–{h1:02d}:{m1:02d} UTC"
    return False, ""


# ─────────────────────────────────────────────
# Late Entry Handler
# ─────────────────────────────────────────────
class LateHandler:
    """
    Handles UP/DOWN signals with INSTANT filter pipeline (no confirmation delay).
    Enters only in the last 90 seconds of a window (t=210-300s).

    Filter Pipeline (all checks < 1s):
      F1: Entry zone check   — elapsed >= 210s AND remaining >= 20s
      F2: Beat distance      — |price - beat| >= $40 in signal direction
      F3: Dual liq check     — recent(3s) >= $15k AND sustained(30s) >= $50k
      F4: CVD alignment      — |cvd_2min| >= $25k in signal direction
      F5: Odds display       — info only, no filter
    """

    def __init__(
        self,
        tracker:    ResultTracker,
        candle:     CandleTracker,
        data:       MarketData,
        blocked:    BlockedTracker,
        executor:   PolymarketExecutor,
        market_cache: dict,
    ):
        self.tracker      = tracker
        self.candle       = candle
        self.data         = data
        self.blocked      = blocked
        self.executor     = executor
        self.market_cache = market_cache

        self._last_window_id: str = ""

        # Filter status strings for dashboard
        self.f1_status:          str = dim("─ waiting")
        self.f2_status:          str = dim("─ waiting")
        self.f3_status:          str = dim("─ waiting")
        self.f4_status:          str = dim("─ waiting")
        self.f5_status:          str = dim("─ waiting")
        self.last_skip_reason:   str = ""

        # Rate limiting
        self._bet_timestamps: deque = deque(maxlen=MAX_BETS_PER_HOUR)
        self._daily_start_balance: float = 0.0
        self._daily_pnl: float = 0.0

        # Manual mode
        self.auto_enabled: bool = True   # False = auto-signals disabled, manual key only
        self._manual_msg:  str  = ""     # last manual action message for dashboard

    def _bets_this_hour(self) -> int:
        cutoff = time.time() - 3600
        return sum(1 for t in self._bet_timestamps if t > cutoff)

    async def on_signal(self, result: SignalResult):
        """
        Called by SignalEngine every time a UP/DOWN signal fires.
        All filter checks are INSTANT — no asyncio.sleep delays.
        """
        if not self.auto_enabled:
            return  # manual-only mode — ignore auto signals

        blocked, blk_label = _in_session_block()
        if blocked:
            self.last_skip_reason = f"SESSION BLOCK: {blk_label}"
            return

        direction = result.signal.value
        window_id = CandleTracker.get_window_id()

        # 1-per-window guard
        if window_id == self._last_window_id:
            return

        elapsed   = CandleTracker.get_time_elapsed()
        remaining = CandleTracker.get_time_remaining()
        beat_p    = self.candle.beat_price if self.candle._initialized else 0.0
        cur_price = self.data.btc_price

        # ── F1: Entry zone check ──
        if elapsed < ENTRY_WINDOW_START:
            self.f1_status = yellow(f"t={elapsed}s (waiting for t={ENTRY_WINDOW_START}s)")
            self.last_skip_reason = f"Too early: t={elapsed}s (min {ENTRY_WINDOW_START}s)"
            return
        if remaining < MIN_WINDOW_REMAINING:
            self.f1_status = red(f"t={elapsed}s, remaining={remaining}s (min {MIN_WINDOW_REMAINING}s)")
            self.last_skip_reason = f"Too late: only {remaining}s remaining (min {MIN_WINDOW_REMAINING}s)"
            return
        self.f1_status = green(f"t={elapsed}s ENTRY ZONE (remaining={remaining}s)")

        if not self.candle._initialized or beat_p == 0.0:
            self.last_skip_reason = "Candle not initialized"
            return

        # ── F2: Beat distance check ──
        vs_beat = cur_price - beat_p  # positive = above beat
        if direction == "UP":
            # For UP signal: price must be $40+ ABOVE beat
            if vs_beat < MIN_BEAT_DISTANCE:
                self.f2_status = red(f"${vs_beat:+.1f} vs beat (need +${MIN_BEAT_DISTANCE:.0f} for UP)")
                reason = f"Beat distance insufficient for UP: ${vs_beat:+.1f} (need +${MIN_BEAT_DISTANCE:.0f})"
                self.last_skip_reason = reason
                self.blocked.add(
                    direction=direction, beat_price=beat_p,
                    btc_at_block=cur_price, window_id=window_id,
                    window_end_ts=CandleTracker.get_window_end().timestamp(),
                    blocked_by="F2", blocked_reason=reason,
                )
                return
            self.f2_status = green(f"+${vs_beat:.1f} vs beat (UP, min +${MIN_BEAT_DISTANCE:.0f})")
        else:
            # For DOWN signal: price must be $40+ BELOW beat
            if vs_beat > -MIN_BEAT_DISTANCE:
                self.f2_status = red(f"${vs_beat:+.1f} vs beat (need -${MIN_BEAT_DISTANCE:.0f} for DOWN)")
                reason = f"Beat distance insufficient for DOWN: ${vs_beat:+.1f} (need -${MIN_BEAT_DISTANCE:.0f})"
                self.last_skip_reason = reason
                self.blocked.add(
                    direction=direction, beat_price=beat_p,
                    btc_at_block=cur_price, window_id=window_id,
                    window_end_ts=CandleTracker.get_window_end().timestamp(),
                    blocked_by="F2", blocked_reason=reason,
                )
                return
            self.f2_status = green(f"${vs_beat:.1f} vs beat (DOWN, min -${MIN_BEAT_DISTANCE:.0f})")

        # ── F3: Dual liquidation check ──
        # For DOWN signal: liq on LONG side (longs being liquidated = bearish pressure)
        # For UP signal:   liq on SHORT side (shorts being liquidated = bullish pressure)
        liq_side = "LONG" if direction == "DOWN" else "SHORT"
        liq_recent,    _ = get_liquidations_in_window(self.data, liq_side, LIQ_RECENT_WINDOW)
        liq_sustained, _ = get_liquidations_in_window(self.data, liq_side, LIQ_SUSTAINED_WINDOW)

        recent_ok    = liq_recent    >= LIQ_RECENT_MIN_USD
        sustained_ok = liq_sustained >= LIQ_SUSTAINED_MIN_USD

        if not recent_ok or not sustained_ok:
            recent_c    = green if recent_ok    else red
            sustained_c = green if sustained_ok else red
            self.f3_status = (
                recent_c(f"recent({LIQ_RECENT_WINDOW}s): ${liq_recent:,.0f}") + dim(" | ") +
                sustained_c(f"sustained({LIQ_SUSTAINED_WINDOW}s): ${liq_sustained:,.0f}")
            )
            reason = (
                f"Liq insufficient [{liq_side}]: "
                f"recent=${liq_recent:,.0f} (min ${LIQ_RECENT_MIN_USD:,.0f}), "
                f"sustained=${liq_sustained:,.0f} (min ${LIQ_SUSTAINED_MIN_USD:,.0f})"
            )
            self.last_skip_reason = reason
            self.blocked.add(
                direction=direction, beat_price=beat_p,
                btc_at_block=cur_price, window_id=window_id,
                window_end_ts=CandleTracker.get_window_end().timestamp(),
                blocked_by="F3", blocked_reason=reason,
            )
            return
        self.f3_status = green(
            f"recent({LIQ_RECENT_WINDOW}s): ${liq_recent:,.0f} | sustained({LIQ_SUSTAINED_WINDOW}s): ${liq_sustained:,.0f}"
        )

        # ── F4: CVD alignment ──
        cvd_2min = get_cvd_change(self.data, 120)
        if direction == "DOWN" and cvd_2min > -CVD_MIN_THRESHOLD:
            self.f4_status = red(f"CVD={cvd_2min:+,.0f} (need < -${CVD_MIN_THRESHOLD:,.0f} for DOWN)")
            reason = f"CVD not aligned for DOWN: {cvd_2min:+,.0f} (need < -{CVD_MIN_THRESHOLD:,.0f})"
            self.last_skip_reason = reason
            self.blocked.add(
                direction=direction, beat_price=beat_p,
                btc_at_block=cur_price, window_id=window_id,
                window_end_ts=CandleTracker.get_window_end().timestamp(),
                blocked_by="F4", blocked_reason=reason,
            )
            return
        if direction == "UP" and cvd_2min < CVD_MIN_THRESHOLD:
            self.f4_status = red(f"CVD={cvd_2min:+,.0f} (need > +${CVD_MIN_THRESHOLD:,.0f} for UP)")
            reason = f"CVD not aligned for UP: {cvd_2min:+,.0f} (need > +{CVD_MIN_THRESHOLD:,.0f})"
            self.last_skip_reason = reason
            self.blocked.add(
                direction=direction, beat_price=beat_p,
                btc_at_block=cur_price, window_id=window_id,
                window_end_ts=CandleTracker.get_window_end().timestamp(),
                blocked_by="F4", blocked_reason=reason,
            )
            return
        self.f4_status = green(f"CVD={cvd_2min:+,.0f} aligned for {direction}")

        # ── F5: Odds — INFO ONLY, tidak memblok entry ──
        # Filter odds dihapus agar bot tidak miss signal valid.
        # Odds tetap ditampilkan di dashboard untuk monitoring.
        market    = self.market_cache.get("market")
        yes_px    = self.market_cache.get("yes_px", 0.0)
        no_px     = self.market_cache.get("no_px",  0.0)
        cache_age = time.time() - self.market_cache.get("odds_refreshed_at", 0)
        odds_valid = self.market_cache.get("odds_valid", True)

        if market is None:
            self.f5_status = yellow("─ no market cache (bet tetap lanjut)")
            yes_px, no_px = 0.5, 0.5
        elif not odds_valid or not (0.05 <= yes_px <= 0.95):
            self.f5_status = yellow(f"⚠ odds stale/invalid (cache {cache_age:.0f}s) — lanjut")
            yes_px = self.market_cache.get("yes_px", 0.5)
            no_px  = self.market_cache.get("no_px",  0.5)
        else:
            odds = yes_px if direction == "UP" else no_px
            self.f5_status = cyan(f"UP={yes_px:.2f} DOWN={no_px:.2f} [{direction}={odds:.2f}] (cache {cache_age:.0f}s) — NO FILTER")

        # ── Rate limiting ──
        if self._bets_this_hour() >= MAX_BETS_PER_HOUR:
            self.last_skip_reason = f"Rate limit: {MAX_BETS_PER_HOUR} bets/hour reached"
            logger.warning(f"[LATE] Rate limit reached: {MAX_BETS_PER_HOUR} bets/hour")
            return

        # ── All filters passed — mark window BEFORE HTTP calls to prevent double-bet ──
        self._last_window_id = window_id
        window_end_ts = CandleTracker.get_window_end().timestamp()

        logger.info(
            f"[LATE] All filters passed {direction} | "
            f"t={elapsed}s rem={remaining}s | "
            f"beat=${beat_p:,.2f} price=${cur_price:,.2f} dist=${vs_beat:+.1f} | "
            f"CVD={cvd_2min:+,.0f} | odds={odds:.3f} | Window: {window_id}"
        )

        asyncio.create_task(
            self._execute_bet(result, window_id, direction, beat_p, cur_price)
        )

    async def manual_bet(self, direction: str):
        """
        Manual bet via keyboard — bypasses F1-F4 filters.
        Still respects rate limiting and 1-per-window guard.
        """
        window_id = CandleTracker.get_window_id()
        remaining = CandleTracker.get_time_remaining()

        blocked, blk_label = _in_session_block()
        if blocked:
            self._manual_msg = red(f"✗ MANUAL {direction} — SESSION BLOCK {blk_label}")
            logger.warning(f"[MANUAL] Blocked by session transition: {blk_label}")
            return

        if remaining < 5:
            self._manual_msg = red(f"✗ MANUAL {direction} — window closing ({remaining}s left)")
            logger.warning(f"[MANUAL] Bet aborted — window closing ({remaining}s left)")
            return

        if self._bets_this_hour() >= MAX_BETS_PER_HOUR:
            self._manual_msg = red(f"✗ MANUAL {direction} — rate limit ({MAX_BETS_PER_HOUR}/hr)")
            logger.warning(f"[MANUAL] Rate limit: {MAX_BETS_PER_HOUR} bets/hour reached")
            return

        if window_id == self._last_window_id:
            self._manual_msg = yellow(f"⚠ MANUAL {direction} — already bet window {window_id}")
            logger.warning(f"[MANUAL] Already bet this window: {window_id}")
            return

        beat_p    = self.candle.beat_price if self.candle._initialized else 0.0
        cur_price = self.data.btc_price

        self._last_window_id = window_id
        self._manual_msg = cyan(f"▶ MANUAL {direction} @ ${cur_price:,.0f}  t={CandleTracker.get_time_elapsed()}s")
        logger.info(
            f"[MANUAL] Manual bet {direction} | "
            f"price=${cur_price:,.2f} beat=${beat_p:,.2f} window={window_id}"
        )
        asyncio.create_task(
            self._execute_bet(None, window_id, direction, beat_p, cur_price)
        )

    async def _execute_bet(
        self,
        result:        SignalResult,
        window_id:     str,
        direction:     str,
        beat_p:        float,
        current_price: float,
    ):
        """Place the bet (or simulate) and register with ResultTracker."""
        try:
            market       = self.market_cache.get("market")
            window_end   = CandleTracker.get_window_end().timestamp()

            yes_px = self.market_cache.get("yes_px", 0.5)
            no_px  = self.market_cache.get("no_px",  0.5)
            odds   = yes_px if direction == "UP" else no_px

            if not DRY_RUN:
                # executor.place_bet calls find_active_btc_5min internally,
                # so market cache being None at signal time is not a blocker
                bet_result = self.executor.place_bet(
                    direction     = direction,
                    amount_usdc   = BET_AMOUNT,
                    signal_reason = (
                        f"LATE F1-F4 passed | {direction} @ ${current_price:,.0f} "
                        f"t={CandleTracker.get_time_elapsed()}s"
                    ),
                )
                if bet_result is None:
                    self.last_skip_reason = "place_bet returned None (market not found / min_odds / dedup)"
                    logger.error(f"[LATE] place_bet returned None for {direction}")
                    return
                if bet_result.status == "FAILED":
                    err_detail = bet_result.error or "unknown"
                    self.last_skip_reason = f"Bet FAILED: {err_detail[:60]}"
                    logger.error(
                        f"[LATE] Bet FAILED {direction} | "
                        f"order={bet_result.order_id} | error={err_detail}"
                    )
                    return
                if bet_result.status not in ("FILLED", "MATCHED", "MACHED", "DRY_RUN"):
                    self.last_skip_reason = f"Bet status unexpected: {bet_result.status}"
                    logger.warning(
                        f"[LATE] Bet status tidak dikenal: {bet_result.status} | "
                        f"order={bet_result.order_id}"
                    )
                    return
                logger.info(
                    f"[LATE] BET PLACED {direction} | ${BET_AMOUNT:.2f} @ {odds:.3f} | "
                    f"Order: {bet_result.order_id} | Window: {window_id}"
                )
            else:
                logger.info(
                    f"[LATE] [DRY] BET SIMULATED {direction} | ${BET_AMOUNT:.2f} @ {odds:.3f} | "
                    f"Window: {window_id}"
                )

            self._bet_timestamps.append(time.time())

            self.tracker.add(
                direction     = direction,
                btc_entry     = current_price,
                beat_price    = beat_p,
                window_id     = window_id,
                window_end_ts = window_end,
                amount_usdc   = BET_AMOUNT,
                odds          = odds,
                on_resolved   = self._on_resolved,
            )
            asyncio.create_task(play_sound("signal"))

        except Exception as e:
            logger.error(f"[LATE] Error in _execute_bet: {e}")

    async def _on_resolved(
        self,
        direction: str,
        is_win:    bool,
        btc_entry: float,
        btc_exit:  float,
    ):
        """Called by ResultTracker when window closes and result is known."""
        result_str = green("WIN") if is_win else red("LOSS")
        logger.info(
            f"[LATE] Resolved {direction} {result_str} | "
            f"Entry ${btc_entry:,.0f} → Exit ${btc_exit:,.0f}"
        )
        asyncio.create_task(play_sound("win" if is_win else "loss"))


# ─────────────────────────────────────────────
# Aggressive market refresh loop
# ─────────────────────────────────────────────
async def _market_refresh_loop(executor: PolymarketExecutor, market_cache: dict):
    """
    Background loop: polls Polymarket for active market and fresh odds.
    - If time_remaining < 100s: poll every 3s (aggressive)
    - Otherwise: poll every 10s
    Stores yes_px, no_px in market_cache for instant access by LateHandler.
    """
    loop = asyncio.get_event_loop()
    while True:
        try:
            remaining = CandleTracker.get_time_remaining()
            poll_interval = ODDS_POLL_INTERVAL if remaining < 100 else 10

            mkt = await loop.run_in_executor(
                None, executor.finder.find_active_btc_5min
            )
            market_cache["market"]       = mkt
            market_cache["refreshed_at"] = time.time()

            if mkt:
                market_cache["question"] = mkt.question
                # Refresh odds prices — validasi harga masuk akal (0.05-0.95)
                try:
                    yes_px, no_px = executor.finder.get_current_prices(mkt)
                    if 0.05 <= yes_px <= 0.95:
                        # Harga valid
                        market_cache["yes_px"]            = yes_px
                        market_cache["no_px"]             = no_px
                        market_cache["odds_refreshed_at"] = time.time()
                        market_cache["odds_valid"]        = True
                    else:
                        # Harga tidak masuk akal (0.00/1.00 = API error)
                        market_cache["odds_valid"] = False
                        logger.debug(f"[LATE CACHE] Odds invalid: YES={yes_px:.3f} NO={no_px:.3f} — skip update")
                except Exception as e:
                    market_cache["odds_valid"] = False
                    logger.debug(f"[LATE CACHE] Odds fetch error: {e}")

                # Next market info
                market_cache["next_question"] = ""
                market_cache["next_secs"]     = 0.0
            else:
                market_cache["question"] = "─ no active market"
                market_cache["yes_px"]   = 0.0
                market_cache["no_px"]    = 0.0
                try:
                    next_mkt, next_secs = await loop.run_in_executor(
                        None, executor.finder.find_next_btc_5min
                    )
                    market_cache["next_question"] = next_mkt.question if next_mkt else ""
                    market_cache["next_secs"]     = next_secs
                except Exception:
                    pass

        except Exception as e:
            logger.debug(f"[LATE CACHE] Refresh error: {e}")

        await asyncio.sleep(poll_interval)


# ─────────────────────────────────────────────
# Auto-Claim loop
# ─────────────────────────────────────────────
async def _auto_claim_loop(
    executor:    "PolymarketExecutor",
    claim_state: dict,
    tracker:     "ResultTracker",
):
    """
    Background loop: auto-claim posisi yang menang agar saldo kembali ke USDC.
    Trigger:
      - Setiap CLAIM_CHECK_INTERVAL detik (default 90s)
      - Langsung saat cash < CLAIM_CASH_THRESHOLD (cek setiap 30s)
    Updates claim_state dict untuk ditampilkan di dashboard.
    """
    if DRY_RUN:
        claim_state["status"] = dim("─ DRY RUN (claim dinonaktifkan)")
        return

    loop       = asyncio.get_event_loop()
    last_check = 0.0          # 0 → langsung trigger saat iterasi pertama

    while True:
        balance    = claim_state.get("last_balance", 999.0)
        time_since = time.time() - last_check
        should_check = (
            last_check == 0.0                                           # startup — langsung cek
            or time_since >= CLAIM_CHECK_INTERVAL                       # jadwal rutin
            or (balance < CLAIM_CASH_THRESHOLD and time_since >= 30)   # darurat cash rendah
        )

        if not should_check:
            await asyncio.sleep(10)
            continue

        claim_state["status"] = dim("... mengecek posisi...")
        last_check = time.time()
        ts_str = datetime.now().strftime("%H:%M:%S")

        try:
            # Kumpulkan window_id posisi yang MASIH AKTIF di bot (belum resolved)
            now = time.time()
            active_window_ids = {
                p.window_id
                for p in tracker._pending
                if p.window_end_ts > now   # window belum berakhir
            }
            if active_window_ids:
                logger.info(f"[CLAIM] Active windows (di-skip dari claim): {active_window_ids}")

            claimed, total = await loop.run_in_executor(
                None, lambda: executor.claim_all_winnings(
                    skip_window_ids=active_window_ids
                )
            )

            if total == 0:
                claim_state["status"] = dim(f"─ nothing to claim ({ts_str})")
            elif claimed > 0:
                claim_state["status"] = green(f"OK claimed {claimed}/{total} positions ({ts_str})")
                asyncio.create_task(play_sound("win"))
            else:
                claim_state["status"] = yellow(f"⚠ {total} redeemable, 0 claimed — cek log ({ts_str})")

        except Exception as e:
            err_str = str(e)
            # Pesan spesifik untuk error MATIC kurang
            if "MATIC" in err_str or "insufficient funds" in err_str.lower():
                claim_state["status"] = red(f"✗ No MATIC for gas! Kirim POL ke wallet Anda")
            else:
                claim_state["status"] = red(f"✗ error: {err_str[:45]} ({ts_str})")
            logger.error(f"[CLAIM] Auto-claim error: {e}")

        await asyncio.sleep(10)   # tunggu sebelum iterasi berikutnya


# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────
def print_late_dashboard(
    data:         MarketData,
    engine:       SignalEngine,
    tracker:      ResultTracker,
    uptime:       float,
    candle:       CandleTracker,
    handler:      LateHandler,
    blocked:      BlockedTracker,
    market_cache: dict,
    balance:      float = 0.0,
    claim_status: str   = "",
):
    os.system("cls" if os.name == "nt" else "clear")

    stats   = tracker.stats
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    up_m    = int(uptime // 60)
    up_s    = int(uptime % 60)
    dry_tag = magenta(" DRY RUN") if DRY_RUN else red(bold(" LIVE"))

    # ── Market data ──
    liq_short_3s,  _ = get_liquidations_in_window(data, "SHORT", 3)
    liq_long_3s,   _ = get_liquidations_in_window(data, "LONG",  3)
    liq_short_30s, _ = get_liquidations_in_window(data, "SHORT", 30)
    liq_long_30s,  _ = get_liquidations_in_window(data, "LONG",  30)
    cvd_2m            = get_cvd_change(data, 120)

    ws_str  = ok_or_err(data.ws_connected, "OK", "ERR")
    btc_str = cyan(f"${data.btc_price:>10,.2f}")
    cvd_str = signed_color(cvd_2m, f"${cvd_2m:>+13,.0f}")

    # ── Candle / Window info ──
    if candle and candle._initialized:
        cs        = candle.get_status()
        elapsed   = cs["time_elapsed"]   if "time_elapsed" in cs else CandleTracker.get_time_elapsed()
        remaining = cs["time_remaining"]
        prog      = cs["progress_pct"]
        wid       = cs["window_id"]
        beat_p    = cs["beat_price"]

        bar_filled = int(prog / 100 * 20)
        prog_bar   = cyan("#" * bar_filled) + dim("." * (20 - bar_filled))

        if beat_p > 0:
            beat_str = cyan(f"${beat_p:,.2f}")
            vs_b_usd = data.btc_price - beat_p
            if vs_b_usd > 0:
                beat_status = green(f"+${vs_b_usd:,.1f}")
            elif vs_b_usd < 0:
                beat_status = red(f"-${abs(vs_b_usd):,.1f}")
            else:
                beat_status = dim("FLAT")
        else:
            beat_str    = yellow("Waiting...")
            beat_status = dim("─")
            vs_b_usd    = 0.0

        # Elapsed: highlight if in entry zone
        if elapsed >= ENTRY_WINDOW_START:
            elapsed_str = green(bold(f"t={elapsed}s ENTRY ZONE"))
        elif elapsed >= ENTRY_WINDOW_START - 30:
            elapsed_str = yellow(f"t={elapsed}s (entry in {ENTRY_WINDOW_START-elapsed}s)")
        else:
            elapsed_str = dim(f"t={elapsed}s (entry at t={ENTRY_WINDOW_START}s)")

        if remaining <= MIN_WINDOW_REMAINING:
            rem_str = red(f"{remaining}s")
        elif remaining <= 30:
            rem_str = yellow(f"{remaining}s")
        else:
            rem_str = green(f"{remaining}s")
    else:
        wid         = "--:--"
        beat_p      = 0.0
        beat_str    = yellow("Waiting...")
        beat_status = dim("─")
        elapsed_str = dim("─")
        rem_str     = dim("─")
        prog_bar    = dim("." * 20)
        elapsed     = 0
        remaining   = 300

    # ── Market cache ──
    mc_age = time.time() - market_cache.get("refreshed_at", 0)
    mc_has = market_cache.get("market") is not None
    if mc_has:
        mc_age_str = green(f"{mc_age:.0f}s") if mc_age < 10 else yellow(f"{mc_age:.0f}s")
        mc_str = green(market_cache.get("question", "")[:38]) + dim(f"  [{mc_age_str}]")
    else:
        next_q    = market_cache.get("next_question", "")
        next_secs = market_cache.get("next_secs", 0.0)
        if next_q:
            cache_elapsed = mc_age
            real_remaining = max(0.0, next_secs - cache_elapsed)
            mins = int(real_remaining // 60)
            secs_r = int(real_remaining % 60)
            mc_str = dim(f"No active market — next in {mins}m{secs_r:02d}s: {next_q[-30:]}")
        else:
            mc_str = yellow("No active market")

    # ── Liq display (for dashboard) ──
    liq_s3_str  = (green if liq_short_3s  >= LIQ_RECENT_MIN_USD    else white)(f"${liq_short_3s:>8,.0f}")
    liq_l3_str  = (green if liq_long_3s   >= LIQ_RECENT_MIN_USD    else white)(f"${liq_long_3s:>8,.0f}")
    liq_s30_str = (green if liq_short_30s >= LIQ_SUSTAINED_MIN_USD else white)(f"${liq_short_30s:>8,.0f}")
    liq_l30_str = (green if liq_long_30s  >= LIQ_SUSTAINED_MIN_USD else white)(f"${liq_long_30s:>8,.0f}")

    # ── Filter statuses ──
    f1_str = getattr(handler, "f1_status", dim("─")) if handler else dim("─")
    f2_str = getattr(handler, "f2_status", dim("─")) if handler else dim("─")
    f3_str = getattr(handler, "f3_status", dim("─")) if handler else dim("─")
    f4_str = getattr(handler, "f4_status", dim("─")) if handler else dim("─")
    f5_str = getattr(handler, "f5_status", dim("─")) if handler else dim("─")
    skip_str = dim(getattr(handler, "last_skip_reason", "")[:50]) if handler else dim("─")

    # ── Last signal ──
    last = engine.last_signal
    if last:
        sig_val  = last.signal.value
        sig_time = datetime.fromtimestamp(last.timestamp).strftime("%H:%M:%S")
        if sig_val == "UP":
            sig_icon = green("UP  ")
        elif sig_val == "DOWN":
            sig_icon = red("DOWN")
        else:
            sig_icon = dim("SKIP")
    else:
        sig_icon = dim("waiting...")
        sig_time = "-"

    # ── Results ──
    wr      = stats["win_rate"]
    bar_len = 20
    filled  = int(wr / 100 * bar_len)
    if wr >= 60:
        bar = green("#" * filled) + dim("." * (bar_len - filled))
    elif wr >= 50:
        bar = yellow("#" * filled) + dim("." * (bar_len - filled))
    else:
        bar = red("#" * filled) + dim("." * (bar_len - filled))

    wr_str   = (green if wr >= 60 else yellow if wr >= 50 else red)(f"{wr:>5.1f}%")
    pnl_str  = signed_color(stats["total_pnl"], f"${stats['total_pnl']:>+9.2f}")
    win_str  = green(f"{stats['wins']:>4}")
    loss_str = red(f"{stats['losses']:>4}")

    # ── Blocked signals ──
    if blocked:
        bs = blocked.stats

    bets_hr = handler._bets_this_hour() if handler else 0

    print(f"+{'-'*62}+")
    print(f"|  {bold('LATE BOT')}{dry_tag}  │  {cyan(now_str)}  │  {dim(f'{up_m:02d}m {up_s:02d}s')}  |")
    print(f"+{'-'*62}+")
    print(f"|  {bold('POLYMARKET WINDOW')}                                         |")
    print(f"|  Window ID   : {cyan(wid)}  │  Remaining: {rem_str}                 |")
    print(f"|  Beat price  : {beat_str}  │  vs beat: {beat_status}                |")
    print(f"|  Progress    : [{prog_bar}]                  |")
    print(f"|  Elapsed     : {elapsed_str}                            |")
    print(f"|  Market      : {mc_str}  |")
    print(f"+{'-'*62}+")
    # ── Odds display ──
    _yes_px    = market_cache.get("yes_px", 0.0)
    _no_px     = market_cache.get("no_px",  0.0)
    _odds_age  = time.time() - market_cache.get("odds_refreshed_at", 0)
    _odds_valid = market_cache.get("odds_valid", False)
    if _odds_valid and 0.05 <= _yes_px <= 0.95:
        _age_str  = dim(f"{_odds_age:.0f}s ago")
        _yes_str  = (green if _yes_px >= 0.6 else yellow if _yes_px >= 0.45 else red)(f"{_yes_px:.3f}")
        _no_str   = (green if _no_px  >= 0.6 else yellow if _no_px  >= 0.45 else red)(f"{_no_px:.3f}")
        odds_str  = f"UP={_yes_str}  DOWN={_no_str}  {_age_str}"
    else:
        odds_str  = yellow("─ menunggu data...")

    print(f"|  {bold('MARKET DATA')}                                               |")
    print(f"|  BTC Price   : {btc_str}   WS: {ws_str}                |")
    print(f"|  Odds        : {odds_str}                     |")
    print(f"|  CVD (2min)  : {cvd_str}                              |")
    print(f"|  Liq SHORT   : 3s={liq_s3_str}  30s={liq_s30_str}              |")
    print(f"|  Liq LONG    : 3s={liq_l3_str}  30s={liq_l30_str}              |")
    print(f"+{'-'*62}+")
    print(f"|  {bold('LATE FILTERS')}  (all instant, no delay)                     |")
    print(f"|  [F1] Entry zone  : {f1_str}  |")
    print(f"|       min t={ENTRY_WINDOW_START}s, max t={300-MIN_WINDOW_REMAINING}s                             |")
    print(f"|  [F2] Beat dist   : {f2_str}  |")
    print(f"|       min |dist|=${MIN_BEAT_DISTANCE:.0f} in signal dir                       |")
    print(f"|  [F3] Liq dual    : {f3_str}  |")
    print(f"|       recent({LIQ_RECENT_WINDOW}s)>=${LIQ_RECENT_MIN_USD/1000:.0f}k, sustained({LIQ_SUSTAINED_WINDOW}s)>=${LIQ_SUSTAINED_MIN_USD/1000:.0f}k            |")
    print(f"|  [F4] CVD align   : {f4_str}  |")
    print(f"|       |cvd_2min|>=${CVD_MIN_THRESHOLD/1000:.0f}k in signal direction                  |")
    print(f"|  [F5] Odds (info) : {f5_str}  |")
    print(f"|       NO FILTER — odds ditampilkan untuk monitoring saja              |")
    print(f"|  Last skip    : {skip_str}  |")
    print(f"+{'-'*62}+")
    print(f"|  {bold('LAST SIGNAL')} : {sig_icon}  @ {cyan(sig_time)}                        |")
    print(f"+{'-'*62}+")

    # ── Active positions ──
    pending = tracker._pending if tracker else []
    print(f"|  {bold('ACTIVE POSITIONS')}  ({len(pending)} open)                              |")
    if not pending:
        print(f"|  {dim('─ tidak ada posisi aktif')}                               |")
    else:
        for pos in pending[-3:]:   # tampilkan max 3 posisi terbaru
            time_left = max(0, pos.window_end_ts - time.time())
            dir_icon  = green("^ UP  ") if pos.direction == "UP" else red("v DOWN")
            payout    = round((1.0 / pos.odds - 1.0) * pos.amount_usdc, 2) if pos.odds > 0 else 0
            pos_str   = (
                f"{dir_icon} ${pos.amount_usdc:.2f} @ {cyan(f'{pos.odds:.3f}')} "
                f"│ window {cyan(pos.window_id)} "
                f"│ sisa {yellow(f'{time_left:.0f}s')} "
                f"│ payout +${payout:.2f}"
            )
            print(f"|  {pos_str}  |")
    print(f"+{'-'*62}+")
    print(f"|  {bold('RESULTS')}                                                   |")
    bal_str = cyan(f"${balance:>8,.2f}")
    dry_bal = dim(" (sim)") if DRY_RUN else ""
    print(f"|  Balance   : {bal_str} USDC{dry_bal}                          |")
    if DRY_RUN:
        claim_thr = dim("─ DRY (claim off)")
    elif balance < CLAIM_CASH_THRESHOLD:
        claim_thr = yellow(f"!! LOW CASH — klaim diprioritaskan!")
    else:
        claim_thr = dim(f"OK (thr < ${CLAIM_CASH_THRESHOLD:.0f})")
    claim_info = claim_status if claim_status else dim("─ belum dicek")
    print(f"|  AutoClaim : {claim_thr:<28}                   |")
    print(f"|  Claim log : {claim_info:<48}|")
    print(f"|  Bets/hour : {cyan(str(bets_hr))}/{MAX_BETS_PER_HOUR}  │  Bet size: {yellow(f'${BET_AMOUNT:.2f}')}             |")
    total_str = cyan(f"{stats['total']:>4}")
    pend_str  = yellow(f"{stats['pending']:>2}")
    print(f"|  Resolved  : {total_str}  (pending: {pend_str})  WIN:{win_str} LOSS:{loss_str}  |")
    print(f"|  Win Rate  : {wr_str}  [{bar}]  |")
    print(f"|  Total PnL : {pnl_str}                                  |")
    if blocked:
        bs = blocked.stats
        b_acc_str = (green if bs["filter_acc_pct"] >= 70 else yellow if bs["filter_acc_pct"] >= 50 else red)(f"{bs['filter_acc_pct']:.1f}%")
        print(f"+{'-'*62}+")
        print(f"|  {bold('BLOCKED')}  resolved:{cyan(str(bs['total']))}  acc:{b_acc_str}  WIN:{green(str(bs['would_win']))} LOSS:{red(str(bs['would_loss']))}  |")
    # ── Manual / keyboard controls + session block status ──
    auto_str    = green("AUTO OK") if handler and handler.auto_enabled else yellow("MANUAL (auto OFF)")
    manual_msg  = getattr(handler, "_manual_msg", "")
    manual_disp = f"  last: {manual_msg}" if manual_msg else ""
    sess_blocked, sess_label = _in_session_block()
    if sess_blocked:
        sess_str = red(f"[X] SESSION BLOCK  {sess_label}  — no bets")
    elif not SESSION_BLOCK_ENABLED:
        sess_str = dim("─ session filter OFF")
    else:
        sess_str = green("OK clear (no session block)")
    print(f"+{'-'*62}+")
    print(f"|  {bold('MODE')} : {auto_str}{manual_disp}                                     |")
    print(f"|  {bold('SESSION')} : {sess_str}                    |")
    print(f"|  {dim('[U] bet UP  [D] bet DOWN  [A] toggle auto  (Windows only)')}  |")
    print(f"+{'-'*62}+")
    print(f"|  {dim('Logs: logs/late_live.log | logs/late_live_results.csv')}     |")
    print(f"+{'-'*62}+")
    print(f"  {dim('Ctrl+C to stop')}\n")


# ─────────────────────────────────────────────
# Keyboard Listener (Windows only)
# ─────────────────────────────────────────────
async def keyboard_listener(handler: LateHandler):
    """
    Non-blocking keyboard input loop.
    Windows: uses msvcrt.  Linux/VPS: uses termios raw mode.

    Keys:
      [U] — manual bet UP    (bypass filters)
      [D] — manual bet DOWN  (bypass filters)
      [A] — toggle AUTO / MANUAL mode
    """
    loop = asyncio.get_event_loop()

    def _handle_key(ch: str):
        ch = ch.lower()
        if ch == "u":
            asyncio.run_coroutine_threadsafe(handler.manual_bet("UP"), loop)
        elif ch == "d":
            asyncio.run_coroutine_threadsafe(handler.manual_bet("DOWN"), loop)
        elif ch == "a":
            handler.auto_enabled = not handler.auto_enabled
            state = "ON" if handler.auto_enabled else "OFF"
            handler._manual_msg = f"Auto-bet toggled: {state}"
            logger.warning(f"[KEYBOARD] Auto-bet {state}")
        elif ch == "\x03":   # Ctrl+C
            raise KeyboardInterrupt

    if sys.platform == "win32":
        logger.info("[KEYBOARD] Windows mode  [U]=UP  [D]=DOWN  [A]=toggle auto")
        while True:
            try:
                if msvcrt.kbhit():
                    raw = msvcrt.getch()
                    try:
                        _handle_key(raw.decode("utf-8"))
                    except (UnicodeDecodeError, KeyboardInterrupt):
                        pass
                    await asyncio.sleep(0.3)   # debounce
            except Exception as e:
                logger.debug(f"[KEYBOARD] Error: {e}")
            await asyncio.sleep(0.05)

    else:
        # Linux / VPS — termios raw mode
        import termios, tty, select
        fd          = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        logger.info("[KEYBOARD] Linux mode  [U]=UP  [D]=DOWN  [A]=toggle auto")
        try:
            tty.setcbreak(fd)
            while True:
                try:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.05)
                    if ready:
                        ch = sys.stdin.read(1)
                        _handle_key(ch)
                        await asyncio.sleep(0.3)   # debounce
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.debug(f"[KEYBOARD] Error: {e}")
                await asyncio.sleep(0.05)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    start_time = time.time()

    mode_label = "DRY RUN (simulation)" if DRY_RUN else "*** LIVE TRADING ***"

    print(f"\n{'='*64}")
    print(f"  POLYMARKET LATE ENTRY BOT")
    print(f"  Mode             : {mode_label}")
    print(f"  Strategy         : Enter ONLY at t=210-300s (last 90s)")
    print(f"  Filters          : ALL INSTANT (no confirmation delay)")
    print(f"  Bet amount       : ${BET_AMOUNT:.2f} (flat)")
    print(f"  Entry zone       : t >= {ENTRY_WINDOW_START}s AND remaining >= {MIN_WINDOW_REMAINING}s")
    print(f"  Beat distance    : |price - beat| >= ${MIN_BEAT_DISTANCE:.0f}")
    print(f"  Liq recent ({LIQ_RECENT_WINDOW}s)   : >= ${LIQ_RECENT_MIN_USD:,.0f}")
    print(f"  Liq sustained ({LIQ_SUSTAINED_WINDOW}s): >= ${LIQ_SUSTAINED_MIN_USD:,.0f}")
    print(f"  CVD threshold    : >= ${CVD_MIN_THRESHOLD:,.0f}")
    print(f"  Odds filter      : DISABLED (odds ditampilkan untuk monitoring)")
    print(f"  Max bets/hour    : {MAX_BETS_PER_HOUR}")
    print(f"  Auto-claim       : setiap {CLAIM_CHECK_INTERVAL}s atau saat cash < ${CLAIM_CASH_THRESHOLD:.0f}")
    print(f"{'='*64}\n")

    if not DRY_RUN:
        print(f"  *** LIVE TRADING MODE ***")
        confirm = input("  Type 'LIVE' to confirm: ").strip()
        if confirm != "LIVE":
            print("  Cancelled.")
            return
        print()

    # ── Init components ──
    data    = MarketData()
    ws      = HyperliquidWSFetcher(data)
    rest    = HyperliquidRESTFetcher(data, poll_interval=5)
    tracker = ResultTracker(data, "logs/late_live_results.csv")
    candle  = CandleTracker(history_size=20)

    blocked_tracker = BlockedTracker(data, "logs/late_live_blocked.csv")

    executor = PolymarketExecutor(
        dry_run  = DRY_RUN,
        min_odds = MIN_ODDS,
    )

    # ── Market cache — shared between handler and refresh loop ──
    market_cache = {
        "market"           : None,
        "question"         : "─ waiting...",
        "refreshed_at"     : 0.0,
        "yes_px"           : 0.0,
        "no_px"            : 0.0,
        "odds_refreshed_at": 0.0,
        "odds_valid"       : False,   # True setelah dapat harga valid (0.05-0.95)
        "next_question"    : "",
        "next_secs"        : 0.0,
    }

    handler = LateHandler(
        tracker      = tracker,
        candle       = candle,
        data         = data,
        blocked      = blocked_tracker,
        executor     = executor,
        market_cache = market_cache,
    )

    engine = SignalEngine(
        market_data         = data,
        liq_threshold_usd   = LIQ_THRESHOLD,
        eval_interval       = 1.0,
        log_path            = "logs/late_live_signals.csv",
        log_only_actionable = False,
    )
    engine._running = True
    engine.on_signal(handler.on_signal)

    # ── Candle update loop (1s) ──
    async def candle_update_loop():
        while True:
            if data.btc_price > 0:
                candle.update(data.btc_price)
            await asyncio.sleep(1)

    # ── Claim state — diupdate oleh _auto_claim_loop, dibaca oleh dashboard ──
    claim_state = {
        "status":       "",      # string untuk dashboard
        "last_balance": 999.0,   # diupdate di main loop agar claim loop tahu cash level
    }

    # ── Launch all tasks ──
    ws_task      = asyncio.create_task(ws.start())
    rest_task    = asyncio.create_task(rest.start())
    tracker_task = asyncio.create_task(tracker.run_loop())
    engine_task  = asyncio.create_task(engine.run_loop())
    blocked_task = asyncio.create_task(blocked_tracker.run_loop())
    candle_task  = asyncio.create_task(candle_update_loop())
    cache_task   = asyncio.create_task(_market_refresh_loop(executor, market_cache))
    claim_task   = asyncio.create_task(_auto_claim_loop(executor, claim_state, tracker))
    kb_task      = asyncio.create_task(keyboard_listener(handler))

    _prev_blocked_resolved = 0

    try:
        while True:
            await asyncio.sleep(5)  # faster dashboard refresh than main.py (5s vs 10s)

            # Sound notification for blocked resolved
            cur_blocked = blocked_tracker.stats["total"]
            if cur_blocked > _prev_blocked_resolved:
                asyncio.create_task(play_sound("blocked"))
                _prev_blocked_resolved = cur_blocked

            uptime  = time.time() - start_time
            balance = executor.get_balance()
            claim_state["last_balance"] = balance   # share balance ke claim loop
            print_late_dashboard(
                data, engine, tracker, uptime,
                candle, handler, blocked_tracker,
                market_cache=market_cache,
                balance=balance,
                claim_status=claim_state["status"],
            )

    except KeyboardInterrupt:
        print("\n\n  Stopping late bot...")

    finally:
        engine.stop()
        tracker.stop()
        ws.stop()
        rest.stop()
        blocked_tracker.stop()
        for t in [ws_task, rest_task, tracker_task, engine_task, blocked_task, candle_task, cache_task, claim_task, kb_task]:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

        # ── Final summary ──
        stats = tracker.stats
        print(f"\n{'='*64}")
        print(f"  LATE BOT STOPPED")
        print(f"{'='*64}")
        duration = time.time() - start_time
        print(f"  Duration      : {int(duration//60)}m {int(duration%60)}s")
        print(f"  Total bets    : {stats['total']}")
        print(f"  WIN           : {stats['wins']}")
        print(f"  LOSS          : {stats['losses']}")
        print(f"  Pending       : {stats['pending']}")
        print(f"  Win Rate      : {stats['win_rate']:.1f}%")
        print(f"  Total PnL     : ${stats['total_pnl']:+.2f}")
        print(f"{'='*64}\n")


if __name__ == "__main__":
    asyncio.run(main())

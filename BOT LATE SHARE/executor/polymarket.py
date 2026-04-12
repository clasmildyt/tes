"""
executor/polymarket.py
======================
Polymarket Executor — place bet UP/DOWN pada market BTC 5 menit.

Flow:
  1. Cari market BTC 5-menit yang aktif via Gamma API
  2. Ambil harga/odds saat ini
  3. Validasi odds >= MIN_ODDS
  4. Place order (atau simulasi di DRY_RUN)
  5. Log hasil ke logs/bets.csv

Autentikasi menggunakan private key dari .env.
DRY_RUN=true → simulasi saja, tidak ada bet sungguhan.
"""

import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config dari .env
# ─────────────────────────────────────────────
DRY_RUN      = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_ODDS     = float(os.getenv("MIN_ODDS", "0.0"))
PRIVATE_KEY  = os.getenv("POLYMARKET_PRIVATE_KEY", "")
FUNDER       = os.getenv("POLYMARKET_FUNDER", "")      # proxy/safe address (tempat saldo)
API_KEY      = os.getenv("POLYMARKET_API_KEY", "")
API_SECRET   = os.getenv("POLYMARKET_API_SECRET", "")
API_PASS     = os.getenv("POLYMARKET_API_PASSPHRASE", "")

# ─────────────────────────────────────────────
# Retry config
# ─────────────────────────────────────────────
BET_MAX_RETRIES      = 5      # max percobaan ulang
BET_RETRY_DELAY      = 0.15   # detik antar retry (150ms) — cepat agar muat dalam 30s window
MIN_MARKET_VOLUME    = 500.0  # minimum volume market ($) agar order book tidak terlalu tipis

# Polymarket endpoints
CLOB_HOST    = "https://clob.polymarket.com"
GAMMA_HOST   = "https://gamma-api.polymarket.com"
POLY_CHAIN   = 137  # Polygon mainnet


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────
@dataclass
class MarketInfo:
    """Info market Polymarket yang aktif."""
    condition_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    yes_price: float   # odds untuk YES (UP) — antara 0 dan 1
    no_price: float    # odds untuk NO (DOWN)
    end_date_iso: str
    active: bool
    volume: float = 0.0  # total volume market ($) untuk cek likuiditas


@dataclass
class BetResult:
    """Hasil satu eksekusi bet."""
    timestamp: float
    market_id: str
    question: str
    direction: str      # "UP" atau "DOWN"
    token_id: str
    amount_usdc: float
    odds: float
    order_id: str
    status: str         # "FILLED", "PENDING", "DRY_RUN", "FAILED"
    dry_run: bool
    error: str = ""


# ─────────────────────────────────────────────
# Bet Logger (CSV)
# ─────────────────────────────────────────────
BET_CSV_HEADERS = [
    "timestamp", "datetime", "market_id", "question",
    "direction", "token_id", "amount_usdc", "odds",
    "order_id", "status", "dry_run", "error",
]


class BetLogger:
    def __init__(self, log_path: str = "logs/bets.csv"):
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            with open(self.path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=BET_CSV_HEADERS).writeheader()
            logger.info(f"[EXEC] Created bet log: {self.path}")

    def write(self, result: BetResult):
        row = {
            "timestamp"   : result.timestamp,
            "datetime"    : datetime.fromtimestamp(result.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "market_id"   : result.market_id,
            "question"    : result.question,
            "direction"   : result.direction,
            "token_id"    : result.token_id,
            "amount_usdc" : round(result.amount_usdc, 4),
            "odds"        : round(result.odds, 4),
            "order_id"    : result.order_id,
            "status"      : result.status,
            "dry_run"     : result.dry_run,
            "error"       : result.error,
        }
        with open(self.path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=BET_CSV_HEADERS).writerow(row)
        logger.info(
            f"[EXEC] Bet logged: {result.direction} "
            f"${result.amount_usdc:.2f} @ {result.odds:.3f} "
            f"| {result.status}"
        )


# ─────────────────────────────────────────────
# Market Finder — cari market BTC 5 menit aktif
# ─────────────────────────────────────────────
class MarketFinder:
    """
    Cari market Polymarket yang aktif dengan keyword BTC dan 5 menit.
    Gunakan Gamma API (tanpa auth) untuk list market.
    """

    # Format market di Polymarket:
    # "Bitcoin Up or Down - March 21, 9:10AM-9:15AM ET"
    BTC_KEYWORDS  = ["btc", "bitcoin"]
    UPDOWN_MARKER = "up or down"

    @staticmethod
    def _is_current_window(
        question: str,
        max_ahead_sec: int = 420,
        end_date_str: str = "",
    ) -> bool:
        """
        Cek apakah window market sedang berlangsung / akan mulai dalam max_ahead_sec detik.

        Dua pendekatan (prioritas):
          1. Parse waktu end dari judul (format: "7:30AM-7:35AM ET") — paling akurat
          2. Fallback: parse endDate field dari API (ISO 8601 UTC)

        Return True  jika: 0 < secs_until_end <= max_ahead_sec
        Return False jika: market sudah lewat atau terlalu jauh ke depan
        Return True  jika: tidak bisa parse SAMA SEKALI (safe fallback)
        """
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td

        now_utc   = _dt.now(_tz.utc)
        et_offset = _td(hours=-4)   # EDT = UTC-4 (Maret-November)
        now_et    = now_utc + et_offset

        # ── Pendekatan 1: Parse waktu dari judul ──
        match = re.search(
            r'(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)',
            question, re.IGNORECASE
        )
        if match:
            h1, m1, p1, h2, m2, p2 = match.groups()
            h1, m1, h2, m2 = int(h1), int(m1), int(h2), int(m2)
            if p1.upper() == "PM" and h1 != 12: h1 += 12
            if p1.upper() == "AM" and h1 == 12: h1 = 0
            if p2.upper() == "PM" and h2 != 12: h2 += 12
            if p2.upper() == "AM" and h2 == 12: h2 = 0
            try:
                end_et = now_et.replace(hour=h2, minute=m2, second=0, microsecond=0)
                if end_et <= now_et:
                    end_et += _td(days=1)
                secs_until_end = (end_et - now_et).total_seconds()
                result = 0 < secs_until_end <= max_ahead_sec
                logger.info(
                    f"[EXEC] Window check (title) '{question[-35:]}': "
                    f"end ET={h2:02d}:{m2:02d} now_ET={now_et.strftime('%H:%M:%S')} "
                    f"secs_until={secs_until_end:.0f}s valid={result}"
                )
                return result
            except Exception:
                pass  # fallthrough ke pendekatan 2

        # ── Pendekatan 2: Fallback ke endDate dari API (UTC ISO string) ──
        if end_date_str:
            try:
                # Handle berbagai format ISO: "2026-03-22T11:35:00Z", "...000Z", "+00:00"
                ed = end_date_str.strip().replace("Z", "+00:00")
                # Hapus milidetik jika ada
                ed = re.sub(r'\.\d+\+', '+', ed)
                end_utc = _dt.fromisoformat(ed)
                secs_until_end = (end_utc - now_utc).total_seconds()
                result = 0 < secs_until_end <= max_ahead_sec
                logger.info(
                    f"[EXEC] Window check (endDate) '{question[-30:]}': "
                    f"endDate={end_date_str} "
                    f"secs_until={secs_until_end:.0f}s valid={result}"
                )
                return result
            except Exception as ex:
                logger.debug(f"[EXEC] endDate parse fail '{end_date_str}': {ex}")

        # ── Tidak bisa parse sama sekali → safe fallback ──
        logger.debug(f"[EXEC] Window check '{question[-30:]}': tidak bisa parse → include")
        return True

    @staticmethod
    def _is_5min_window(question: str) -> bool:
        """
        Pastikan market adalah window 5-menit bukan 15-menit atau lainnya.

        Format yang dikenali:
          Format A (lama): "Bitcoin Up or Down - March 22, 7:30AM-7:35AM ET"
                           → hitung selisih jam = 5 menit
          Format B (eksplisit): "Bitcoin Up or Down - 5 Minutes"
                           → ada "5 min" atau "5-min" di question
          Format C (baru 2025+): "Bitcoin Up or Down - March 23, 6:55"
                           → tanggal + satu waktu saja, TIDAK ada "15" di judul
                           → semua format ini adalah 5-min window
        """
        q_lower = question.lower()

        # Kalau ada "15" di judul → bukan 5-menit
        if re.search(r'\b15[\s-]?min(utes?)?\b', q_lower):
            return False

        # Format B: eksplisit "5 minutes" atau "5 min" atau "5-minute"
        if re.search(r'\b5[\s-]?min(utes?)?\b', q_lower):
            return True

        # Format A: parse start-end time → hitung selisih
        match = re.search(
            r'(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)',
            question, re.IGNORECASE
        )
        if match:
            h1, m1, p1, h2, m2, p2 = match.groups()
            h1, m1, h2, m2 = int(h1), int(m1), int(h2), int(m2)
            if p1.upper() == "PM" and h1 != 12: h1 += 12
            if p1.upper() == "AM" and h1 == 12: h1 = 0
            if p2.upper() == "PM" and h2 != 12: h2 += 12
            if p2.upper() == "AM" and h2 == 12: h2 = 0
            diff = ((h2 * 60 + m2) - (h1 * 60 + m1)) % (24 * 60)
            return diff == 5

        # Format C (baru): "Bitcoin Up or Down - March 23, 6:55"
        # Satu timestamp saja tanpa range → selalu 5-min window di Polymarket
        if re.search(r'[A-Za-z]+ \d{1,2},\s*\d{1,2}:\d{2}', question):
            return True

        return False

    def find_active_btc_5min(self) -> MarketInfo | None:
        """
        Cari market Bitcoin Up or Down 5-MENIT yang aktif paling awal berakhirnya.
        Filter ketat:
          - Hanya 5-menit window (bukan 15-menit, dsb)
          - Window harus sedang berlangsung atau paling lama 7 menit lagi
          - Tidak boleh window yang masih berjam-jam ke depan
        """
        MAX_END_AHEAD = 7 * 60   # 7 menit dalam detik

        try:
            from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td2

            now_utc    = _dt2.now(_tz2.utc)
            # Window: cari market yang berakhir antara sekarang s/d 8 menit ke depan
            end_min    = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_max    = (now_utc + _td2(minutes=8)).strftime("%Y-%m-%dT%H:%M:%SZ")

            markets = []

            # ── Strategi 1: filter berdasarkan endDate (paling akurat) ──
            # Langsung dapat market yang akan berakhir dalam 8 menit
            try:
                resp1 = requests.get(
                    f"{GAMMA_HOST}/markets",
                    params={
                        "closed"       : "false",
                        "limit"        : 50,
                        "end_date_min" : end_min,
                        "end_date_max" : end_max,
                    },
                    timeout=10,
                )
                if resp1.status_code == 200:
                    raw1 = resp1.json()
                    markets = raw1 if isinstance(raw1, list) else raw1.get("markets", raw1.get("data", []))
                    logger.debug(f"[EXEC] Strategi 1 (endDate filter): {len(markets)} markets")
            except Exception as e1:
                logger.debug(f"[EXEC] Strategi 1 gagal: {e1}")

            # ── Strategi 2: fallback — keyword search by volume ──
            # Dipakai kalau strategi 1 tidak dapat BTC market
            has_btc = any(
                any(k in m.get("question", "").lower() for k in self.BTC_KEYWORDS)
                and self.UPDOWN_MARKER in m.get("question", "").lower()
                for m in markets
            )
            if not has_btc:
                for search_kw in ["Bitcoin Up or Down", "Bitcoin Up or Down - 5 Minutes", None]:
                    p2 = {"closed": "false", "limit": 100, "order": "volume", "ascending": "false"}
                    if search_kw:
                        p2["search"] = search_kw
                    try:
                        resp2 = requests.get(f"{GAMMA_HOST}/markets", params=p2, timeout=10)
                        resp2.raise_for_status()
                        raw2    = resp2.json()
                        mkts2   = raw2 if isinstance(raw2, list) else raw2.get("markets", raw2.get("data", []))
                        has_b2  = any(
                            any(k in m.get("question","").lower() for k in self.BTC_KEYWORDS)
                            and self.UPDOWN_MARKER in m.get("question","").lower()
                            for m in mkts2
                        )
                        if has_b2:
                            markets = mkts2
                            logger.debug(f"[EXEC] Strategi 2 (keyword='{search_kw}'): {len(markets)} markets")
                            break
                    except Exception:
                        pass

            # Log semua market BTC Up/Down yang ditemukan (sebelum time filter)
            btc_all = [
                m for m in markets
                if any(k in m.get("question","").lower() for k in self.BTC_KEYWORDS)
                and self.UPDOWN_MARKER in m.get("question","").lower()
            ]
            logger.info(
                f"[EXEC] Gamma API: {len(markets)} total markets, "
                f"{len(btc_all)} BTC Up/Down markets ditemukan"
            )
            for m in btc_all[:8]:   # log max 8 pertama
                q5 = m.get('question','')
                ed = m.get("endDate") or m.get("end_date") or m.get("end_date_iso") or "—"
                logger.info(
                    f"[EXEC]   - active={m.get('active')} closed={m.get('closed')} "
                    f"5min={self._is_5min_window(q5)} "
                    f"endDate={ed} "
                    f"| {q5[-55:]}"
                )

            candidates = []
            for m in markets:
                question = m.get("question", "")
                q_lower  = question.lower()

                is_btc    = any(k in q_lower for k in self.BTC_KEYWORDS)
                is_updown = self.UPDOWN_MARKER in q_lower
                is_5min   = self._is_5min_window(question)

                # is_active: gunakan active=True ATAU closed=False (lebih toleran)
                # Polymarket kadang set active=False saat window sudah dimulai/close
                is_active = m.get("active", False) or not m.get("closed", True)

                if not (is_btc and is_updown and is_5min):
                    continue
                if not is_active:
                    logger.info(f"[EXEC] Skip (inactive+closed): active={m.get('active')} closed={m.get('closed')} | {question[-50:]}")
                    continue

                # ── Filter waktu: window harus sedang berlangsung atau maks 7 menit lagi ──
                # Pass endDate sebagai fallback untuk market format baru (tanpa waktu di judul)
                end_date = m.get("endDate") or m.get("end_date_iso") or ""
                if not self._is_current_window(
                    question,
                    max_ahead_sec=MAX_END_AHEAD,
                    end_date_str=end_date,
                ):
                    continue

                candidates.append(m)

            if not candidates:
                logger.info("[EXEC] Tidak ada market BTC 5-min aktif dalam 7 menit ke depan")
                return None

            # Pilih yang end_date paling awal (window yang paling dekat berakhir)
            # Sorting berdasarkan waktu end dari judul market (lebih reliable dari endDate field)
            def _sort_key(m):
                q = m.get("question", "")
                match = re.search(
                    r'(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)',
                    q, re.IGNORECASE
                )
                if not match:
                    return 9999
                _, _, _, h2, m2, p2 = match.groups()
                h2, m2 = int(h2), int(m2)
                if p2.upper() == "PM" and h2 != 12: h2 += 12
                if p2.upper() == "AM" and h2 == 12: h2 = 0
                return h2 * 60 + m2
            try:
                candidates.sort(key=_sort_key)
            except Exception:
                pass

            best = candidates[0]
            logger.info(f"[EXEC] Market found: {best.get('question','')}")
            return self._parse_market(best)

        except requests.RequestException as e:
            logger.error(f"[EXEC] Gamma API error: {e}")
            return None

    def find_next_btc_5min(self) -> tuple[MarketInfo | None, float]:
        """
        Cari market BTC 5-menit yang AKAN datang.

        Strategi dua tahap:
          1. TARGETED SEARCH — hitung waktu window berikutnya secara tepat,
             lalu cari market dengan title mengandung jam tersebut.
             Market baru punya volume ~$0 sehingga tidak muncul di top-200
             volume; targeted search menemukan mereka langsung.
          2. FALLBACK — jika tidak ketemu (market belum dibuat), pakai
             volume-sort limit=200 untuk window yang lebih jauh.

        Returns:
            (MarketInfo, secs_until_start) — market berikutnya + detik sampai dimulai
            (None, 0) jika tidak ada yang ditemukan
        """
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td

        _MONTHS = [
            "January","February","March","April","May","June",
            "July","August","September","October","November","December",
        ]

        now_utc   = _dt.now(_tz.utc)
        et_offset = _td(hours=-4)   # EDT (UTC-4)
        now_et    = now_utc + et_offset

        # ── Hitung window berikutnya dalam ET ──
        cur_min_block = (now_et.minute // 5) * 5
        cur_win_start = now_et.replace(minute=cur_min_block, second=0, microsecond=0)

        def _try_targeted(window_start_et, secs_until_start):
            """Coba cari market untuk window tertentu via targeted search."""
            window_end_et = window_start_et + _td(minutes=5)
            month_name    = _MONTHS[window_start_et.month - 1]
            day           = window_start_et.day

            hs, ms = window_start_et.hour, window_start_et.minute
            he, me = window_end_et.hour,   window_end_et.minute
            aps = "AM" if hs < 12 else "PM"
            ape = "AM" if he < 12 else "PM"
            h12s = hs % 12 or 12
            h12e = he % 12 or 12

            # Beberapa format judul yang mungkin dipakai Polymarket
            candidates = [
                f"Bitcoin Up or Down - {month_name} {day}, {h12s}:{ms:02d}{aps}-{h12e}:{me:02d}{ape}",
                f"Bitcoin Up or Down - {month_name} {day}, {h12s}:{ms:02d} {aps}-{h12e}:{me:02d} {ape}",
                f"Bitcoin Up or Down - {month_name} {day}, {h12s}:{ms:02d}",
            ]

            for search_str in candidates:
                try:
                    params = {
                        "closed" : "false",
                        "limit"  : 10,
                        "search" : search_str,
                    }
                    resp = requests.get(f"{GAMMA_HOST}/markets", params=params, timeout=8)
                    if not resp.ok:
                        continue
                    raw  = resp.json()
                    mkts = raw if isinstance(raw, list) else raw.get("markets", raw.get("data", []))
                    for m in mkts:
                        q      = m.get("question", "")
                        ql     = q.lower()
                        is_btc = any(k in ql for k in self.BTC_KEYWORDS)
                        is_ud  = self.UPDOWN_MARKER in ql
                        is_5m  = self._is_5min_window(q)
                        is_act = m.get("active", False) or not m.get("closed", True)
                        if is_btc and is_ud and is_5m and is_act:
                            parsed = self._parse_market(m)
                            if parsed:
                                logger.debug(
                                    f"[EXEC] find_next targeted hit: {q[:60]} "
                                    f"(in {secs_until_start:.0f}s)"
                                )
                                return parsed, max(0.0, secs_until_start)
                except Exception:
                    continue
            return None, 0.0

        # ── Tahap 1: targeted search untuk 6 window ke depan (30 menit) ──
        for i in range(1, 7):
            win_start_et   = cur_win_start + _td(minutes=5 * i)
            elapsed_in_cur = (now_et - cur_win_start).total_seconds()
            secs_to_start  = (5 * i * 60) - elapsed_in_cur
            if secs_to_start <= 0:
                continue
            mkt, secs = _try_targeted(win_start_et, secs_to_start)
            if mkt:
                return mkt, secs

        # ── Tahap 2: fallback volume-sort untuk window lebih jauh ──
        try:
            params = {
                "closed"   : "false",
                "limit"    : 200,
                "order"    : "volume",
                "ascending": "false",
                "search"   : "Bitcoin Up or Down",
            }
            resp = requests.get(f"{GAMMA_HOST}/markets", params=params, timeout=10)
            resp.raise_for_status()
            raw     = resp.json()
            markets = raw if isinstance(raw, list) else raw.get("markets", raw.get("data", []))

            best_mkt        = None
            best_secs       = float("inf")
            best_start_secs = 0.0

            for m in markets:
                question = m.get("question", "")
                q_lower  = question.lower()
                if not (any(k in q_lower for k in self.BTC_KEYWORDS)
                        and self.UPDOWN_MARKER in q_lower
                        and self._is_5min_window(question)):
                    continue
                if not (m.get("active", False) or not m.get("closed", True)):
                    continue

                secs_until_start = None
                secs_until_end   = None

                # Parse waktu dari title dulu
                title_match = re.search(
                    r'(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)',
                    question, re.IGNORECASE
                )
                if title_match:
                    h1, m1, p1, h2, m2, p2 = title_match.groups()
                    h1, m1, h2, m2 = int(h1), int(m1), int(h2), int(m2)
                    if p1.upper() == "PM" and h1 != 12: h1 += 12
                    if p1.upper() == "AM" and h1 == 12: h1 = 0
                    if p2.upper() == "PM" and h2 != 12: h2 += 12
                    if p2.upper() == "AM" and h2 == 12: h2 = 0
                    try:
                        end_et   = now_et.replace(hour=h2, minute=m2, second=0, microsecond=0)
                        start_et = now_et.replace(hour=h1, minute=m1, second=0, microsecond=0)
                        if end_et <= now_et:
                            end_et   += _td(days=1)
                            start_et += _td(days=1)
                        secs_until_end   = (end_et   - now_et).total_seconds()
                        secs_until_start = (start_et - now_et).total_seconds()
                    except Exception:
                        pass

                # Fallback endDate
                if secs_until_end is None:
                    ed_raw = m.get("endDate") or m.get("end_date_iso") or ""
                    if ed_raw:
                        try:
                            ed  = re.sub(r'\.\d+\+', '+', ed_raw.strip().replace("Z", "+00:00"))
                            edt = _dt.fromisoformat(ed)
                            sdt = edt - _td(minutes=5)
                            secs_until_end   = (edt - now_utc).total_seconds()
                            secs_until_start = (sdt - now_utc).total_seconds()
                        except Exception:
                            pass

                if secs_until_end is None or secs_until_start is None:
                    continue
                if secs_until_end   <= 0:
                    continue
                if secs_until_start <= 0:
                    continue

                if secs_until_start < best_secs:
                    best_secs       = secs_until_start
                    best_mkt        = m
                    best_start_secs = max(0.0, secs_until_start)

            if best_mkt is None:
                return None, 0.0
            parsed = self._parse_market(best_mkt)
            return parsed, best_start_secs

        except requests.RequestException as e:
            logger.error(f"[EXEC] find_next fallback error: {e}")
            return None, 0.0

    def _parse_market(self, m: dict) -> MarketInfo | None:
        """
        Parse response Gamma API jadi MarketInfo.
        Handle dua format token:
          Format A: tokens = [{"outcome":"YES","token_id":"...","price":0.5}, ...]
          Format B: clobTokenIds = ["yes_token_id", "no_token_id"]
        """
        try:
            tokens = m.get("tokens", [])

            # Format B: clobTokenIds (format baru Gamma API) — tersimpan sebagai JSON string
            if len(tokens) < 2:
                import json as _json
                raw_ids    = m.get("clobTokenIds", [])
                raw_prices = m.get("outcomePrices", [])

                # Parse dari JSON string jika perlu
                if isinstance(raw_ids, str):
                    try:
                        raw_ids = _json.loads(raw_ids)
                    except Exception:
                        raw_ids = []
                if isinstance(raw_prices, str):
                    try:
                        raw_prices = _json.loads(raw_prices)
                    except Exception:
                        raw_prices = []

                if len(raw_ids) >= 2:
                    yes_p = float(raw_prices[0]) if len(raw_prices) >= 2 else 0.5
                    no_p  = float(raw_prices[1]) if len(raw_prices) >= 2 else 0.5
                    tokens = [
                        {"outcome": "YES", "token_id": raw_ids[0], "price": yes_p},
                        {"outcome": "NO",  "token_id": raw_ids[1], "price": no_p},
                    ]
                    logger.debug(f"[EXEC] Menggunakan clobTokenIds (parsed JSON)")

            if len(tokens) < 2:
                logger.error(f"[EXEC] Tidak ada token ditemukan di market: {m.get('question','')}")
                return None

            yes_token = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), tokens[0])
            no_token  = next((t for t in tokens if t.get("outcome", "").upper() == "NO"),  tokens[1])

            yes_price = float(yes_token.get("price", 0.5))
            no_price  = float(no_token.get("price", 0.5))

            # Jika harga masih default 0.5 (dari clobTokenIds), fetch dari CLOB
            yes_id = yes_token.get("token_id", "")
            if yes_id and yes_price == 0.5:
                try:
                    r = requests.get(
                        f"{CLOB_HOST}/price",
                        params={"token_id": yes_id, "side": "buy"},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        yes_price = float(r.json().get("price", 0.5))
                        no_price  = round(1.0 - yes_price, 4)
                except Exception:
                    pass

            # Parse volume dari field yang tersedia
            vol_raw = m.get("volume", m.get("volumeClob", m.get("volume24hr", 0)))
            try:
                volume = float(vol_raw) if vol_raw else 0.0
            except (ValueError, TypeError):
                volume = 0.0

            return MarketInfo(
                condition_id  = m.get("conditionId", ""),
                question      = m.get("question", ""),
                yes_token_id  = yes_token.get("token_id", ""),
                no_token_id   = no_token.get("token_id", ""),
                yes_price     = yes_price,
                no_price      = no_price,
                end_date_iso  = m.get("endDate", ""),
                active        = m.get("active", False),
                volume        = volume,
            )
        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"[EXEC] Market parse error: {e}")
            return None

    def get_current_prices(self, market: MarketInfo) -> tuple[float, float] | None:
        """
        Ambil harga terkini dari CLOB orderbook.
        Return (yes_price, no_price) atau None jika gagal.

        Harga dari orderbook lebih akurat daripada Gamma API.
        """
        try:
            # Ambil best ask untuk YES token (harga beli)
            resp = requests.get(
                f"{CLOB_HOST}/price",
                params={
                    "token_id": market.yes_token_id,
                    "side": "buy",
                },
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                yes_price = float(data.get("price", market.yes_price))
                no_price  = round(1.0 - yes_price, 4)
                return yes_price, no_price

        except requests.RequestException as e:
            logger.warning(f"[EXEC] Price fetch error: {e}, pakai harga Gamma")

        return market.yes_price, market.no_price


# ─────────────────────────────────────────────
# Polymarket Executor
# ─────────────────────────────────────────────
class PolymarketExecutor:
    """
    Eksekutor bet di Polymarket.
    DRY_RUN=true → simulasi tanpa transaksi nyata.
    DRY_RUN=false → perlu POLYMARKET_PRIVATE_KEY di .env.
    """

    def __init__(
        self,
        dry_run: bool = True,
        min_odds: float = MIN_ODDS,
        log_path: str = "logs/bets.csv",
    ):
        self.dry_run  = dry_run
        self.min_odds = min_odds
        self.finder   = MarketFinder()
        self.bet_log  = BetLogger(log_path)
        self._client  = None   # CLOB client (init saat pertama bet)

        # Dedup guard — set condition_id yang sedang dieksekusi
        # Mencegah double-bet jika AUTO dan MANUAL triggered bersamaan
        self._executing: set[str] = set()

        # ── Gas-free relayer (opsional, butuh BUILDER_API_KEY di .env) ──
        from executor.claim_relayer import ClaimRelayer
        self._relayer = ClaimRelayer()

        mode = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"[EXEC] Polymarket Executor initialized ({mode} mode)")

        if not dry_run:
            self._init_client()

    def _init_client(self):
        """
        Inisialisasi py-clob-client dengan private key dari .env.
        Hanya dipanggil di LIVE mode.
        """
        if not PRIVATE_KEY:
            raise ValueError(
                "[EXEC] POLYMARKET_PRIVATE_KEY tidak ditemukan di .env! "
                "Set DRY_RUN=true untuk testing."
            )

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key        = API_KEY,
                api_secret     = API_SECRET,
                api_passphrase = API_PASS,
            )
            # funder = proxy/safe address tempat saldo USDC disimpan
            # signature_type=1 diperlukan saat menggunakan funder address
            self._client = ClobClient(
                host           = CLOB_HOST,
                chain_id       = POLY_CHAIN,
                key            = PRIVATE_KEY,
                creds          = creds,
                funder         = FUNDER if FUNDER else None,
                signature_type = 1 if FUNDER else 0,
            )
            funder_info = f" | funder: {FUNDER[:10]}..." if FUNDER else " | no funder"
            logger.info(f"[EXEC] CLOB client connected (LIVE mode){funder_info}")

        except ImportError:
            raise ImportError("Install py-clob-client: pip install py-clob-client")
        except Exception as e:
            raise RuntimeError(f"[EXEC] CLOB client init failed: {e}")

    def get_balance(self) -> float:
        """
        Ambil saldo USDC (collateral) dari Polymarket via CLOB SDK.
        Method yang benar: get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        Response: {"balance": "9.00", "allowance": "..."}
        """
        if self.dry_run:
            return 1000.0  # Saldo simulasi

        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            resp   = self._client.get_balance_allowance(params)

            # Normalisasi response ke dict — SDK bisa kembalikan berbagai tipe
            if isinstance(resp, dict):
                data = resp
            elif hasattr(resp, "json") and callable(resp.json):
                # requests.Response
                try:
                    data = resp.json()
                except Exception:
                    data = {}
            elif hasattr(resp, "__dict__"):
                # object/namespace
                data = vars(resp)
            else:
                # coba cast ke str dulu lalu parse
                try:
                    import json as _json
                    data = _json.loads(str(resp))
                except Exception:
                    data = {}

            # Log raw response untuk debug
            logger.info(f"[EXEC] balance_allowance raw: {data}")

            # Cari field balance/allowance
            for key in ("balance", "Balance", "USDC", "usdc", "amount", "BALANCE"):
                val = data.get(key) if isinstance(data, dict) else getattr(data, key, None)
                if val is not None and str(val).strip() not in ("", "None"):
                    raw = float(str(val).strip())
                    # USDC bisa dalam micro-USDC (6 desimal) — cek apakah > 1000
                    if raw > 1000:
                        return round(raw / 1_000_000, 4)
                    return round(raw, 4)

            logger.warning(f"[EXEC] Field balance tidak ditemukan dalam response: {data}")
            return 0.0

        except Exception as e:
            logger.error(f"[EXEC] get_balance error: {e}")
            return 0.0

    # ──────────────────────────────────────────────────────────────────
    # Auto-Claim — Redeem winning positions agar saldo kembali ke USDC
    # ──────────────────────────────────────────────────────────────────

    def _get_wallet_address(self) -> str:
        """Return alamat wallet yang menyimpan posisi (FUNDER jika ada, else dari private key)."""
        if FUNDER:
            return FUNDER
        if PRIVATE_KEY:
            try:
                from eth_account import Account
                return Account.from_key(PRIVATE_KEY).address
            except Exception:
                pass
        return ""

    def get_redeemable_positions(self) -> list[dict]:
        """
        Fetch posisi yang bisa di-claim (redeemable = menang & sudah resolved).
        Menggunakan Polymarket Data API: data-api.polymarket.com/positions
        Returns list of position dicts — tiap dict punya 'conditionId', 'title', 'size'.
        """
        if self.dry_run:
            return []

        address = self._get_wallet_address()
        if not address:
            logger.warning("[EXEC] Tidak bisa mendapatkan wallet address untuk claim check")
            return []

        try:
            resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": address, "sizeThreshold": "0.01"},
                timeout=10,
            )
            if resp.status_code != 200:
                logger.warning(f"[EXEC] Data API error {resp.status_code}: {resp.text[:80]}")
                return []

            raw       = resp.json()
            positions = raw if isinstance(raw, list) else raw.get("data", raw.get("positions", []))

            redeemable = []
            for p in positions:
                # ── Lapis 1: field redeemable dari Polymarket Data API ──
                # Hanya True jika market sudah resolved & posisi menang
                if not p.get("redeemable") is True:
                    continue

                # ── Lapis 2: pastikan market benar-benar closed/resolved ──
                # curPrice = 0 atau 1 menandakan market sudah selesai
                # Active market selalu punya curPrice antara 0.05-0.95
                cur_price = float(p.get("curPrice", 0.5))
                if 0.05 < cur_price < 0.95:
                    logger.warning(
                        f"[EXEC] SKIP claim — market masih aktif "
                        f"(curPrice={cur_price:.2f}): {p.get('title','?')[:40]}"
                    )
                    continue

                # ── Lapis 3: size harus > 0 (ada token yang bisa di-redeem) ──
                size = float(p.get("size", 0))
                if size <= 0:
                    continue

                redeemable.append(p)
                logger.debug(
                    f"[EXEC] Redeemable: {p.get('title','?')[:40]} "
                    f"| size={size} | curPrice={cur_price}"
                )

            logger.info(
                f"[EXEC] Redeemable positions: {len(redeemable)}/{len(positions)} "
                f"(skipped active/empty: {len(positions)-len(redeemable)})"
            )
            return redeemable

        except Exception as e:
            logger.error(f"[EXEC] get_redeemable_positions error: {e}")
            return []

    def claim_position(self, condition_id: str) -> bool:
        """
        Redeem posisi yang menang.
        Prioritas:
          1. Gas-free via Relayer API (jika BUILDER_API_KEY tersedia)
          2. Fallback: on-chain via Polygon RPC (butuh MATIC untuk gas)
        Returns True jika berhasil.
        """
        if self.dry_run:
            logger.info(f"[EXEC] [DRY] Simulasi claim {condition_id[:16]}...")
            return True

        # ── Prioritas 1: Gas-free via Relayer ──
        if self._relayer.is_available():
            logger.info(f"[EXEC] Mencoba claim via relayer (gas-free): {condition_id[:16]}...")
            if self._relayer.claim(condition_id):
                return True
            logger.warning("[EXEC] Relayer gagal (quota/error) — akan retry di siklus berikutnya")
            return False

    def claim_all_winnings(
        self,
        skip_condition_ids: set[str] | None = None,
        skip_window_ids:    set[str] | None = None,
    ) -> tuple[int, int]:
        """
        Claim semua posisi yang redeemable (menang & resolved).

        skip_condition_ids: set hex condition_id yang masih aktif di bot.
        skip_window_ids   : set window_id string ("06:35") posisi yang masih aktif
                            di bot — dicocokkan ke title market (e.g. "6:35AM-6:40AM").
                            Ini mencegah claim posisi yang window-nya belum selesai.
        Returns (claimed_count, total_redeemable).
        """
        positions = self.get_redeemable_positions()
        if not positions:
            return 0, 0

        skip_cids = skip_condition_ids or set()
        skip_wids = skip_window_ids    or set()

        logger.info(f"[EXEC] Auto-claim: {len(positions)} posisi ditemukan, mulai claim...")
        claimed        = 0
        seen: set[str] = set()

        for pos in positions:
            cond_id = pos.get("conditionId", "")
            if not cond_id or cond_id in seen:
                continue
            seen.add(cond_id)

            # ── Lapis 4a: skip via condition_id ──
            if cond_id in skip_cids:
                logger.warning(
                    f"[EXEC] SKIP — condition_id masih aktif di bot: {cond_id[:16]}..."
                )
                continue

            # ── Lapis 4b: skip via window_id yang ada di title market ──
            # window_id bot: "06:35" → cocokkan ke title "...6:35AM-6:40AM..."
            title = pos.get("title", "")
            if skip_wids:
                # Normalise: hilangkan leading zero — "06:35" → "6:35"
                active_in_title = any(
                    wid.lstrip("0") in title or wid in title
                    for wid in skip_wids
                )
                if active_in_title:
                    logger.warning(
                        f"[EXEC] SKIP — window masih aktif di bot: '{title[:50]}'"
                    )
                    continue

            size = float(pos.get("size", 0))
            logger.info(f"[EXEC] Claiming: {title[:40]} | size={size}")

            if self.claim_position(cond_id):
                claimed += 1

            time.sleep(1.0)   # jeda antar claim agar nonce tidak tabrakan

        logger.info(f"[EXEC] Auto-claim selesai: {claimed}/{len(seen)} berhasil")
        return claimed, len(seen)

    def place_bet(
        self,
        direction: str,
        amount_usdc: float,
        signal_reason: str = "",
    ) -> BetResult | None:
        """
        Entry point utama — place bet UP atau DOWN.

        Args:
            direction    : "UP" atau "DOWN"
            amount_usdc  : jumlah USDC yang akan dibet
            signal_reason: alasan dari signal engine (untuk log)

        Returns:
            BetResult jika berhasil, None jika skip/gagal
        """
        now = time.time()

        # ── Step 1: Cari market aktif ──
        market = self.finder.find_active_btc_5min()
        if market is None:
            logger.warning("[EXEC] Tidak ada market aktif, skip bet")
            return None

        logger.info(
            f"[EXEC] Market: {market.question[:60]} "
            f"| Vol: ${market.volume:,.0f}"
        )

        # ── Step 1b: Cek likuiditas market ──
        # Market dengan volume terlalu rendah sering kena FOK killed (order book tipis)
        if not self.dry_run and market.volume < MIN_MARKET_VOLUME:
            logger.warning(
                f"[EXEC] Market volume terlalu rendah: "
                f"${market.volume:,.0f} < ${MIN_MARKET_VOLUME:,.0f} — skip bet "
                f"(order book mungkin terlalu tipis, risiko FOK killed)"
            )
            return None

        # ── Step 2: Dedup guard — cegah double bet di window yang sama ──
        cid = market.condition_id
        if cid in self._executing:
            logger.warning(
                f"[EXEC] Bet sudah sedang dieksekusi untuk window ini "
                f"({cid[:16]}...) — skip"
            )
            return None
        self._executing.add(cid)

        try:
            # ── Step 3: Update harga terkini ──
            yes_price, no_price = self.finder.get_current_prices(market)
            market.yes_price = yes_price
            market.no_price  = no_price

            # ── Step 4: Tentukan token & odds berdasarkan direction ──
            if direction == "UP":
                token_id = market.yes_token_id
                odds     = yes_price
                label    = "YES (UP)"
            elif direction == "DOWN":
                token_id = market.no_token_id
                odds     = no_price
                label    = "NO (DOWN)"
            else:
                logger.error(f"[EXEC] Direction tidak valid: {direction}")
                return None

            # ── Step 5: Cek minimum odds (jika diset) ──
            if self.min_odds > 0 and odds < self.min_odds:
                logger.warning(
                    f"[EXEC] Odds {odds:.3f} < minimum {self.min_odds:.3f} "
                    f"untuk {label} — SKIP bet"
                )
                return None

            logger.info(
                f"[EXEC] {'[DRY RUN] ' if self.dry_run else ''}"
                f"Placing {direction} bet | "
                f"${amount_usdc:.2f} USDC @ {odds:.3f} odds | "
                f"Token: {token_id[:12]}..."
            )

            # ── Step 6: Eksekusi bet ──
            if self.dry_run:
                result = self._simulate_bet(
                    market, direction, token_id, amount_usdc, odds, now
                )
            else:
                result = self._live_bet_with_retry(
                    market, direction, token_id, amount_usdc, odds, now
                )

            # ── Step 7: Log hasil ──
            if result:
                self.bet_log.write(result)

            return result

        finally:
            # Selalu hapus guard setelah selesai (sukses atau gagal)
            self._executing.discard(cid)

    def place_bet_on_market(
        self,
        market: "MarketInfo",
        direction: str,
        amount_usdc: float,
        signal_reason: str = "",
    ) -> "BetResult | None":
        """
        Place bet pada market yang SUDAH DITEMUKAN sebelumnya.
        Berbeda dari place_bet() yang mencari market sendiri via find_active_btc_5min().

        Digunakan oleh early.py untuk bet pada market BERIKUTNYA (belum dimulai)
        menggunakan hasil find_next_btc_5min() — Polymarket menerima bet pada
        market future sebelum window dimulai.

        Args:
            market       : MarketInfo dari find_next_btc_5min() atau find_active_btc_5min()
            direction    : "UP" atau "DOWN"
            amount_usdc  : jumlah USDC yang akan dibet
            signal_reason: alasan untuk log

        Returns:
            BetResult jika berhasil, None jika skip/gagal
        """
        now = time.time()
        cid = market.condition_id

        # Dedup guard
        if cid in self._executing:
            logger.warning(f"[EXEC] place_bet_on_market: dedup skip ({cid[:16]}...)")
            return None
        self._executing.add(cid)

        try:
            # Ambil harga terkini
            yes_price, no_price = self.finder.get_current_prices(market)
            market.yes_price = yes_price
            market.no_price  = no_price

            if direction == "UP":
                token_id = market.yes_token_id
                odds     = yes_price
            elif direction == "DOWN":
                token_id = market.no_token_id
                odds     = no_price
            else:
                logger.error(f"[EXEC] place_bet_on_market: direction tidak valid: {direction}")
                return None

            logger.info(
                f"[EXEC] {'[DRY] ' if self.dry_run else ''}"
                f"place_bet_on_market {direction} | "
                f"${amount_usdc:.2f} @ {odds:.3f} | "
                f"{market.question[:50]} | {signal_reason}"
            )

            if self.dry_run:
                result = self._simulate_bet(market, direction, token_id, amount_usdc, odds, now)
            else:
                result = self._live_bet_with_retry(market, direction, token_id, amount_usdc, odds, now)

            if result:
                self.bet_log.write(result)

            return result

        finally:
            self._executing.discard(cid)

    def _simulate_bet(
        self,
        market: MarketInfo,
        direction: str,
        token_id: str,
        amount_usdc: float,
        odds: float,
        timestamp: float,
    ) -> BetResult:
        """Simulasi bet tanpa transaksi nyata (DRY_RUN mode)."""
        fake_order_id = f"DRY-{int(timestamp)}-{direction}"

        print(f"\n  ┌─ [DRY RUN] BET SIMULATION ──────────────────────┐")
        print(f"  │  Direction  : {direction}")
        print(f"  │  Amount     : ${amount_usdc:.2f} USDC")
        print(f"  │  Odds       : {odds:.4f} ({odds*100:.1f}%)")
        print(f"  │  Potential  : ${amount_usdc / odds:.2f} USDC")
        print(f"  │  Market     : {market.question[:45]}...")
        print(f"  │  Order ID   : {fake_order_id}")
        print(f"  └──────────────────────────────────────────────────┘\n")

        return BetResult(
            timestamp     = timestamp,
            market_id     = market.condition_id,
            question      = market.question,
            direction     = direction,
            token_id      = token_id,
            amount_usdc   = amount_usdc,
            odds          = odds,
            order_id      = fake_order_id,
            status        = "DRY_RUN",
            dry_run       = True,
        )

    def _live_bet_with_retry(
        self,
        market: MarketInfo,
        direction: str,
        token_id: str,
        amount_usdc: float,
        odds: float,
        timestamp: float,
    ) -> BetResult:
        """
        Place bet sungguhan via CLOB API dengan retry otomatis.

        Strategi:
          - Max BET_MAX_RETRIES percobaan (default 3)
          - Delay antar retry: BET_RETRY_DELAY detik (default 500ms)
          - Setiap retry: re-fetch harga terbaru (slippage sudah terupdate)
          - Jika order FILLED → stop, return sukses
          - Retry juga untuk: network error, ConnectionError, Timeout
          - Jika semua retry gagal → return status FAILED (tidak double-bet
            karena dedup guard di place_bet sudah mencegah call paralel)
        """
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        import requests as _requests

        last_status = "FAILED"
        last_error  = ""
        order_id    = "FAILED"

        for attempt in range(1, BET_MAX_RETRIES + 1):
            try:
                # Retry: ambil harga terbaru agar tidak kena slippage lama
                if attempt > 1:
                    logger.info(
                        f"[EXEC] Retry {attempt}/{BET_MAX_RETRIES} | "
                        f"{direction} | Re-fetching price..."
                    )
                    fresh_yes, fresh_no = self.finder.get_current_prices(market)
                    market.yes_price = fresh_yes
                    market.no_price  = fresh_no
                    if direction == "UP":
                        token_id = market.yes_token_id
                        odds     = fresh_yes
                    else:
                        token_id = market.no_token_id
                        odds     = fresh_no

                order_args   = MarketOrderArgs(token_id=token_id, amount=amount_usdc, side="BUY")
                signed_order = self._client.create_market_order(order_args)
                response     = self._client.post_order(signed_order, OrderType.FOK)

                order_id    = response.get("orderID", "unknown")
                last_status = response.get("status", "UNKNOWN").upper()

                if last_status in ("FILLED", "MATCHED", "MACHED"):
                    logger.info(
                        f"[EXEC] ✓ {direction} order {order_id} | "
                        f"{last_status} (attempt {attempt}/{BET_MAX_RETRIES}) | "
                        f"${amount_usdc:.2f} @ {odds:.3f}"
                    )
                    break   # sukses, hentikan retry

                # Order ditolak tapi tidak error (mis. slippage, no liquidity)
                last_error = f"status={last_status}"
                logger.warning(
                    f"[EXEC] Attempt {attempt}/{BET_MAX_RETRIES}: "
                    f"order {last_status} | "
                    f"{'retrying...' if attempt < BET_MAX_RETRIES else 'giving up'}"
                )
                if attempt < BET_MAX_RETRIES:
                    time.sleep(BET_RETRY_DELAY)

            except (
                _requests.exceptions.RequestException,
                ConnectionError,
                TimeoutError,
            ) as net_err:
                # Network / connection / timeout errors — retry
                last_error = str(net_err)
                logger.warning(
                    f"[EXEC] Retry {attempt}/{BET_MAX_RETRIES} setelah network error: {net_err} | "
                    f"{'retrying...' if attempt < BET_MAX_RETRIES else 'giving up'}"
                )
                if attempt < BET_MAX_RETRIES:
                    time.sleep(BET_RETRY_DELAY)
                else:
                    raise RuntimeError(
                        f"[EXEC] Semua {BET_MAX_RETRIES} retry gagal karena network error: {net_err}"
                    ) from net_err

            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"[EXEC] Attempt {attempt}/{BET_MAX_RETRIES} error: {e} | "
                    f"{'retrying...' if attempt < BET_MAX_RETRIES else 'giving up'}"
                )
                if attempt < BET_MAX_RETRIES:
                    time.sleep(BET_RETRY_DELAY)

        return BetResult(
            timestamp   = timestamp,
            market_id   = market.condition_id,
            question    = market.question,
            direction   = direction,
            token_id    = token_id,
            amount_usdc = amount_usdc,
            odds        = odds,
            order_id    = order_id,
            status      = last_status,
            dry_run     = False,
            error       = last_error,
        )

    def close_position(
        self,
        token_id: str,
        shares: float,
        condition_id: str = "",
        direction: str = "CLOSE",
    ) -> BetResult | None:
        """
        Tutup posisi yang sedang berjalan dengan menjual token ke pasar.

        Args:
            token_id     : token YES atau NO yang dipegang
            shares       : jumlah shares yang dimiliki (≈ amount_usdc / odds)
            condition_id : ID market untuk dedup guard
            direction    : "UP" atau "DOWN" (untuk logging)

        Returns:
            BetResult dengan status FILLED/FAILED, atau None jika dedup skip.
        """
        now       = time.time()
        close_key = f"CLOSE-{token_id[:20]}"

        # DRY RUN — simulasi saja
        if self.dry_run:
            fake_id = f"DRY-CLOSE-{int(now)}"
            logger.info(f"[EXEC] [DRY] Close position {direction} | {shares:.4f} shares")
            result = BetResult(
                timestamp   = now,
                market_id   = condition_id,
                question    = f"CLOSE {direction}",
                direction   = f"CLOSE_{direction}",
                token_id    = token_id,
                amount_usdc = shares,
                odds        = 0.0,
                order_id    = fake_id,
                status      = "DRY_RUN",
                dry_run     = True,
            )
            self.bet_log.write(result)
            return result

        # Dedup guard untuk close
        if close_key in self._executing:
            logger.warning("[EXEC] Close already in progress, skip")
            return None
        self._executing.add(close_key)

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType

            last_status = "FAILED"
            last_error  = ""
            order_id    = "FAILED"

            for attempt in range(1, BET_MAX_RETRIES + 1):
                try:
                    order_args   = MarketOrderArgs(token_id=token_id, amount=shares, side="SELL")
                    signed_order = self._client.create_market_order(order_args)
                    response     = self._client.post_order(signed_order, OrderType.FOK)

                    order_id    = response.get("orderID", "unknown")
                    last_status = response.get("status", "UNKNOWN").upper()

                    if last_status in ("FILLED", "MATCHED", "MACHED"):
                        logger.info(f"[EXEC] ✓ Close {direction} | {order_id} | {last_status}")
                        break

                    last_error = f"status={last_status}"
                    logger.warning(f"[EXEC] Close attempt {attempt}: {last_status}")
                    if attempt < BET_MAX_RETRIES:
                        time.sleep(BET_RETRY_DELAY)

                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"[EXEC] Close attempt {attempt} error: {e}")
                    if attempt < BET_MAX_RETRIES:
                        time.sleep(BET_RETRY_DELAY)

            result = BetResult(
                timestamp   = now,
                market_id   = condition_id,
                question    = f"CLOSE {direction}",
                direction   = f"CLOSE_{direction}",
                token_id    = token_id,
                amount_usdc = shares,
                odds        = 0.0,
                order_id    = order_id,
                status      = last_status,
                dry_run     = False,
                error       = last_error,
            )
            self.bet_log.write(result)
            return result

        finally:
            self._executing.discard(close_key)

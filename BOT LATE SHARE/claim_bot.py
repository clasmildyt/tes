"""
claim_bot.py
============
Standalone auto-claim bot untuk Polymarket BTC 5-min positions.

Cara pakai:
  python claim_bot.py

Bot ini TERPISAH dari bot_late.py — bisa jalan bersamaan di screen/tmux berbeda.
Tidak perlu ada bot aktif agar claim berjalan.

Alur:
  1. Cek posisi yang redeemable (menang & sudah resolved) via Data API
  2. Untuk tiap posisi → encode calldata → minta relay payload → sign → submit
  3. Ulangi setiap CHECK_INTERVAL detik
  4. Tampilkan log di terminal (cocok untuk VPS screen session)

Konfigurasi dari .env:
  POLYMARKET_PRIVATE_KEY   — EOA private key (yang dipakai untuk signing)
  POLYMARKET_FUNDER        — ProxyWallet address (tempat token disimpan)
  RELAYER_API_KEY          — dari polymarket.com/settings > Relayer API Keys
  RELAYER_API_KEY_ADDRESS  — address relayer API key
  AUTO_CLAIM_INTERVAL_MINUTES — interval cek (default: 10 menit)
  AUTO_REDEEM_ENABLED      — "true"/"false" (default: true)
"""

import logging
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

# ── Config ──────────────────────────────────────────────────
CHECK_INTERVAL = int(os.getenv("AUTO_CLAIM_INTERVAL_MINUTES", "10")) * 60
AUTO_REDEEM    = os.getenv("AUTO_REDEEM_ENABLED", "true").lower() == "true"

# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/claim_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Warna terminal sederhana ─────────────────────────────────
def green(s):  return f"\033[92m{s}\033[0m"
def red(s):    return f"\033[91m{s}\033[0m"
def yellow(s): return f"\033[93m{s}\033[0m"
def cyan(s):   return f"\033[96m{s}\033[0m"
def bold(s):   return f"\033[1m{s}\033[0m"
def dim(s):    return f"\033[2m{s}\033[0m"


def banner():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print(bold("+" + "-"*54 + "+"))
    print(bold("|") + f"  POLYMARKET AUTO-CLAIM BOT".center(54) + bold("|"))
    print(bold("|") + f"  {ts}".center(54) + bold("|"))
    print(bold("+" + "-"*54 + "+"))
    print(f"  Interval : {CHECK_INTERVAL//60} menit")
    print(f"  Enabled  : {green('YES') if AUTO_REDEEM else red('NO')}")
    print(bold("+" + "-"*54 + "+"))
    print()


def print_claim_result(pos: dict, success: bool, attempt: int = 1):
    title   = pos.get("title", pos.get("conditionId", "?"))[:50]
    size    = float(pos.get("size", 0))
    cur_px  = float(pos.get("curPrice", 1))
    pnl_est = size * cur_px

    status  = green("  [CLAIMED]") if success else red("  [FAILED] ")
    print(f"{status} {title}")
    print(f"           size={size:.4f}  curPrice={cur_px:.2f}  est.USDC~${pnl_est:.2f}")
    if not success:
        print(f"           {yellow('→ akan retry di siklus berikutnya')}")


# Set global untuk track posisi yang sudah berhasil di-claim
# (supaya tidak re-claim selama Polymarket UI belum sync)
_claimed_this_session: set = set()


def run_claim_cycle(executor) -> tuple[int, int]:
    """
    Satu siklus claim:
      1. Fetch redeemable positions
      2. Skip yang sudah di-claim sesi ini
      3. Claim tiap posisi satu per satu
      4. Return (claimed, total)
    """
    global _claimed_this_session
    logger.info("[CLAIM] Mengecek posisi redeemable...")

    positions = executor.get_redeemable_positions()
    if not positions:
        logger.info("[CLAIM] Tidak ada posisi yang perlu di-claim.")
        return 0, 0

    # Filter: skip yang sudah di-claim sesi ini
    new_positions = [p for p in positions if p.get("conditionId", "") not in _claimed_this_session]
    skipped = len(positions) - len(new_positions)

    if skipped > 0:
        logger.info(f"[CLAIM] {skipped} posisi di-skip (sudah di-claim, tunggu UI Polymarket sync)")

    if not new_positions:
        logger.info("[CLAIM] Semua posisi sudah di-claim sesi ini.")
        return 0, 0

    logger.info(f"[CLAIM] Ditemukan {len(new_positions)} posisi baru untuk di-claim.")
    claimed = 0

    for pos in new_positions:
        cond_id = pos.get("conditionId", "")
        title   = pos.get("title", cond_id[:20])
        if not cond_id:
            logger.warning(f"[CLAIM] Skip — conditionId kosong: {pos}")
            continue

        logger.info(f"[CLAIM] Claiming: {title[:50]} | condId={cond_id[:16]}...")
        ok = executor.claim_position(cond_id)
        print_claim_result(pos, ok)
        if ok:
            claimed += 1
            _claimed_this_session.add(cond_id)   # tandai sudah di-claim
            logger.info(f"[CLAIM] condId {cond_id[:16]}... ditandai sudah di-claim")
        # Jeda kecil antar posisi
        time.sleep(2)

    return claimed, len(new_positions)


def main():
    import os
    os.makedirs("logs", exist_ok=True)

    banner()

    if not AUTO_REDEEM:
        print(red("[CLAIM] AUTO_REDEEM_ENABLED=false — bot tidak aktif."))
        print(yellow("        Set AUTO_REDEEM_ENABLED=true di .env untuk mengaktifkan."))
        sys.exit(0)

    # Import executor
    try:
        from executor.polymarket import PolymarketExecutor
    except ImportError as e:
        print(red(f"[CLAIM] Import error: {e}"))
        print(yellow("        Jalankan dari folder polymarket-bot/"))
        sys.exit(1)

    executor = PolymarketExecutor(dry_run=False)

    # Cek relayer tersedia
    if not executor._relayer or not executor._relayer.is_available():
        print(red("[CLAIM] Relayer tidak aktif!"))
        print(yellow("        Cek RELAYER_API_KEY dan RELAYER_API_KEY_ADDRESS di .env"))
        sys.exit(1)

    print(green("[CLAIM] Relayer aktif — bot siap berjalan"))
    print(dim(f"        EOA      : {executor._relayer._account.address}"))
    print(dim(f"        Proxy    : {executor._relayer._funder}"))
    print(dim(f"        API Key  : {os.getenv('RELAYER_API_KEY','?')[:8]}..."))
    print()

    cycle_num   = 0
    total_claimed_all = 0

    while True:
        cycle_num += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(bold(f"\n[{ts}] === Siklus #{cycle_num} ==="))

        try:
            claimed, total = run_claim_cycle(executor)
            total_claimed_all += claimed

            if total == 0:
                print(dim(f"  Tidak ada yang perlu di-claim."))
            else:
                color = green if claimed > 0 else yellow
                print(color(f"  Hasil: {claimed}/{total} berhasil di-claim"))

            print(dim(f"  Total sepanjang sesi: {total_claimed_all} klaim"))

        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"[CLAIM] Cycle error: {e}", exc_info=True)
            print(red(f"  ERROR: {e}"))

        # Hitung waktu tunggu berikutnya
        next_ts = datetime.fromtimestamp(time.time() + CHECK_INTERVAL).strftime("%H:%M:%S")
        print(dim(f"\n  Cek berikutnya: {next_ts} (dalam {CHECK_INTERVAL//60} menit)"))
        print(dim("  (Tekan Ctrl+C untuk berhenti)"))

        try:
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            print(yellow("\n\n[CLAIM] Bot dihentikan (Ctrl+C). Sampai jumpa!"))
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(yellow("\n\n[CLAIM] Bot dihentikan. Sampai jumpa!"))

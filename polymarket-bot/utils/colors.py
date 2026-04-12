"""
utils/colors.py
===============
ANSI color helper untuk terminal dashboard.
Aktifkan ANSI support di Windows secara otomatis.
"""

import os
import sys

# Aktifkan ANSI escape codes di Windows
if sys.platform == "win32":
    os.system("")  # trigger Windows ANSI mode


class C:
    """ANSI color codes."""
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"

    # Foreground
    BLACK   = "\033[30m"
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    BLUE    = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN    = "\033[36m"
    WHITE   = "\033[37m"

    # Bright foreground
    BRED    = "\033[91m"
    BGREEN  = "\033[92m"
    BYELLOW = "\033[93m"
    BBLUE   = "\033[94m"
    BMAGENTA= "\033[95m"
    BCYAN   = "\033[96m"
    BWHITE  = "\033[97m"

    # Background
    BG_RED    = "\033[41m"
    BG_GREEN  = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE   = "\033[44m"
    BG_CYAN   = "\033[46m"


def green(s):  return f"{C.BGREEN}{s}{C.RESET}"
def red(s):    return f"{C.BRED}{s}{C.RESET}"
def yellow(s): return f"{C.BYELLOW}{s}{C.RESET}"
def cyan(s):   return f"{C.BCYAN}{s}{C.RESET}"
def magenta(s):return f"{C.BMAGENTA}{s}{C.RESET}"
def white(s):  return f"{C.BWHITE}{s}{C.RESET}"
def dim(s):    return f"{C.DIM}{s}{C.RESET}"
def bold(s):   return f"{C.BOLD}{s}{C.RESET}"


def signed_color(val: float, s: str) -> str:
    """Hijau jika positif, merah jika negatif, putih jika nol."""
    if val > 0:   return green(s)
    elif val < 0: return red(s)
    else:         return white(s)


def ok_or_err(condition: bool, ok_text: str, err_text: str) -> str:
    """Hijau jika True, merah jika False."""
    return green(ok_text) if condition else red(err_text)

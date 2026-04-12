"""
generate_api_creds.py
=====================
Generate CLOB API credentials dari private key wallet kamu.
Credentials ini BERBEDA dari Builder Codes di website Polymarket.

Jalankan SEKALI: python generate_api_creds.py
Lalu copy hasilnya ke .env
"""

import os
from dotenv import load_dotenv
load_dotenv()

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
CLOB_HOST   = "https://clob.polymarket.com"
POLY_CHAIN  = 137  # Polygon mainnet

if not PRIVATE_KEY:
    print("❌ POLYMARKET_PRIVATE_KEY tidak ada di .env!")
    exit(1)

print(f"\n{'='*60}")
print(f"  Generate Polymarket CLOB API Credentials")
print(f"{'='*60}")
print(f"  Private key : {PRIVATE_KEY[:10]}...{PRIVATE_KEY[-4:]}")
print(f"  Host        : {CLOB_HOST}")
print()

try:
    from py_clob_client.client import ClobClient

    # Init client hanya dengan private key (Level 1 auth)
    client = ClobClient(
        host     = CLOB_HOST,
        chain_id = POLY_CHAIN,
        key      = PRIVATE_KEY,
    )

    # Coba derive dulu (key sudah ada di server, recover dari private key)
    # Kalau belum ada, baru create
    creds = None
    for nonce in range(3):
        print(f"  Mencoba derive key (nonce={nonce})...")
        try:
            creds = client.derive_api_key(nonce=nonce)
            if creds and creds.api_key:
                print(f"  ✓ Key ditemukan di nonce={nonce}")
                break
        except Exception as e:
            print(f"  derive nonce={nonce} gagal: {e}")

    if creds is None or not creds.api_key:
        print("  Semua derive gagal, mencoba create baru (nonce=1)...")
        try:
            creds = client.create_api_key(nonce=1)
        except Exception as e:
            print(f"❌ Create juga gagal: {e}")
            exit(1)

    if creds is None:
        print("❌ Tidak bisa mendapatkan API credentials.")
        exit(1)

    print(f"\n{'='*60}")
    print(f"  ✅ BERHASIL! Copy ke .env kamu:")
    print(f"{'='*60}")
    print(f"\n  POLYMARKET_API_KEY={creds.api_key}")
    print(f"  POLYMARKET_API_SECRET={creds.api_secret}")
    print(f"  POLYMARKET_API_PASSPHRASE={creds.api_passphrase}")
    print(f"\n{'='*60}")
    print(f"  ⚠  Simpan credentials ini — tidak bisa ditampilkan lagi!")
    print(f"{'='*60}\n")

    # Tawaran update .env otomatis
    ans = input("  Update .env otomatis sekarang? (y/n): ").strip().lower()
    if ans == "y":
        env_path = ".env"
        with open(env_path, "r") as f:
            content = f.read()

        import re
        content = re.sub(r"^POLYMARKET_API_KEY=.*$",      f"POLYMARKET_API_KEY={creds.api_key}",          content, flags=re.MULTILINE)
        content = re.sub(r"^POLYMARKET_API_SECRET=.*$",   f"POLYMARKET_API_SECRET={creds.api_secret}",    content, flags=re.MULTILINE)
        content = re.sub(r"^POLYMARKET_API_PASSPHRASE=.*$", f"POLYMARKET_API_PASSPHRASE={creds.api_passphrase}", content, flags=re.MULTILINE)

        with open(env_path, "w") as f:
            f.write(content)

        print("\n  ✅ .env sudah diupdate! Restart bot sekarang.\n")
    else:
        print("\n  Copy manual ke .env lalu restart bot.\n")

except ImportError:
    print("❌ py-clob-client tidak terinstall. Jalankan: pip install py-clob-client")
except Exception as e:
    print(f"❌ Error: {e}")

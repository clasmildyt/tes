"""
executor/claim_relayer.py
=========================
Gas-free auto-claim via Polymarket Relayer v2 API.
Menggunakan RELAYER_API_KEY (dari polymarket.com/settings > Relayer API Keys).

Auth: 2 header saja:
  RELAYER_API_KEY: <uuid-key>
  RELAYER_API_KEY_ADDRESS: <eth-address>

Referensi:
  https://github.com/Polymarket/builder-relayer-client/blob/main/src/builder/proxy.ts
"""

import logging
import os
import time

import requests
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_hash.auto import keccak
from web3 import Web3

load_dotenv()

logger = logging.getLogger(__name__)

# ── Contract Addresses (Polygon Mainnet) ───────────────────
PROXY_FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
RELAY_HUB     = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
CTF_ADDRESS   = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS  = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RELAYER_HOST  = "https://relayer-v2.polymarket.com"
DEFAULT_GAS   = 300_000     # fallback gas limit (browser Polymarket pakai ~139338)

# ── Credentials ────────────────────────────────────────────
RELAYER_API_KEY     = os.getenv("RELAYER_API_KEY", "")
RELAYER_API_ADDRESS = os.getenv("RELAYER_API_KEY_ADDRESS", "")
PRIVATE_KEY         = os.getenv("POLYMARKET_PRIVATE_KEY", "")
FUNDER              = os.getenv("POLYMARKET_FUNDER", "")

# ── ABI ────────────────────────────────────────────────────
# RelayHub ABI — hanya fungsi getNonce untuk baca nonce on-chain
RELAY_HUB_ABI = [
    {
        "inputs": [{"name": "from", "type": "address"}],
        "name": "getNonce",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken",    "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSets",          "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ProxyFactory ABI — fungsi proxy(ProxyTransaction[] txns)
# ProxyTransaction: { typeCode, to, value, data }
PROXY_FACTORY_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "typeCode", "type": "uint8"},
                    {"name": "to",       "type": "address"},
                    {"name": "value",    "type": "uint256"},
                    {"name": "data",     "type": "bytes"},
                ],
                "name": "txns",
                "type": "tuple[]",
            }
        ],
        "name": "proxy",
        "outputs": [{"name": "", "type": "bytes[]"}],
        "stateMutability": "payable",
        "type": "function",
    }
]

POLYGON_RPCS = [
    os.getenv("RPC_URL", ""),
    "https://polygon-bor-rpc.publicnode.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
]


class ClaimRelayer:
    """Gas-free claim via Polymarket Relayer v2 (PROXY wallet)."""

    def __init__(self):
        self.enabled      = False
        self._w3          = None
        self._account     = None
        self._ctf         = None
        self._proxy_fac   = None
        self._session     = requests.Session()

        if not (RELAYER_API_KEY and RELAYER_API_ADDRESS and PRIVATE_KEY and FUNDER):
            logger.info("[RELAYER] RELAYER_API_KEY tidak ada — relayer dinonaktifkan")
            return

        try:
            for rpc in POLYGON_RPCS:
                if not rpc:
                    continue
                try:
                    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
                    if w3.is_connected():
                        self._w3 = w3
                        break
                except Exception:
                    continue

            if not self._w3:
                raise Exception("Tidak bisa connect ke Polygon RPC")

            self._account    = Account.from_key(PRIVATE_KEY)
            self._funder     = Web3.to_checksum_address(FUNDER)
            self._ctf        = self._w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS),
                abi=CTF_ABI,
            )
            self._proxy_fac  = self._w3.eth.contract(
                address=Web3.to_checksum_address(PROXY_FACTORY),
                abi=PROXY_FACTORY_ABI,
            )
            self._relay_hub  = self._w3.eth.contract(
                address=Web3.to_checksum_address(RELAY_HUB),
                abi=RELAY_HUB_ABI,
            )

            self.enabled = True
            logger.info(
                f"[RELAYER] ✓ Initialized | "
                f"EOA={self._account.address[:10]}... | "
                f"Proxy={self._funder[:10]}..."
            )

        except Exception as e:
            logger.error(f"[RELAYER] Init error: {e}")
            self.enabled = False

    # ── Auth Headers ─────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "RELAYER_API_KEY":         RELAYER_API_KEY,
            "RELAYER_API_KEY_ADDRESS": RELAYER_API_ADDRESS,
            "Content-Type":            "application/json",
        }

    # ── Relay Payload ─────────────────────────────────────────

    def _get_relay_payload(self) -> tuple[str, str, int]:
        """
        GET /relay-payload?address={EOA}&type=PROXY
        Returns (relay_address, nonce_string, gas_limit_int)
        gas_limit diambil dari API jika ada, fallback ke DEFAULT_GAS.
        """
        resp = self._session.get(
            f"{RELAYER_HOST}/relay-payload",
            params={"address": self._account.address, "type": "PROXY"},
            headers=self._headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data      = resp.json()
        logger.info(f"[RELAYER] relay-payload response: {data}")
        relay     = data.get("address", RELAY_HUB)
        gas_limit = int(data.get("gasLimit", data.get("gas_limit", DEFAULT_GAS)))

        # Baca nonce ON-CHAIN langsung dari RelayHub contract
        # Lebih reliable daripada API yang bisa out-of-sync
        try:
            onchain_nonce = self._relay_hub.functions.getNonce(self._account.address).call()
            nonce = str(onchain_nonce)
            logger.info(f"[RELAYER] on-chain nonce={nonce} | API nonce={data.get('nonce','?')} | relay={relay[:10]}... | gasLimit={gas_limit}")
        except Exception as e:
            # Fallback ke API nonce jika RPC gagal
            nonce = str(data.get("nonce", "0"))
            logger.warning(f"[RELAYER] Gagal baca on-chain nonce ({e}), pakai API nonce={nonce}")

        return relay, nonce, gas_limit

    # ── Calldata ─────────────────────────────────────────────

    def _redeem_calldata(self, condition_id: str) -> str:
        """Encode redeemPositions calldata (0x-prefixed hex)."""
        cond_bytes  = bytes.fromhex(condition_id.replace("0x", "").zfill(64))
        parent_coll = b"\x00" * 32
        return self._ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS),
            parent_coll,
            cond_bytes,
            [1, 2],
        )._encode_transaction_data()

    def _proxy_calldata(self, inner_calldata: str) -> str:
        """
        Encode proxy([(typeCode=1, to=CTF, value=0, data=innerCalldata)])
        pada ProxyFactory contract.
        """
        return self._proxy_fac.functions.proxy([
            (
                1,  # typeCode = CALL
                Web3.to_checksum_address(CTF_ADDRESS),
                0,  # value
                bytes.fromhex(inner_calldata.replace("0x", "")),
            )
        ])._encode_transaction_data()

    # ── Signature ────────────────────────────────────────────

    def _create_struct_hash(
        self,
        encoded_data: str,
        nonce: str,
        relay: str,
        gas_limit: int,
    ) -> bytes:
        """
        keccak256(
          "rlx:" + from(EOA) + to(ProxyFactory) + data + txFee(0)
          + gasPrice(0) + gasLimit + nonce + relayHub + relay
        )
        Ref: builder-relayer-client/src/builder/proxy.ts :: createStructHash()
        """
        def pad32(addr: str) -> bytes:
            return bytes.fromhex(addr.replace("0x", "").lower())

        data_bytes = bytes.fromhex(encoded_data.replace("0x", ""))

        message = (
            b"rlx:"
            + pad32(self._account.address)   # from
            + pad32(PROXY_FACTORY)           # to (ProxyFactory, NOT CTF)
            + data_bytes                     # encoded proxy() calldata
            + (0).to_bytes(32, "big")        # txFee = 0
            + (0).to_bytes(32, "big")        # gasPrice = 0
            + gas_limit.to_bytes(32, "big")  # gasLimit
            + int(nonce).to_bytes(32, "big") # nonce
            + pad32(RELAY_HUB)               # relayHub
            + pad32(relay)                   # relay address
        )
        return keccak(message)

    def _sign(self, struct_hash: bytes) -> str:
        """Sign struct hash menggunakan personal_sign (eth_sign)."""
        msg    = encode_defunct(struct_hash)
        signed = self._account.sign_message(msg)
        return "0x" + signed.signature.hex()

    # ── Main Claim ───────────────────────────────────────────

    def claim(self, condition_id: str, max_retries: int = 3) -> bool:
        """
        Klaim posisi menang via Polymarket Relayer v2 (gas-free).
        Returns True jika berhasil submit.
        """
        if not self.enabled:
            return False

        for attempt in range(1, max_retries + 1):
            try:
                # 1. Get relay payload
                relay, nonce, gas_limit = self._get_relay_payload()

                # 2. Build calldata
                redeem_cd = self._redeem_calldata(condition_id)
                proxy_cd  = self._proxy_calldata(redeem_cd)

                # 3. Build struct hash & sign
                struct_hash = self._create_struct_hash(proxy_cd, nonce, relay, gas_limit)
                signature   = self._sign(struct_hash)

                # 4. Submit
                body = {
                    "type":        "PROXY",
                    "from":        self._account.address,
                    "to":          PROXY_FACTORY,        # ProxyFactory, bukan CTF!
                    "proxyWallet": self._funder,
                    "data":        proxy_cd,
                    "nonce":       nonce,
                    "signature":   signature,
                    "signatureParams": {
                        "gasPrice":   "0",
                        "gasLimit":   str(gas_limit),
                        "relayerFee": "0",
                        "relayHub":   RELAY_HUB,
                        "relay":      relay,
                    },
                    "metadata": "",
                }

                # DEBUG: log payload lengkap
                import json as _json
                logger.info(
                    f"[RELAYER] Submitting payload:\n"
                    f"  from        : {body['from']}\n"
                    f"  to          : {body['to']}\n"
                    f"  proxyWallet : {body['proxyWallet']}\n"
                    f"  nonce       : {body['nonce']}\n"
                    f"  gasLimit    : {body['signatureParams']['gasLimit']}\n"
                    f"  relay       : {body['signatureParams']['relay']}\n"
                    f"  data[:20]   : {body['data'][:22]}...\n"
                    f"  sig[:20]    : {body['signature'][:22]}..."
                )

                resp = self._session.post(
                    f"{RELAYER_HOST}/submit",
                    headers=self._headers(),
                    json=body,
                    timeout=30,
                )

                logger.info(f"[RELAYER] Response HTTP {resp.status_code}: {resp.text[:300]}")

                if resp.status_code == 200:
                    result = resp.json()
                    tx_id  = (
                        result.get("transactionID")
                        or result.get("transactionHash")
                        or str(result)
                    )
                    logger.info(
                        f"[RELAYER] ✓ Claim submitted! "
                        f"txID={str(tx_id)} | "   # full txID, tidak dipotong
                        f"cond={condition_id[:16]}..."
                    )
                    return True

                elif resp.status_code == 401:
                    logger.error("[RELAYER] ✗ Unauthorized — cek RELAYER_API_KEY di .env")
                    self.enabled = False
                    return False

                elif resp.status_code == 429:
                    logger.warning(f"[RELAYER] Rate limit (429) — tunggu 60s... ({resp.text[:100]})")
                    time.sleep(60)

                else:
                    logger.warning(
                        f"[RELAYER] Attempt {attempt}/{max_retries}: "
                        f"HTTP {resp.status_code} — {resp.text[:300]}"
                    )

            except Exception as e:
                logger.warning(f"[RELAYER] Attempt {attempt}/{max_retries} error: {e}")

            if attempt < max_retries:
                time.sleep(3.0 * attempt)

        logger.warning(f"[RELAYER] ✗ Claim gagal: {condition_id[:16]}...")
        return False

    def is_available(self) -> bool:
        return self.enabled

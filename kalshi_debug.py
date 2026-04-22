"""
kalshi_debug.py
Diagnostic script to identify the correct Kalshi API v2 auth format.
Run: python kalshi_debug.py

Prints the full HTTP response body for each auth variant so you can see
exactly what Kalshi is accepting or rejecting.
"""

import os, sys, base64, time, json
import requests
sys.path.insert(0, os.path.dirname(__file__))

BASE_DIR    = os.path.dirname(__file__)
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
TEST_URL    = f"{KALSHI_BASE}/markets"
TEST_PARAMS = {"limit": 5, "status": "open"}


# ── Load .env ──────────────────────────────────────────────────────────────────
def load_env():
    vals = {}
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    vals[k.strip()] = v.strip().strip('"').strip("'")
    return vals

env     = load_env()
api_key = env.get("KALSHI_API_KEY", "")
key_path_raw = env.get("KALSHI_PRIVATE_KEY_PATH", "")
if not os.path.isabs(key_path_raw):
    key_path_raw = os.path.join(BASE_DIR, key_path_raw)

print("=" * 60)
print(f"API Key ID : {api_key[:8]}..." if api_key else "API Key ID : NOT FOUND")
print(f"PEM path   : {key_path_raw}")
print(f"PEM exists : {os.path.exists(key_path_raw)}")
print("=" * 60)


# ── Load and inspect private key ───────────────────────────────────────────────
pem_bytes = None
if os.path.exists(key_path_raw):
    with open(key_path_raw, "rb") as f:
        pem_bytes = f.read()
    pem_text = pem_bytes.decode("utf-8", errors="replace")
    first_line = pem_text.strip().splitlines()[0] if pem_text.strip() else ""
    print(f"PEM header : {first_line}")
else:
    print("ERROR: PEM file not found — check KALSHI_PRIVATE_KEY_PATH in .env")
    sys.exit(1)

# Try loading private key with multiple strategies
private_key = None
key_type    = "unknown"

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding, ec

for strategy, attempt_bytes in [
    ("original PEM",       pem_bytes),
    ("PRIVATE KEY header", (
        "-----BEGIN PRIVATE KEY-----\n" +
        "\n".join(l for l in pem_bytes.decode().splitlines() if not l.startswith("-----")) +
        "\n-----END PRIVATE KEY-----\n"
    ).encode()),
]:
    try:
        pk = serialization.load_pem_private_key(attempt_bytes, password=None, backend=default_backend())
        private_key = pk
        key_type    = type(pk).__name__
        print(f"Key loaded : strategy='{strategy}'  type={key_type}")
        break
    except Exception as e:
        print(f"Load fail  : strategy='{strategy}'  err={e}")

if private_key is None:
    print("ERROR: could not load private key with any strategy")
    sys.exit(1)

print("=" * 60)


# ── Sign helper ────────────────────────────────────────────────────────────────
def rsa_sign(pk, message: str) -> str:
    sig = pk.sign(message.encode("utf-8"), asym_padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(sig).decode()

def ec_sign(pk, message: str) -> str:
    sig = pk.sign(message.encode("utf-8"), ec.ECDSA(hashes.SHA256()))
    return base64.b64encode(sig).decode()

def sign(pk, message: str) -> str:
    try:
        return rsa_sign(pk, message)
    except Exception:
        return ec_sign(pk, message)

def try_request(label: str, headers: dict):
    try:
        r = requests.get(TEST_URL, params=TEST_PARAMS, headers=headers, timeout=10)
        body = r.text[:300]
        print(f"\n[{label}]")
        print(f"  Status : {r.status_code}")
        print(f"  Body   : {body}")
        return r.status_code == 200
    except Exception as e:
        print(f"\n[{label}] ERROR: {e}")
        return False


# ── Test variant 1: path only (no query string), ms timestamp ─────────────────
ts = str(int(time.time() * 1000))
path_only = "/trade-api/v2/markets"
msg = ts + "GET" + path_only
sig = sign(private_key, msg)
try_request("RSA / path-only / ms", {
    "KALSHI-ACCESS-KEY":       api_key,
    "KALSHI-ACCESS-TIMESTAMP": ts,
    "KALSHI-ACCESS-SIGNATURE": sig,
    "Content-Type":            "application/json",
})

# ── Test variant 2: path + query string, ms timestamp ─────────────────────────
ts = str(int(time.time() * 1000))
path_with_qs = "/trade-api/v2/markets?limit=5&status=open"
msg = ts + "GET" + path_with_qs
sig = sign(private_key, msg)
try_request("RSA / path+qs / ms", {
    "KALSHI-ACCESS-KEY":       api_key,
    "KALSHI-ACCESS-TIMESTAMP": ts,
    "KALSHI-ACCESS-SIGNATURE": sig,
    "Content-Type":            "application/json",
})

# ── Test variant 3: path only, SECONDS timestamp ──────────────────────────────
ts_s = str(int(time.time()))
msg = ts_s + "GET" + path_only
sig = sign(private_key, msg)
try_request("RSA / path-only / SECONDS", {
    "KALSHI-ACCESS-KEY":       api_key,
    "KALSHI-ACCESS-TIMESTAMP": ts_s,
    "KALSHI-ACCESS-SIGNATURE": sig,
    "Content-Type":            "application/json",
})

# ── Test variant 4: simple Authorization header (legacy) ──────────────────────
try_request("Simple Authorization header", {
    "Authorization": api_key,
    "Content-Type":  "application/json",
})

# ── Test variant 5: Bearer token ──────────────────────────────────────────────
try_request("Bearer token", {
    "Authorization": f"Bearer {api_key}",
    "Content-Type":  "application/json",
})

print("\n" + "=" * 60)
print("Done. Share the output above so we can identify the right format.")

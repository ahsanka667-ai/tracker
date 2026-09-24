"""
shared/security.py — hashing, signing, secret storage.

1. Password hashing for username/password dashboard login (stdlib PBKDF2).
2. Signed, expiring, single-use **correlation tokens** (§6/§12): the opaque
   string that travels inside `t.me/bot?start=<token>` or a DM-bridge URL.
   Never a database id, never PII, HMAC-SHA256 over a canonical payload.
3. Meta's required one-way hashing for user_data fields (SHA-256 of the
   normalized value).
4. At-rest encryption for per-account secrets (Meta CAPI access token, bot
   token, MTProto session path) when ENCRYPTION_KEY is configured.
5. `redact()` — used by the logging filter so a token can never reach a log
   file or an API response.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Any

_ITERATIONS = 260_000
_ALGO = "sha256"

# ── passwords ─────────────────────────────────────────────────────────


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(_ALGO, password.encode("utf-8"), bytes.fromhex(salt), _ITERATIONS)
    return f"{salt}${dk.hex()}"


def verify_password(password: str, stored: str | None) -> bool:
    if not stored or "$" not in stored:
        return False
    salt_hex, hash_hex = stored.split("$", 1)
    try:
        salt = bytes.fromhex(salt_hex)
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac(_ALGO, password.encode("utf-8"), salt, _ITERATIONS)
    return hmac.compare_digest(dk.hex(), hash_hex)


# ── generic opaque identifiers ──────────────────────────────────────────


def random_id(nbytes: int = 12) -> str:
    """URL-safe opaque id (click ids, link codes, job ids)."""
    return secrets.token_urlsafe(nbytes).replace("-", "_")


def random_token(nbytes: int = 24) -> str:
    return secrets.token_urlsafe(nbytes)


def short_code(length: int = 8) -> str:
    alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O/1/l/I
    return "".join(secrets.choice(alphabet) for _ in range(length))


def stable_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


# ── signed correlation tokens ───────────────────────────────────────────
#
# Format:  base64url(json payload) + "." + base64url(hmac_sha256)
# The payload carries ONLY opaque references (click public id, destination
# kind, nonce, expiry). Telegram sees this string; it reveals nothing about
# the database and cannot be forged or edited without SECRET_KEY.

_TOK_RE = re.compile(r"^[A-Za-z0-9_\-]{8,512}\.[A-Za-z0-9_\-]{16,256}$")


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64d(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def _sign(secret: str, message: str) -> str:
    return _b64e(hmac.new(secret.encode(), message.encode(), hashlib.sha256).digest())


def sign_token(payload: dict[str, Any], secret: str, ttl_seconds: int) -> str:
    body = dict(payload)
    now = int(time.time())
    body.setdefault("iat", now)
    body["exp"] = now + int(ttl_seconds)
    body.setdefault("nonce", secrets.token_hex(8))
    encoded = _b64e(json.dumps(body, separators=(",", ":"), sort_keys=True).encode())
    return f"{encoded}.{_sign(secret, encoded)}"


class TokenError(ValueError):
    """Raised for a malformed, forged, expired or already-used token."""


def verify_token(token: str, secret: str, *, purpose: str | None = None,
                 max_age_seconds: int | None = None) -> dict[str, Any]:
    """
    Verify signature + expiry (and optionally the `purp` claim). Returns the
    decoded payload. Never trusts anything unsigned.
    """
    if not token or not _TOK_RE.match(token):
        raise TokenError("malformed_token")
    try:
        encoded, signature = token.rsplit(".", 1)
        expected = _sign(secret, encoded)
    except (ValueError, binascii.Error) as exc:  # pragma: no cover - defensive
        raise TokenError("malformed_token") from exc
    if not hmac.compare_digest(expected, signature):
        raise TokenError("bad_signature")
    try:
        payload = json.loads(_b64d(encoded).decode())
    except (ValueError, UnicodeDecodeError) as exc:  # pragma: no cover - defensive
        raise TokenError("malformed_payload") from exc
    if not isinstance(payload, dict):
        raise TokenError("malformed_payload")
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or exp < time.time():
        raise TokenError("expired")
    if max_age_seconds and payload.get("iat", 0) < time.time() - max_age_seconds:
        raise TokenError("expired")
    if purpose and payload.get("purp") != purpose:
        raise TokenError("wrong_purpose")
    return payload


def verify_webhook_secret(provided: str | None, expected: str) -> bool:
    """Constant-time compare for inbound webhook secrets (Telegram, partners)."""
    if not expected:
        return False
    return bool(provided) and hmac.compare_digest(provided, expected)


def is_signed_token(value: str) -> bool:
    return bool(value) and bool(_TOK_RE.match(value.strip()))


# ── Meta user_data hashing (spec §16) ───────────────────────────────────


def normalize_meta_value(field: str, value: Any) -> str:
    """Meta's normalization rules, applied before hashing."""
    if value is None:
        return ""
    s = str(value).strip().lower()
    if field in ("ph", "client_phone_number"):
        s = re.sub(r"\D", "", s)
        if s.startswith("00"):
            s = s[2:]
    elif field in ("em", "client_email_address"):
        s = s.strip()
        # gmail: dots in the local part are insignificant
        if s.endswith("@gmail.com"):
            local, dom = s.split("@", 1)
            s = f"{local.replace('.', '')}@{dom}"
    elif field in ("external_id",):
        s = s.strip()
    elif field in ("ge", "ln", "fn"):
        s = re.sub(r"\s+", " ", s).strip()
    elif field in ("st",):
        s = s[:2]
    elif field in ("ct",):
        s = re.sub(r"[^a-z ]", "", s).strip()
    elif field in ("zip", "zp"):
        s = re.sub(r"[^a-z0-9]", "", s)
    elif field in ("country",):
        s = re.sub(r"[^a-z]", "", s)
    return s


def meta_hash(field: str, value: Any) -> str:
    """SHA-256 of the normalized value, lowercase hex — what CAPI expects."""
    norm = normalize_meta_value(field, value)
    if not norm:
        return ""
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


# Aliases kept so call sites read naturally in both meta_capi.py and tests.
def hash_email(value: Any) -> str:
    return meta_hash("em", value)


def hash_phone(value: Any) -> str:
    return meta_hash("ph", value)


def hash_name(field: str, value: Any) -> str:
    return meta_hash(field, value)


# ── at-rest encryption of per-account secrets ───────────────────────────


def _aes_key(secret: str) -> bytes | None:
    if not secret:
        return None
    raw: bytes | None = None
    try:
        raw = base64.b64decode(secret, validate=True)
    except (binascii.Error, ValueError):
        raw = None
    if raw is None or len(raw) != 32:
        raw = hashlib.sha256(secret.encode()).digest()
    return raw


def encrypt_secret(plaintext: str | None, key: str) -> str | None:
    """
    AES-256-GCM when a key is configured, otherwise pass-through. Ciphertext
    is stored as `enc:v1:<base64url(nonce+ct+tag)>` so a later key rotation can
    still tell encrypted rows apart from legacy plaintext ones.
    """
    if plaintext is None or plaintext == "":
        return plaintext
    aes = _aes_key(key)
    if aes is None:
        return plaintext
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:  # pragma: no cover - dependency optional
        return plaintext
    if plaintext.startswith("enc:v1:"):
        return plaintext
    nonce = secrets.token_bytes(12)
    ct = AESGCM(aes).encrypt(nonce, plaintext.encode(), None)
    return "enc:v1:" + _b64e(nonce + ct)


def decrypt_secret(stored: str | None, key: str) -> str | None:
    if not stored or not stored.startswith("enc:v1:"):
        return stored  # legacy plaintext
    aes = _aes_key(key)
    if aes is None:
        return None
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        blob = _b64d(stored[len("enc:v1:"):])
        return AESGCM(aes).decrypt(blob[:12], blob[12:], None).decode()
    except Exception:
        return None


# ── redaction ─────────────────────────────────────────────────────────────

_SENSITIVE_KEYS = {
    "access_token", "capi_token", "meta_capi_token", "bot_token", "token",
    "session_string", "session", "password", "api_hash", "password_hash",
    "secret", "secret_key", "api_id", "private_key", "encryption_key",
    "test_event_code", "x-telegram-bot-api-secret-token",
}
_SECRET_PATTERNS = [
    # bot tokens look like 123456789:AAHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    re.compile(r"\b\d{8,12}:[A-Za-z0-9_\-]{30,}\b"),
    # CAPI tokens: EAA... long alnum blobs
    re.compile(r"\bEAA[A-Za-z0-9_\-]{20,}\b"),
    # bearer tokens in free text
    re.compile(r"(?i)\b(bearer|access_token=|api_hash=|Authorization:)\s*\S+"),
]


def mask_secret(value: str | None, keep: int = 4) -> str:
    """`EAAx…abcd` → `EAA…abcd` style mask, safe for display."""
    if not value:
        return ""
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}…{value[-keep:]} ({len(value)} chars)"


def redact(text: str) -> str:
    """Scrub credential-shaped substrings out of anything destined for logs."""
    if not text:
        return text
    out = text
    for pat in _SECRET_PATTERNS:
        out = pat.sub("[REDACTED]", out)
    return out


def redact_obj(obj: Any) -> Any:
    """Recursively replace values under sensitive keys (for log payloads)."""
    if isinstance(obj, dict):
        return {
            k: ("[REDACTED]" if str(k).lower() in _SENSITIVE_KEYS and v not in (None, "")
                else redact_obj(v))
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v) for v in obj]
    if isinstance(obj, str):
        return redact(obj)
    return obj

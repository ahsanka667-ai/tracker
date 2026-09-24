"""
shared/security.py — password hashing for username/password dashboard login.

Stdlib only (hashlib.pbkdf2_hmac) so no new dependency is needed just to
support a login form. PBKDF2-HMAC-SHA256 with a random per-user salt and
260k iterations (same order of magnitude as Django's default).

Storage format: "<salt_hex>$<derived_key_hex>" in DashboardUser.password_hash.
"""
import hashlib
import hmac
import secrets

_ITERATIONS = 260_000
_ALGO = "sha256"


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

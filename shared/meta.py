"""
shared/meta.py — Meta CAPI helpers (Meta-only, §15-§17)

- builds user_data / custom_data correctly
- reuses stored fbc/fbp verbatim (never regenerates)
- generates a single event_id and returns it for Pixel+CAPI dedup
- hashing helpers delegate to shared.security
"""
from __future__ import annotations

import hashlib
import time
import uuid
import json
from typing import Any


def build_user_data(
    *,
    telegram_id: int | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    username: str | None = None,
    phone: str | None = None,
    email: str | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
    fbc: str | None = None,
    fbp: str | None = None,
    country: str | None = None,
    city: str | None = None,
    zip_code: str | None = None,
    external_id: str | None = None,
) -> dict[str, Any]:
    """
    Build Meta user_data. Hashed fields are SHA-256 of normalized value.
    fbc/fbp/client_ip/user_agent are sent as-is per Meta spec.
    """
    from shared.security import meta_hash

    ud: dict[str, Any] = {}

    # external_id — strongest dedup signal we have
    if external_id:
        ud["external_id"] = meta_hash("external_id", external_id)
    elif telegram_id:
        ud["external_id"] = meta_hash("external_id", str(telegram_id))

    if first_name:
        h = meta_hash("fn", first_name)
        if h:
            ud["fn"] = h
    if last_name:
        h = meta_hash("ln", last_name)
        if h:
            ud["ln"] = h
    if phone:
        h = meta_hash("ph", phone)
        if h:
            ud["ph"] = h
    if email:
        h = meta_hash("em", email)
        if h:
            ud["em"] = h
    if country:
        h = meta_hash("country", country)
        if h:
            ud["country"] = h
    if city:
        h = meta_hash("ct", city)
        if h:
            ud["ct"] = h
    if zip_code:
        h = meta_hash("zip", zip_code)
        if h:
            ud["zp"] = h

    if client_ip:
        ud["client_ip_address"] = client_ip.strip()
    if user_agent:
        ud["client_user_agent"] = user_agent.strip()
    # CRITICAL: use stored fbc/fbp verbatim, do NOT regenerate
    if fbc:
        ud["fbc"] = fbc.strip()
    if fbp:
        ud["fbp"] = fbp.strip()

    return ud


def build_custom_data(
    *,
    value: float | None = None,
    currency: str | None = None,
    content_name: str | None = None,
    content_ids: list[str] | None = None,
    content_category: str | None = None,
    num_items: int | None = None,
    order_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cd: dict[str, Any] = {}
    if value is not None:
        cd["value"] = float(value)
    if currency:
        cd["currency"] = currency.upper().strip()
    if content_name:
        cd["content_name"] = content_name
    if content_ids:
        cd["content_ids"] = content_ids
        cd.setdefault("content_type", "product")
    if content_category:
        cd["content_category"] = content_category
    if num_items is not None:
        cd["num_items"] = int(num_items)
    if order_id:
        cd["order_id"] = str(order_id)
    if extra:
        cd.update(extra)
    return cd


def new_event_id() -> str:
    return str(uuid.uuid4())


def dedup_key(event_name: str, event_id: str, pixel_id: str) -> str:
    """Stable idempotency key for meta_events dedup."""
    raw = f"{pixel_id}:{event_name}:{event_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

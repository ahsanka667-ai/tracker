"""
shared/tracking.py — Click capture + link resolution

- Builds fbc/fbp correctly (once at click time, never regenerated)
- Parses Meta params (fbclid, fbc, fbp, campaign/adset/ad, subs)
- Captures request geography / device / browser where available
- Persists the Click row + Redis correlation cache
- Resolves a signed correlation token back to its click
"""
from __future__ import annotations

import re
import time
import uuid
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import urlparse, parse_qs, unquote

from sqlalchemy import select

from shared.config import get_settings
from shared.database import AsyncSessionLocal, bind_ts

# ── constants ─────────────────────────────────────────────────────────

TRACKING_PARAMS = {
    "fbclid", "fbc", "fbp",
    "campaign", "campaign_id", "adset", "adset_id", "ad", "ad_id",
    "creative", "placement",
    "sub1", "sub2", "sub3", "sub4", "sub5", "sub6", "sub7", "sub8", "sub9",
}

# Valid fbclid looks like random alnum + _ + - ; Meta's docs are loose, be permissive
_FBCLID_RE = re.compile(r"^[A-Za-z0-9_\-]{10,512}$")
_FBC_RE = re.compile(r"^fb\.1\.\d+\.[A-Za-z0-9_\-]{5,512}$")


def build_fbc(fbclid: str | None, creation_ms: int | None = None) -> str | None:
    """Create Meta fbc from fbclid exactly once, at click time."""
    if not fbclid or not _FBCLID_RE.match(fbclid.strip()):
        return None
    ts = creation_ms or int(time.time() * 1000)
    return f"fb.1.{ts}.{fbclid.strip()}"


def normalize_fbc_or_build(fbc_param: str | None, fbclid: str | None) -> str | None:
    """
    If the landing page already sent a correct fbc (e.g. from Pixel cookie),
    keep it. Otherwise build from fbclid. Never invent a random value.
    """
    if fbc_param and _FBC_RE.match(fbc_param.strip()):
        return fbc_param.strip()
    return build_fbc(fbclid)


def normalize_fbp(fbp_param: str | None, cookie_fbp: str | None = None) -> str | None:
    """
    fbp must be the real _fbp cookie value. Prefer explicit param, fallback to
    cookie. Never generate a fake one.
    Accepts `fb.1.<ts>.<rand>` or `fb.2.<ts>.<rand>` formats.
    """
    raw = (fbp_param or cookie_fbp or "").strip()
    if not raw:
        return None
    # loose validation — Meta's format is fb.<ver>.<ts>.<rand>
    if re.match(r"^fb\.[12]\.\d+\.\d+$", raw):
        return raw
    return None


def extract_tracking_params(query: dict[str, str], cookies: dict[str, str] | None = None) -> dict[str, str | None]:
    """Pull known tracking params from query + cookies (for fbp)."""
    out: dict[str, str | None] = {}
    for key in TRACKING_PARAMS:
        val = query.get(key)
        if val is not None:
            # parse_qs returns lists; our caller normalizes to str
            if isinstance(val, list):
                val = val[0] if val else None
            out[key] = val.strip() if isinstance(val, str) and val.strip() else None
        else:
            out[key] = None
    # fbp from _fbp cookie if not in query
    if not out.get("fbp") and cookies:
        out["fbp"] = cookies.get("_fbp")
    return out


def get_client_ip(request_headers: dict[str, str], client_host: str | None, trust_hops: int = 1) -> str:
    """Extract real client IP respecting TRUST_PROXY_HOPS."""
    xff = request_headers.get("x-forwarded-for") or request_headers.get("X-Forwarded-For") or ""
    if xff and trust_hops > 0:
        parts = [p.strip() for p in xff.split(",") if p.strip()]
        # trust last N hops
        if parts:
            idx = -min(trust_hops, len(parts))
            cand = parts[idx]
            if cand:
                return cand
    x_real = request_headers.get("x-real-ip") or request_headers.get("X-Real-IP")
    if x_real and trust_hops > 0:
        return x_real.strip()
    return (client_host or "0.0.0.0").strip()


def anonymize_ip(ip: str) -> str:
    """Truncate to /24 (v4) or /64 (v6) if privacy setting enabled."""
    if not ip or ip in ("0.0.0.0", "127.0.0.1"):
        return ip
    if ":" in ip:  # v6
        parts = ip.split(":")
        return ":".join(parts[:4]) + "::"
    parts = ip.split(".")
    if len(parts) == 4:
        return ".".join(parts[:3] + ["0"])
    return ip


# ── device / browser detection (lightweight, no extra dep) ────────────

_BROWSER_PATTERNS = [
    (re.compile(r"Edg/([\d.]+)"), "Edge"),
    (re.compile(r"OPR/([\d.]+)"), "Opera"),
    (re.compile(r"Chrome/([\d.]+)"), "Chrome"),
    (re.compile(r"Firefox/([\d.]+)"), "Firefox"),
    (re.compile(r"Version/([\d.]+).*Safari"), "Safari"),
    (re.compile(r"Safari/([\d.]+)"), "Safari"),
]

_OS_PATTERNS = [
    (re.compile(r"Windows NT"), "Windows"),
    (re.compile(r"Mac OS X"), "macOS"),
    (re.compile(r"Android"), "Android"),
    (re.compile(r"iPhone|iPad"), "iOS"),
    (re.compile(r"Linux"), "Linux"),
]

_DEVICE_TYPE_RE = re.compile(r"Mobile|Android|iPhone|iPad", re.I)


def parse_user_agent(ua: str) -> dict[str, str | None]:
    """Very small UA parser — good enough for breakdowns, not fingerprinting."""
    if not ua:
        return {"browser": None, "browser_version": None, "os": None, "device_type": None}
    browser, version = None, None
    for pat, name in _BROWSER_PATTERNS:
        m = pat.search(ua)
        if m:
            browser, version = name, m.group(1).split(".")[0] if m.lastindex else None
            break
    os_name = None
    for pat, name in _OS_PATTERNS:
        if pat.search(ua):
            os_name = name
            break
    device_type = "mobile" if _DEVICE_TYPE_RE.search(ua) else "desktop"
    # Telegram in-app browser
    if "Telegram" in ua:
        device_type = "mobile"
    return {"browser": browser, "browser_version": version, "os": os_name, "device_type": device_type}


def is_crawler(user_agent: str) -> bool:
    if not user_agent:
        return False
    ua = user_agent.lower()
    patterns = [
        "telegrambot", "twitterbot", "facebookexternalhit", "whatsapp", "linkedinbot",
        "discordbot", "slackbot", "skypeuripreview", "viberbot", "bingbot", "googlebot",
        "yandexbot", "applebot", "embedly", "vkshare", "redditbot", "crawler", "spider", "preview",
        "facebookcatalog", "amazonbot", "applebot", "baiduspider",
    ]
    return any(p in ua for p in patterns)


# ── click creation ────────────────────────────────────────────────────

async def create_click(
    *,
    click_id: str,
    campaign_id: int | None,
    tracking_link_id: int | None,
    domain_id: int | None,
    params: dict[str, str | None],
    request_data: dict[str, Any],
    event_source_url: str | None = None,
) -> Any:
    """
    Persist a Click row. Returns the Click object.
    `params` is the dict from extract_tracking_params (includes fbclid/fbc/fbp/subs).
    `request_data` contains ip, user_agent, referrer, landing_page, etc.
    """
    from shared.models import Click

    fbc = normalize_fbc_or_build(params.get("fbc"), params.get("fbclid"))
    fbp = normalize_fbp(params.get("fbp"))

    ua = request_data.get("user_agent") or ""
    parsed_ua = parse_user_agent(ua)

    # optional privacy
    settings = get_settings()
    ip = request_data.get("ip")
    if settings.ANONYMIZE_IPS and ip:
        ip = anonymize_ip(ip)

    # build click row
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=settings.REDIS_TTL_HOURS)

    click = Click(
        click_id=click_id,
        campaign_id=campaign_id,
        tracking_link_id=tracking_link_id,
        domain_id=domain_id,
        fbclid=params.get("fbclid"),
        fbc=fbc,
        fbp=fbp,
        campaign_name=params.get("campaign"),
        adset=params.get("adset"),
        adset_id=params.get("adset_id"),
        ad=params.get("ad"),
        ad_id=params.get("ad_id"),
        creative=params.get("creative"),
        placement=params.get("placement"),
        sub1=params.get("sub1"),
        sub2=params.get("sub2"),
        sub3=params.get("sub3"),
        sub4=params.get("sub4"),
        sub5=params.get("sub5"),
        sub6=params.get("sub6"),
        sub7=params.get("sub7"),
        sub8=params.get("sub8"),
        sub9=params.get("sub9"),
        ip=ip,
        country=request_data.get("country"),
        region=request_data.get("region"),
        city=request_data.get("city"),
        timezone=request_data.get("timezone"),
        language=request_data.get("language"),
        user_agent=ua,
        browser=parsed_ua["browser"],
        browser_version=parsed_ua["browser_version"],
        os=parsed_ua["os"],
        device=parsed_ua.get("device"),
        device_type=parsed_ua["device_type"],
        referrer=request_data.get("referrer"),
        landing_page=request_data.get("landing_page"),
        event_id=str(uuid.uuid4()),
        event_source_url=event_source_url or request_data.get("landing_page"),
        created_at=bind_ts(now),
        expires_at=bind_ts(expires_at),
    )
    async with AsyncSessionLocal() as db:
        db.add(click)
        await db.commit()
        await db.refresh(click)

    # also write Redis correlation cache (for fast bot-start resolution)
    try:
        import redis.asyncio as aioredis
        redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        payload = {
            "click_id": click.id,
            "click_public_id": click.click_id,
            "campaign_id": campaign_id,
            "tracking_link_id": tracking_link_id,
            "fbclid": params.get("fbclid") or "",
            "fbc": fbc or "",
            "fbp": fbp or "",
            "campaign": params.get("campaign") or "",
            "adset": params.get("adset") or "",
            "ad": params.get("ad") or "",
            "sub1": params.get("sub1") or "",
            "sub2": params.get("sub2") or "",
            "sub3": params.get("sub3") or "",
            "sub4": params.get("sub4") or "",
            "sub5": params.get("sub5") or "",
            "sub6": params.get("sub6") or "",
            "sub7": params.get("sub7") or "",
            "sub8": params.get("sub8") or "",
            "sub9": params.get("sub9") or "",
            "ip": ip or "",
            "user_agent": ua,
            "event_id": click.event_id,
            "created_at": now.isoformat(),
        }
        await redis.setex(f"click:{click.click_id}", settings.REDIS_TTL_HOURS * 3600, json.dumps(payload))
        await redis.aclose()
    except Exception:
        pass  # DB is source of truth; Redis is best-effort cache

    return click


async def get_click_by_public_id(click_public_id: str) -> Any | None:
    from shared.models import Click
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Click).where(Click.click_id == click_public_id))
        return result.scalar_one_or_none()


async def consume_click_payload(public_id: str) -> dict | None:
    """
    Try Redis first (fast), fallback to DB. Atomic GETDEL when possible.
    Returns the click payload dict or None if not found/expired.
    """
    settings = get_settings()
    # Redis fast path
    try:
        import redis.asyncio as aioredis
        redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
        try:
            raw = await redis.getdel(f"click:{public_id}")
        except AttributeError:
            raw = await redis.get(f"click:{public_id}")
            if raw:
                await redis.delete(f"click:{public_id}")
        await redis.aclose()
        if raw:
            return json.loads(raw)
    except Exception:
        pass

    # DB fallback (still valid even after Redis TTL if retention keeps it)
    click = await get_click_by_public_id(public_id)
    if not click:
        return None
    # check expiry
    now = datetime.now(timezone.utc)
    if click.expires_at and click.expires_at.replace(tzinfo=timezone.utc) < now:
        return None
    return {
        "click_id": click.id,
        "click_public_id": click.click_id,
        "campaign_id": click.campaign_id,
        "tracking_link_id": click.tracking_link_id,
        "fbclid": click.fbclid or "",
        "fbc": click.fbc or "",
        "fbp": click.fbp or "",
        "campaign": click.campaign_name or "",
        "adset": click.adset or "",
        "ad": click.ad or "",
        "sub1": click.sub1 or "",
        "sub2": click.sub2 or "",
        "sub3": click.sub3 or "",
        "sub4": click.sub4 or "",
        "sub5": click.sub5 or "",
        "sub6": click.sub6 or "",
        "sub7": click.sub7 or "",
        "sub8": click.sub8 or "",
        "sub9": click.sub9 or "",
        "ip": click.ip or "",
        "user_agent": click.user_agent or "",
        "event_id": click.event_id,
        "created_at": click.created_at.isoformat() if click.created_at else "",
    }

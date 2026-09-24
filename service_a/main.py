"""
service_a/main.py — FastAPI Web Router + WebSocket hub
Full production API: click tracking, auth, accounts, campaigns,
conversions, messages, funnels, real-time WebSocket notifications.
"""
import asyncio, hashlib, hmac, json, logging, os, secrets, string, sys, urllib.parse
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select, text, update, desc
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings, validate_or_exit
from shared.logging_config import setup_logging
from shared.database import get_db, init_db, AsyncSessionLocal
from shared.models import (
    AccessToken, AccountType, Campaign, ClickDestinationType, ConversionLog, ConversionStatus,
    ConversionTrigger, DashboardUser, EventType, Funnel, FunnelStep,
    Message, MessageDirection, TelegramAccount, TriggerType, UserSession, WebhookToken
)
from shared.security import hash_password, verify_password, sign_token, verify_token, TokenError, mask_secret
from service_a.websocket_manager import ws_manager
from shared.tracking import build_fbc, normalize_fbc_or_build, normalize_fbp, parse_user_agent, get_client_ip, anonymize_ip
from shared.meta import build_user_data as build_meta_user_data, build_custom_data, new_event_id as new_meta_event_id, dedup_key as meta_dedup_key

setup_logging("service_a")
validate_or_exit("dashboard")

logger = logging.getLogger(__name__)
settings = get_settings()
redis_client: aioredis.Redis | None = None
_memory_sessions: dict[str, str] = {}
_memory_session_expiry: dict[str, float] = {}

# ─────────────────────────────────────────────────────────────────────
# Session tokens
#
# X-Telegram-Id alone is NOT proof of identity — it's just a number the
# browser sends, and anyone can change it via dev tools to impersonate
# any user. The actual proof of identity happens once, at login time,
# via cryptographic verification (Telegram Web App initData HMAC, or
# the Telegram Login Widget HMAC). After that verification succeeds we
# issue an opaque session token stored in Redis; every subsequent API
# call must present that token, and the server looks up the real
# telegram_id from Redis — never from a client-supplied header.
# ─────────────────────────────────────────────────────────────────────

SESSION_TTL_SECONDS = 7 * 24 * 3600  # 7 days


async def _create_session(telegram_id: int) -> str:
    import time as _time
    token = secrets.token_urlsafe(32)
    try:
        if redis_client:
            await redis_client.setex(f"session:{token}", SESSION_TTL_SECONDS, str(telegram_id))
            return token
    except Exception:
        pass
    # Fallback to in-memory (dev without Redis)
    _memory_sessions[token] = str(telegram_id)
    _memory_session_expiry[token] = _time.time() + SESSION_TTL_SECONDS
    return token


async def _resolve_session(token: str) -> int | None:
    import time as _time
    try:
        if redis_client:
            raw = await redis_client.get(f"session:{token}")
            if raw is not None:
                await redis_client.expire(f"session:{token}", SESSION_TTL_SECONDS)
                return int(raw)
            # also check memory fallback
    except Exception:
        pass
    # Memory fallback
    exp = _memory_session_expiry.get(token)
    if exp and exp > _time.time():
        # sliding expiry
        _memory_session_expiry[token] = _time.time() + SESSION_TTL_SECONDS
        return int(_memory_sessions[token])
    elif exp:
        _memory_sessions.pop(token, None)
        _memory_session_expiry.pop(token, None)
    # also try memory even if redis was not tried
    if token in _memory_sessions:
        exp2 = _memory_session_expiry.get(token)
        if exp2 and exp2 > _time.time():
            _memory_session_expiry[token] = _time.time() + SESSION_TTL_SECONDS
            return int(_memory_sessions[token])
    return None
DASHBOARD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard")


async def _next_local_id(db: AsyncSession) -> int:
    """
    Allocates a synthetic telegram_id for a username/password-only user.
    Real Telegram ids are always positive, so negative ids can never
    collide with a real account — this lets both login styles share one
    table (and the existing session mechanism, which keys on telegram_id)
    with no schema fork.
    """
    result = await db.execute(select(func.min(DashboardUser.telegram_id)))
    lowest = result.scalar()
    if lowest is None or lowest > 0:
        return -1
    return lowest - 1


async def _seed_admin_accounts():
    """
    Runs once at startup. Auto-creates (or refreshes) admin access from
    whichever credentials are configured in .env:
      - ADMIN_USERNAME / ADMIN_PASSWORD → username/password admin login,
        no Telegram bot, widget, or domain setup required at all.
      - ADMIN_TELEGRAM_ID (+ MASTER_BOT_TOKEN) → Telegram-based admin login,
        as before. Fully optional now.
    Both can be set at once — same admin, two ways in.
    """
    async with AsyncSessionLocal() as db:
        if settings.ADMIN_TELEGRAM_ID:
            existing = await db.get(DashboardUser, settings.ADMIN_TELEGRAM_ID)
            if not existing:
                db.add(DashboardUser(telegram_id=settings.ADMIN_TELEGRAM_ID, first_name="Admin",
                                      is_admin=True, is_active=True))
                logger.info("Auto-created Telegram admin: %d", settings.ADMIN_TELEGRAM_ID)

        if settings.ADMIN_USERNAME and settings.ADMIN_PASSWORD:
            result = await db.execute(
                select(DashboardUser).where(DashboardUser.login_username == settings.ADMIN_USERNAME)
            )
            user = result.scalar_one_or_none()
            if not user:
                synthetic_id = await _next_local_id(db)
                db.add(DashboardUser(
                    telegram_id=synthetic_id, first_name=settings.ADMIN_USERNAME,
                    login_username=settings.ADMIN_USERNAME,
                    password_hash=hash_password(settings.ADMIN_PASSWORD),
                    is_admin=True, is_active=True,
                ))
                logger.info("Auto-created local admin login: %s", settings.ADMIN_USERNAME)
            else:
                # Keep the password in sync with .env — editing .env and
                # restarting is enough to change it, no DB surgery needed.
                user.password_hash = hash_password(settings.ADMIN_PASSWORD)
                user.is_admin = True
                user.is_active = True

        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    await init_db()
    await _seed_admin_accounts()
    redis_client = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    # Subscribe to notifications from Service B via Redis pub/sub
    import asyncio
    asyncio.create_task(_redis_subscriber())
    yield
    if redis_client:
        await redis_client.aclose()


async def _redis_subscriber():
    """
    Listen for events published by Service B and forward to WebSocket clients.
    Auto-reconnects when Redis drops the connection (e.g. Redis restart, Docker
    stop/start, Docker's idle connection cleanup after ~5 minutes of inactivity).
    Without this, the subscriber task dies and real-time notifications stop
    working until the whole app restarts.
    """
    global redis_client
    while True:
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe("tg_notifications")
            logger.info("Redis pub/sub subscriber connected")
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        event = json.loads(message["data"])
                        user_id = event.pop("target_user_id", None)
                        if user_id:
                            await ws_manager.send_to_user(user_id, event)
                        else:
                            await ws_manager.broadcast(event)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("Redis subscriber lost connection: %s — reconnecting in 3s", e)
            try:
                await pubsub.close()
            except Exception:
                pass
            await asyncio.sleep(3)


app = FastAPI(title="TG Tracker API", version="3.0.0", lifespan=lifespan,
              docs_url="/internal/docs", redoc_url=None)

# CORS — the dashboard is served from this same origin (via /dashboard mount),
# but Telegram's Web App wrapper loads it inside an iframe on web.telegram.org,
# so that origin (and t.me) must be allowed too. allow_origins=["*"] combined
# with allow_credentials=True is rejected by browsers anyway and is an
# unnecessary attack surface for an API that handles access tokens.
_allowed_origins = [
    settings.BASE_URL,
    "https://web.telegram.org",
    "https://t.me",
]
# Local dev: allow ngrok / localhost variants so testing isn't broken
if not settings.BASE_URL.startswith("https://"):
    _allowed_origins += ["http://localhost:8000", "http://127.0.0.1:8000"]

app.add_middleware(CORSMiddleware, allow_origins=_allowed_origins, allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

app.mount("/dashboard", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")


# ── Auth helpers ──────────────────────────────────────────────────────

async def require_user(
    authorization: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> DashboardUser:
    """
    Resolves the authenticated user from a session token, NEVER from a
    client-supplied ID. Expects: Authorization: Bearer <session_token>
    The token only exists if /api/auth/verify or /api/auth/login-widget
    previously succeeded — both require cryptographic proof of Telegram
    identity, so a session token is real proof of "this is who they say
    they are", unlike a raw X-Telegram-Id header.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing_session")
    token = authorization[len("Bearer "):].strip()
    if not token:
        raise HTTPException(401, "missing_session")

    telegram_id = await _resolve_session(token)
    if telegram_id is None:
        raise HTTPException(401, "session_expired")

    result = await db.execute(
        select(DashboardUser).where(
            DashboardUser.telegram_id == telegram_id,
            DashboardUser.is_active == True,
        )
    )
    user = result.scalar_one_or_none()
    if not user:
        # Access was revoked after the session was issued — kill the session too
        await redis_client.delete(f"session:{token}")
        raise HTTPException(403, "Access denied")
    return user


async def require_admin(user: DashboardUser = Depends(require_user)) -> DashboardUser:
    if not user.is_admin:
        raise HTTPException(403, "Admin access required")
    return user


def _gen_key(n=10):
    return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(n))


# User-Agent substrings of known link-preview crawlers and bots.
# These fetch tracking URLs to generate link previews (Telegram, WhatsApp,
# Facebook share previews, etc.) — NOT real ad clicks. We must NOT count
# these as clicks or they inflate click counts with no matching conversion.
_CRAWLER_UA_PATTERNS = [
    "telegrambot", "twitterbot", "facebookexternalhit", "whatsapp",
    "linkedinbot", "discordbot", "slackbot", "skypeuripreview",
    "viberbot", "bingbot", "googlebot", "yandexbot", "applebot",
    "embedly", "vkshare", "redditbot", "tumblr", "pinterest",
    "bot", "crawler", "spider", "preview", "facebookcatalog",
]


def _is_crawler(user_agent: str) -> bool:
    """Returns True if the User-Agent looks like a link-preview bot/crawler."""
    if not user_agent:
        return False
    ua = user_agent.lower()
    return any(pattern in ua for pattern in _CRAWLER_UA_PATTERNS)


# ═══════════════════════════════════════════════════════
# WEBSOCKET — real-time notifications
# ═══════════════════════════════════════════════════════

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    """
    Dashboard connects here on load.
    Receives real-time notifications pushed from Service B via Redis pub/sub.
    """
    await ws_manager.connect(websocket, user_id)
    try:
        while True:
            # Keep alive — client sends ping every 30s
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, user_id)


# ═══════════════════════════════════════════════════════
# CLICK CAPTURE
# ═══════════════════════════════════════════════════════

@app.get("/t/{slug}")
async def capture_click(slug: str, request: Request,
                         fbclid: str | None = Query(default=None),
                         fbc: str | None = Query(default=None),
                         fbp: str | None = Query(default=None),
                         campaign: str | None = Query(default=None),
                         campaign_id: str | None = Query(default=None),
                         adset: str | None = Query(default=None),
                         adset_id: str | None = Query(default=None),
                         ad: str | None = Query(default=None),
                         ad_id: str | None = Query(default=None),
                         creative: str | None = Query(default=None),
                         placement: str | None = Query(default=None),
                         sub1: str | None = Query(default=None),
                         sub2: str | None = Query(default=None),
                         sub3: str | None = Query(default=None),
                         sub4: str | None = Query(default=None),
                         sub5: str | None = Query(default=None),
                         sub6: str | None = Query(default=None),
                         sub7: str | None = Query(default=None),
                         sub8: str | None = Query(default=None),
                         sub9: str | None = Query(default=None),
                         db: AsyncSession = Depends(get_db)):
    # Try campaign first, then tracking_link
    campaign_obj = None
    tracking_link = None
    result = await db.execute(select(Campaign).where(Campaign.slug == slug, Campaign.is_active == True))
    campaign_obj = result.scalar_one_or_none()
    if not campaign_obj:
        from shared.models import TrackingLink
        r2 = await db.execute(select(TrackingLink).where(TrackingLink.slug == slug, TrackingLink.is_active == True))
        tracking_link = r2.scalar_one_or_none()
        if not tracking_link:
            raise HTTPException(404, "Campaign or tracking link not found")
        if tracking_link.campaign_id:
            campaign_obj = await db.get(Campaign, tracking_link.campaign_id)

    if tracking_link and tracking_link.destination:
        dest = tracking_link.destination
        if "t.me/" in dest:
            target = dest.split("t.me/")[-1].split("?")[0].split("/")[0].lstrip("@")
        else:
            target = dest.lstrip("@")
        owner_id = tracking_link.user_id
        campaign_id_val = tracking_link.campaign_id or (campaign_obj.id if campaign_obj else None)
        account_id_val = campaign_obj.account_id if campaign_obj else None
        # if link has campaign, keep campaign_obj for later but target is link's destination
    elif campaign_obj:
        target = campaign_obj.target_telegram_username.lstrip("@")
        owner_id = campaign_obj.user_id
        campaign_id_val = campaign_obj.id
        account_id_val = campaign_obj.account_id
    else:
        raise HTTPException(404, "Not found")

    acct = await db.get(TelegramAccount, account_id_val) if account_id_val else None
    raw_headers = dict(request.headers)
    client_host = request.client.host if request.client else "0.0.0.0"
    client_ip = get_client_ip(raw_headers, client_host, trust_hops=settings.TRUST_PROXY_HOPS)
    user_agent = request.headers.get("User-Agent", "") or ""
    referrer = request.headers.get("Referer") or request.headers.get("referer")
    landing_page = str(request.url)
    language = request.headers.get("Accept-Language", "")[:32] if request.headers.get("Accept-Language") else None

    if _is_crawler(user_agent) and settings.FILTER_CRAWLERS:
        return RedirectResponse(url=f"https://t.me/{target}", status_code=302)

    rl_key = f"ratelimit:click:{client_ip}"
    try:
        current = await redis_client.incr(rl_key)
        if current == 1:
            await redis_client.expire(rl_key, settings.CLICK_RATE_LIMIT_WINDOW_SECONDS)
        if current > settings.CLICK_RATE_LIMIT_MAX:
            logger.warning("Rate limit exceeded for IP %s on slug %s", client_ip, slug)
            return RedirectResponse(url=f"https://t.me/{target}", status_code=302)
    except Exception:
        pass

    cookies = dict(request.cookies) if hasattr(request, "cookies") else {}
    effective_fbp = fbp or cookies.get("_fbp") or request.query_params.get("fbp")
    effective_fbc = normalize_fbc_or_build(fbc, fbclid)
    effective_fbp_norm = normalize_fbp(effective_fbp)

    params = {
        "fbclid": fbclid, "fbc": effective_fbc, "fbp": effective_fbp_norm,
        "campaign": campaign, "campaign_id": campaign_id, "adset": adset, "adset_id": adset_id,
        "ad": ad, "ad_id": ad_id, "creative": creative, "placement": placement,
        "sub1": sub1, "sub2": sub2, "sub3": sub3, "sub4": sub4, "sub5": sub5,
        "sub6": sub6, "sub7": sub7, "sub8": sub8, "sub9": sub9,
    }

    from shared.tracking import create_click as tracking_create_click
    from shared.models import TrackingLink as TL
    short_key = _gen_key(10)
    try:
        correlation_token = sign_token({"cid": short_key, "purp": "click"}, settings.SECRET_KEY, settings.CORRELATION_TOKEN_TTL_SECONDS)
    except Exception:
        correlation_token = short_key

    request_data = {
        "ip": client_ip, "user_agent": user_agent, "referrer": referrer,
        "landing_page": landing_page, "language": language,
    }

    try:
        click = await tracking_create_click(
            click_id=short_key,
            campaign_id=campaign_id_val,
            tracking_link_id=tracking_link.id if tracking_link else None,
            domain_id=tracking_link.domain_id if tracking_link else None,
            params=params,
            request_data=request_data,
            event_source_url=landing_page,
        )
        try:
            await redis_client.setex(f"correlation:{correlation_token}", settings.CORRELATION_TOKEN_TTL_SECONDS, json.dumps({"click_id": click.id, "click_public_id": short_key, "campaign_id": campaign_id_val}))
        except Exception:
            pass
        event_id_for_pixel = click.event_id
    except Exception as e:
        logger.exception("Failed to persist click: %s", e)
        payload = {
            "fbclid": fbclid or "", "fbc": effective_fbc or "", "fbp": effective_fbp_norm or "",
            "campaign_id": campaign_id_val, "account_id": account_id_val,
            "user_id": owner_id,
            "event_type": campaign_obj.event_type if campaign_obj else "Lead",
            "meta_pixel_id": acct.meta_pixel_id if acct else "",
            "meta_capi_token": acct.meta_capi_token if acct else "",
            "target_username": target,
            "client_ip": client_ip, "user_agent": user_agent,
            "clicked_at": datetime.now(timezone.utc).isoformat(),
            "subs": {f"sub{i}": params.get(f"sub{i}") for i in range(1,10)},
        }
        try:
            if redis_client:
                await redis_client.setex(f"click:{short_key}", settings.REDIS_TTL_HOURS * 3600, json.dumps(payload))
        except Exception:
            pass
        if campaign_obj:
            await db.execute(update(Campaign).where(Campaign.id == campaign_obj.id).values(total_clicks=Campaign.total_clicks + 1))
            await db.commit()
        event_id_for_pixel = str(payload.get("event_id", short_key))
        correlation_token = short_key

    if tracking_link:
        try:
            await db.execute(update(TL).where(TL.id == tracking_link.id).values(total_clicks=TL.total_clicks + 1))
            await db.commit()
        except Exception:
            pass
    elif campaign_obj:
        try:
            await db.execute(update(Campaign).where(Campaign.id == campaign_obj.id).values(total_clicks=Campaign.total_clicks + 1))
            await db.commit()
        except Exception:
            pass

    try:
        if redis_client:
            await redis_client.publish("tg_notifications", json.dumps({
                "target_user_id": owner_id,
                "type": "click",
                "title": "New Click",
                "body": f"{(campaign_obj.name if campaign_obj else slug)} — {client_ip}",
                "data": {"campaign_id": campaign_id_val, "campaign_name": campaign_obj.name if campaign_obj else slug},
                "ts": datetime.now(timezone.utc).isoformat(),
            }))
    except Exception:
        pass

    token_for_tg = correlation_token if len(correlation_token) < 64 else short_key
    if len(token_for_tg) > 64:
        token_for_tg = short_key

    return RedirectResponse(url=f"https://t.me/{target}?start={token_for_tg}", status_code=302)


# ═══════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════
#
# Two legitimate ways to authenticate, both cryptographically verified
# against MASTER_BOT_TOKEN — there is no other path into the dashboard.
#
# 1. Telegram Web App (in-app):
#    Dashboard opened via the bot's menu button / Web App button inside
#    Telegram. Telegram signs a payload (initData) with HMAC-SHA256 using
#    the bot token as the key. We recompute the HMAC server-side and only
#    trust the payload if it matches exactly.
#
# 2. Telegram Login Widget (browser):
#    For opening the dashboard directly in a browser (e.g. via the ngrok
#    URL), Telegram's official Login Widget produces a similarly signed
#    payload after the person taps "Log in with Telegram" and confirms
#    inside their own Telegram app. Same HMAC verification approach,
#    different field set per Telegram's login widget spec.
#
# CRITICAL: there is intentionally NO fallback that trusts an unsigned
# Telegram ID. A previous version of this code accepted a free-typed
# numeric ID with no proof of ownership — anyone who knew or guessed
# your ID could log in as you. That path has been removed entirely.

def _verify_webapp_init_data(init_data: str) -> dict[str, Any]:
    """
    Verifies Telegram Web App initData per Telegram's official spec:
    https://core.telegram.org/bots/webapps#validating-data-received-via-the-web-app
    Raises ValueError if the signature doesn't match — caller must NOT
    proceed with unverified data on failure.
    """
    parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        raise ValueError("Missing hash")
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", settings.MASTER_BOT_TOKEN.encode(), hashlib.sha256).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        raise ValueError("Invalid signature")
    return json.loads(parsed.get("user", "{}"))


def _verify_login_widget_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Verifies a Telegram Login Widget payload per Telegram's official spec:
    https://core.telegram.org/widgets/login#checking-authorization

    The widget sends fields (id, first_name, username, photo_url, auth_date,
    hash) signed with SHA256(bot_token) as the HMAC key — note this differs
    from the Web App scheme, which uses the literal string "WebAppData" as
    the key. Mixing these up silently breaks verification, so they're kept
    as two separate functions rather than one "smart" one.

    Also rejects stale logins — auth_date older than 24h is refused, since
    a leaked/replayed login link should not grant indefinite access.
    """
    payload = dict(payload)  # don't mutate caller's dict
    received_hash = payload.pop("hash", None)
    if not received_hash:
        raise ValueError("Missing hash")

    auth_date = payload.get("auth_date")
    if not auth_date:
        raise ValueError("Missing auth_date")
    try:
        auth_ts = int(auth_date)
    except (TypeError, ValueError):
        raise ValueError("Invalid auth_date")
    if (datetime.now(timezone.utc).timestamp() - auth_ts) > 86400:
        raise ValueError("Login expired — please log in again")

    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(payload.items()) if v is not None
    )
    secret_key = hashlib.sha256(settings.MASTER_BOT_TOKEN.encode()).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, received_hash):
        raise ValueError("Invalid signature")

    return payload


async def _login_user(tg_id: int, db: AsyncSession) -> dict[str, Any]:
    """
    Shared post-verification step: confirm dashboard access, then issue
    a session token. This is the ONLY place session tokens are created,
    and it only runs after cryptographic verification of Telegram
    identity in the caller (verify_auth or verify_login_widget).
    """
    result = await db.execute(
        select(DashboardUser).where(
            DashboardUser.telegram_id == tg_id,
            DashboardUser.is_active == True,
        )
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(403, "no_access")

    await db.execute(
        update(DashboardUser).where(DashboardUser.telegram_id == tg_id)
        .values(last_login=datetime.now(timezone.utc))
    )
    await db.commit()

    session_token = await _create_session(tg_id)

    return {"ok": True, "session_token": session_token,
            "telegram_id": tg_id, "first_name": user.first_name,
            "username": user.username, "is_admin": user.is_admin}


@app.post("/api/auth/verify")
async def verify_auth(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Verify Telegram Web App initData and return user profile.
    Called by dashboard on load when opened inside Telegram.

    Returns:
      200 { ok, telegram_id, first_name, username, is_admin }
      403 { detail: "no_access" }          — verified identity, not in dashboard_users
      403 { detail: "invalid_signature" }  — HMAC check failed, request rejected
      400 { detail: "empty_initdata" }     — not opened via Telegram
    """
    body = await request.json()
    init_data = body.get("initData", "").strip()

    if not init_data:
        raise HTTPException(400, "empty_initdata")

    try:
        user_data = _verify_webapp_init_data(init_data)
    except ValueError as e:
        logger.warning("initData HMAC verification failed: %s", e)
        raise HTTPException(403, "invalid_signature")

    tg_id = user_data.get("id")
    if not tg_id:
        raise HTTPException(400, "empty_initdata")

    return await _login_user(tg_id, db)


@app.post("/api/auth/login-widget")
async def verify_login_widget(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Verify a Telegram Login Widget callback and return user profile.
    Used for browser-mode access (outside the Telegram app) — replaces
    the old free-text "enter your Telegram ID" box, which had no proof
    of identity and let anyone who knew/guessed an ID log in as that user.

    Frontend integration: render Telegram's official login widget script
    pointing at this bot, and POST the resulting auth object here.
    https://core.telegram.org/widgets/login

    Returns the same shape as /api/auth/verify.
    """
    body = await request.json()
    try:
        user_data = _verify_login_widget_payload(body)
    except ValueError as e:
        logger.warning("Login widget verification failed: %s", e)
        raise HTTPException(403, str(e) if "expired" in str(e).lower() else "invalid_signature")

    tg_id = user_data.get("id")
    if not tg_id:
        raise HTTPException(400, "missing_id")
    try:
        tg_id = int(tg_id)
    except (TypeError, ValueError):
        raise HTTPException(400, "invalid_id")

    return await _login_user(tg_id, db)


@app.post("/api/auth/login")
async def login_with_password(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Username + password login — no Telegram account, bot, or widget
    involved at all. This is the primary browser login path; Telegram
    login (above) remains available as an alternative but is optional.

    Returns the same shape as /api/auth/verify.
      200 { ok, session_token, telegram_id, first_name, username, is_admin }
      401 { detail: "invalid_credentials" }
      400 { detail: "missing_credentials" }
    """
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not username or not password:
        raise HTTPException(400, "missing_credentials")

    result = await db.execute(
        select(DashboardUser).where(
            func.lower(DashboardUser.login_username) == username.lower(),
            DashboardUser.is_active == True,
        )
    )
    user = result.scalar_one_or_none()
    # Same generic error whether the username doesn't exist or the
    # password is wrong — don't let a client enumerate valid usernames.
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(401, "invalid_credentials")

    return await _login_user(user.telegram_id, db)


@app.get("/api/me")
async def get_me(user: DashboardUser = Depends(require_user)):
    return {"ok": True, "telegram_id": user.telegram_id, "first_name": user.first_name,
            "username": user.username, "login_username": user.login_username,
            "is_admin": user.is_admin}


@app.post("/api/me/password")
async def set_my_password(body: dict, user: DashboardUser = Depends(require_user),
                           db: AsyncSession = Depends(get_db)):
    """
    Set/change the current user's own username+password login. Works for
    ANY logged-in user (including one who signed in via Telegram) — this
    is how a Telegram-first user adds a password fallback, or how a local
    user changes their password.
    """
    new_password = body.get("new_password") or ""
    username = (body.get("username") or user.login_username or "").strip()
    if len(new_password) < 8:
        raise HTTPException(400, "password_too_short")
    if not username:
        raise HTTPException(400, "username_required")

    clash = await db.execute(select(DashboardUser).where(
        func.lower(DashboardUser.login_username) == username.lower(),
        DashboardUser.telegram_id != user.telegram_id,
    ))
    if clash.scalar_one_or_none():
        raise HTTPException(409, "username_taken")

    await db.execute(
        update(DashboardUser).where(DashboardUser.telegram_id == user.telegram_id)
        .values(password_hash=hash_password(new_password), login_username=username)
    )
    await db.commit()
    return {"ok": True, "username": username}


@app.post("/api/auth/logout")
async def logout(authorization: str | None = Header(default=None)):
    """Revoke the current session token immediately."""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):].strip()
        if token:
            try:
                if redis_client:
                    await redis_client.delete(f"session:{token}")
            except Exception:
                pass
            _memory_sessions.pop(token, None)
            _memory_session_expiry.pop(token, None)
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# USERS
# ═══════════════════════════════════════════════════════

@app.get("/api/users")
async def list_users(admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DashboardUser).order_by(DashboardUser.created_at))
    return [{"telegram_id": u.telegram_id, "username": u.username, "first_name": u.first_name,
             "login_username": u.login_username, "login_type": "local" if u.telegram_id < 0 else "telegram",
             "is_active": u.is_active, "is_admin": u.is_admin,
             "created_at": u.created_at.isoformat(),
             "last_login": u.last_login.isoformat() if u.last_login else None}
            for u in result.scalars().all()]


@app.post("/api/users", status_code=201)
async def grant_access(body: dict, admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    """Grants access to an existing/known Telegram user by numeric ID."""
    tg_id = body.get("telegram_id")
    if not tg_id:
        raise HTTPException(400, "telegram_id required")
    existing = await db.get(DashboardUser, tg_id)
    if existing:
        existing.is_active = True
        await db.commit()
        return {"ok": True, "action": "reactivated"}
    db.add(DashboardUser(telegram_id=tg_id, first_name=body.get("first_name","User"),
                          username=body.get("username"), is_active=True, is_admin=False))
    await db.commit()
    return {"ok": True, "action": "created"}


@app.post("/api/users/local", status_code=201)
async def create_local_user(body: dict, admin: DashboardUser = Depends(require_admin),
                             db: AsyncSession = Depends(get_db)):
    """
    Creates a dashboard user with a username/password login — no Telegram
    account needed for THEM either. Useful for giving a teammate or
    client access without them having a Telegram account, or wanting a
    login independent of one.
    """
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    first_name = (body.get("first_name") or username).strip()
    is_admin_flag = bool(body.get("is_admin", False))

    if not username or not password:
        raise HTTPException(400, "username_and_password_required")
    if len(password) < 8:
        raise HTTPException(400, "password_too_short")

    clash = await db.execute(select(DashboardUser).where(func.lower(DashboardUser.login_username) == username.lower()))
    if clash.scalar_one_or_none():
        raise HTTPException(409, "username_taken")

    synthetic_id = await _next_local_id(db)
    db.add(DashboardUser(
        telegram_id=synthetic_id, first_name=first_name or username,
        login_username=username, password_hash=hash_password(password),
        is_admin=is_admin_flag, is_active=True,
    ))
    await db.commit()
    return {"ok": True, "telegram_id": synthetic_id, "username": username}


@app.delete("/api/users/{telegram_id}")
async def revoke_access(telegram_id: int, admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    user = await db.get(DashboardUser, telegram_id)
    if not user:
        raise HTTPException(404, "Not found")
    if user.telegram_id == admin.telegram_id:
        raise HTTPException(400, "Cannot revoke your own access")
    user.is_active = False
    await db.commit()
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# TOKENS
# ═══════════════════════════════════════════════════════

@app.get("/api/tokens")
async def list_tokens(admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AccessToken).order_by(AccessToken.created_at.desc()).limit(50))
    return [{"id": t.id, "token": t.token, "is_used": t.is_used, "used_by": t.used_by,
             "expires_at": t.expires_at.isoformat() if t.expires_at else None,
             "created_at": t.created_at.isoformat()}
            for t in result.scalars().all()]


@app.post("/api/tokens", status_code=201)
async def create_token(body: dict, admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    from datetime import timedelta
    expires_hours = int(body.get("expires_hours", 72))
    token_str = secrets.token_hex(12)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_hours) if expires_hours > 0 else None
    db.add(AccessToken(token=token_str, created_by=admin.telegram_id, expires_at=expires_at, is_used=False))
    await db.commit()
    return {"ok": True, "token": token_str, "expires_hours": expires_hours}


@app.delete("/api/tokens/{token_id}")
async def delete_token(token_id: int, admin: DashboardUser = Depends(require_admin), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AccessToken).where(AccessToken.id == token_id))
    token = result.scalar_one_or_none()
    if not token:
        raise HTTPException(404, "Not found")
    await db.delete(token)
    await db.commit()
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# ACCOUNTS
# ═══════════════════════════════════════════════════════

@app.get("/api/accounts")
async def list_accounts(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db),
                         all_users: bool = Query(default=False)):
    q = select(TelegramAccount)
    if not (user.is_admin and all_users):
        q = q.where(TelegramAccount.user_id == user.telegram_id)
    result = await db.execute(q)
    return [{"id": a.id, "account_type": a.account_type, "label": a.label,
             "identifier_hint": a.identifier[:6]+"***", "user_id": a.user_id,
             "meta_pixel_id": a.meta_pixel_id, "has_capi_token": bool(a.meta_capi_token),
             "proxy_set": bool(a.proxy_string), "is_active": a.is_active,
             "created_at": a.created_at.isoformat()}
            for a in result.scalars().all()]


@app.post("/api/accounts/bot", status_code=201)
async def add_bot(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    token = body.get("bot_token","").strip()
    if not token or ":" not in token:
        raise HTTPException(400, "Valid bot token required")
    db.add(TelegramAccount(user_id=user.telegram_id, account_type=AccountType.BOT,
        identifier=token, session_name=f"bot_{token.split(':')[0]}",
        meta_pixel_id=body.get("meta_pixel_id") or None,
        meta_capi_token=body.get("meta_capi_token") or None,
        label=body.get("label") or None, proxy_string=body.get("proxy_string") or None, is_active=True))
    await db.commit()
    return {"ok": True}


@app.patch("/api/accounts/{account_id}")
async def update_account(account_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    acct = await db.get(TelegramAccount, account_id)
    if not acct or acct.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    for field in ["label","meta_pixel_id","meta_capi_token","proxy_string","is_active"]:
        if field in body:
            setattr(acct, field, body[field] if body[field] != "" else None)
    if "is_active" in body:
        acct.is_active = bool(body["is_active"])
    await db.commit()
    return {"ok": True}


@app.delete("/api/accounts/{account_id}")
async def delete_account(account_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    acct = await db.get(TelegramAccount, account_id)
    if not acct or acct.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(acct)
    await db.commit()
    return {"ok": True}


@app.post("/api/accounts/personal/step1")
async def personal_step1(body: dict, user: DashboardUser = Depends(require_user)):
    phone = body.get("phone","").strip()
    if not phone:
        raise HTTPException(400, "phone required")
    flow_token = secrets.token_urlsafe(16)
    payload = {"action":"send_code","phone":phone,"user_id":user.telegram_id,
               "flow_token":flow_token,"status":"pending",
               "meta_pixel_id":body.get("meta_pixel_id",""),
               "meta_capi_token":body.get("meta_capi_token",""),
               "proxy_string":body.get("proxy_string")}
    await redis_client.setex(f"signin:{flow_token}", 600, json.dumps(payload))
    await redis_client.rpush("signin_queue", json.dumps(payload))
    return {"ok": True, "flow_token": flow_token}


@app.post("/api/accounts/personal/step2")
async def personal_step2(body: dict, user: DashboardUser = Depends(require_user)):
    import asyncio
    flow_token = body.get("flow_token","").strip()
    code = body.get("code","").strip()
    if not flow_token or not code:
        raise HTTPException(400, "flow_token and code required")
    raw = await redis_client.get(f"signin:{flow_token}")
    if not raw:
        raise HTTPException(404, "Session expired")
    flow_data = json.loads(raw)
    flow_data.update({"action":"verify_code","code":code})
    await redis_client.setex(f"signin:{flow_token}", 300, json.dumps(flow_data))
    await redis_client.rpush("signin_queue", json.dumps(flow_data))
    for _ in range(60):  # 60 seconds max wait
        await asyncio.sleep(1)
        updated = await redis_client.get(f"signin:{flow_token}")
        if updated:
            state = json.loads(updated)
            if state.get("status") == "completed":
                return {"ok": True, "account_id": state.get("account_id")}
            if state.get("status") == "error":
                raise HTTPException(400, state.get("error","Sign-in failed"))
    raise HTTPException(408, "Timed out — the code was accepted but account boot is slow. Try refreshing accounts in 30 seconds.")


@app.post("/api/accounts/{account_id}/test-pixel")
async def test_pixel(account_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from service_b.meta_capi import fire_conversion_event, _build_user_data
    acct = await db.get(TelegramAccount, account_id)
    if not acct or acct.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if not acct.meta_pixel_id or not acct.meta_capi_token:
        raise HTTPException(400, "Pixel ID and CAPI token must be set first")
    ud = _build_user_data(telegram_id=user.telegram_id, first_name=user.first_name)
    result = await fire_conversion_event(pixel_id=acct.meta_pixel_id, capi_token=acct.meta_capi_token,
        event_name="Lead", user_data=ud, test_event_code="TEST12345")
    if result.get("error"):
        return {"ok": False, "detail": result}
    return {"ok": True, "events_received": result.get("events_received", 0),
            "message": "Test event sent! Check Meta Events Manager → Test Events tab."}


# ═══════════════════════════════════════════════════════
# CAMPAIGNS
# ═══════════════════════════════════════════════════════

@app.get("/api/campaigns")
async def list_campaigns(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Campaign).where(Campaign.user_id == user.telegram_id).order_by(Campaign.created_at.desc())
    )
    return [{"id": c.id, "name": c.name, "slug": c.slug,
             "target_telegram_username": c.target_telegram_username,
             "event_type": c.event_type, "account_id": c.account_id,
             "is_active": c.is_active, "total_clicks": c.total_clicks,
             "total_conversions": c.total_conversions,
             "conversion_rate": round(c.total_conversions/c.total_clicks*100,1) if c.total_clicks else 0.0,
             "created_at": c.created_at.isoformat(),
             "tracking_url": f"{settings.BASE_URL}/t/{c.slug}"}
            for c in result.scalars().all()]


@app.post("/api/campaigns", status_code=201)
async def create_campaign(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    name = body.get("name","").strip()
    acct_id = body.get("account_id")
    target = body.get("target_telegram_username","").strip()
    event = body.get("event_type","Lead")
    custom_slug = body.get("slug","").strip()
    if not all([name, acct_id, target]):
        raise HTTPException(400, "name, account_id, target_telegram_username required")
    acct = await db.get(TelegramAccount, acct_id)
    if not acct or acct.user_id != user.telegram_id:
        raise HTTPException(404, "Account not found")
    slug = custom_slug or _gen_key(10)
    while (await db.execute(select(Campaign).where(Campaign.slug == slug))).scalar_one_or_none():
        slug = _gen_key(10)
    new_camp = Campaign(user_id=user.telegram_id, account_id=acct_id, name=name,
                    slug=slug, target_telegram_username=target,
                    event_type=EventType(event), is_active=True)
    db.add(new_camp)
    await db.commit()
    await db.refresh(new_camp)
    return {"ok": True, "id": new_camp.id, "slug": slug, "tracking_url": f"{settings.BASE_URL}/t/{slug}"}


@app.patch("/api/campaigns/{campaign_id}")
async def update_campaign(campaign_id: int, body: dict,
                           user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if "name"       in body: camp.name       = body["name"]
    if "is_active"  in body: camp.is_active  = bool(body["is_active"])
    if "event_type" in body: camp.event_type = EventType(body["event_type"])
    await db.commit()
    return {"ok": True}


@app.delete("/api/campaigns/{campaign_id}")
async def delete_campaign(campaign_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(camp)
    await db.commit()
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# CONVERSIONS
# ═══════════════════════════════════════════════════════

@app.get("/api/conversions")
async def list_conversions(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db),
                            campaign_id: int | None = Query(default=None),
                            event_type: str | None = Query(default=None),
                            trigger_type: str | None = Query(default=None),
                            limit: int = Query(default=100, le=500),
                            offset: int = Query(default=0)):
    q = (select(ConversionLog, Campaign.name.label("campaign_name"))
         .join(Campaign, ConversionLog.campaign_id == Campaign.id)
         .where(Campaign.user_id == user.telegram_id))
    if campaign_id:
        q = q.where(ConversionLog.campaign_id == campaign_id)
    if event_type:
        q = q.where(ConversionLog.event_type == event_type)
    if trigger_type:
        q = q.where(ConversionLog.trigger_type == trigger_type)
    q = q.order_by(ConversionLog.fired_at.desc()).limit(limit).offset(offset)
    rows = (await db.execute(q)).all()
    return [{"id": r.ConversionLog.id, "campaign_id": r.ConversionLog.campaign_id,
             "campaign_name": r.campaign_name, "account_id": r.ConversionLog.account_id,
             "trigger_type": r.ConversionLog.trigger_type,
             "telegram_user_id": r.ConversionLog.telegram_user_id,
             "telegram_username": r.ConversionLog.telegram_username,
             "fbclid": (r.ConversionLog.fbclid[:12]+"…") if r.ConversionLog.fbclid else None,
             "client_ip": r.ConversionLog.client_ip, "event_type": r.ConversionLog.event_type,
             "event_value": r.ConversionLog.event_value,
             "event_currency": r.ConversionLog.event_currency,
             "content_name": r.ConversionLog.content_name,
             "status": r.ConversionLog.status, "error_detail": r.ConversionLog.error_detail,
             "fired_at": r.ConversionLog.fired_at.isoformat()} for r in rows]


@app.get("/api/conversions/summary")
async def conversion_summary(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Campaign.name, Campaign.total_clicks, Campaign.total_conversions)
        .where(Campaign.user_id == user.telegram_id, Campaign.is_active == True)
        .order_by(Campaign.total_conversions.desc())
    )
    rows = result.all()
    total_clicks = sum(r.total_clicks for r in rows)
    total_convs  = sum(r.total_conversions for r in rows)
    return {"total_clicks": total_clicks, "total_conversions": total_convs,
            "overall_rate": round(total_convs/total_clicks*100,1) if total_clicks else 0.0,
            "by_campaign": [{"name": r.name, "clicks": r.total_clicks, "conversions": r.total_conversions,
                              "rate": round(r.total_conversions/r.total_clicks*100,1) if r.total_clicks else 0.0}
                             for r in rows]}


# ═══════════════════════════════════════════════════════
# MESSAGES — inbox
# ═══════════════════════════════════════════════════════

@app.get("/api/messages")
async def list_messages(
    user: DashboardUser = Depends(require_user),
    db: AsyncSession = Depends(get_db),
    unread_only: bool = Query(default=False),
    account_id: int | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
):
    q = (select(Message, TelegramAccount.label.label("account_label"),
                TelegramAccount.account_type.label("account_type"))
         .join(TelegramAccount, Message.account_id == TelegramAccount.id)
         .where(TelegramAccount.user_id == user.telegram_id))
    if unread_only:
        q = q.where(Message.is_read == False)
    if account_id:
        q = q.where(Message.account_id == account_id)
    q = q.order_by(Message.received_at.desc()).limit(limit).offset(offset)
    rows = (await db.execute(q)).all()
    return [{"id": r.Message.id, "account_id": r.Message.account_id,
             "account_label": r.account_label, "account_type": r.account_type,
             "tg_chat_id": r.Message.tg_chat_id, "tg_user_id": r.Message.tg_user_id,
             "tg_username": r.Message.tg_username, "tg_first_name": r.Message.tg_first_name,
             "direction": r.Message.direction, "text": r.Message.text,
             "is_read": r.Message.is_read, "campaign_id": r.Message.campaign_id,
             "received_at": r.Message.received_at.isoformat()} for r in rows]


@app.get("/api/messages/unread-count")
async def unread_count(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    q = (select(func.count())
         .select_from(Message)
         .join(TelegramAccount, Message.account_id == TelegramAccount.id)
         .where(TelegramAccount.user_id == user.telegram_id, Message.is_read == False,
                Message.direction == MessageDirection.inbound))
    count = (await db.execute(q)).scalar() or 0
    return {"unread": count}


@app.patch("/api/messages/{message_id}/read")
async def mark_read(message_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    msg = await db.get(Message, message_id)
    if not msg:
        raise HTTPException(404, "Not found")
    msg.is_read = True
    await db.commit()
    return {"ok": True}


@app.post("/api/messages/read-all")
async def mark_all_read(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    account_id = body.get("account_id")
    q = (update(Message)
         .where(Message.is_read == False)
         .values(is_read=True))
    if account_id:
        q = q.where(Message.account_id == account_id)
    await db.execute(q)
    await db.commit()
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# FUNNELS
# ═══════════════════════════════════════════════════════

@app.get("/api/funnels")
async def list_funnels(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db),
                        campaign_id: int | None = Query(default=None)):
    q = select(Funnel).where(Funnel.user_id == user.telegram_id)
    if campaign_id:
        q = q.where(Funnel.campaign_id == campaign_id)
    q = q.order_by(Funnel.created_at.desc())
    funnels = (await db.execute(q)).scalars().all()
    return [await _funnel_dict(db, f) for f in funnels]


async def _step_count(db: AsyncSession, trigger_id: int) -> int:
    result = await db.execute(
        select(func.count(func.distinct(ConversionLog.telegram_user_id)))
        .where(ConversionLog.trigger_id == trigger_id, ConversionLog.status == ConversionStatus.fired)
    )
    return result.scalar() or 0


def _step_dict(s: "FunnelStep", count: int) -> dict:
    t = s.trigger
    return {
        "id": s.id, "step_order": s.step_order,
        "label": s.label or (t.event_name if t else "Step"),
        "trigger_id": s.trigger_id,
        "trigger_type": t.trigger_type if t else None,
        "event_name": t.event_name if t else None,
        "keywords": t.keywords if t else None,
        "count": count,
    }


async def _funnel_dict(db: AsyncSession, f: "Funnel") -> dict:
    steps_r = await db.execute(
        select(FunnelStep).where(FunnelStep.funnel_id == f.id).order_by(FunnelStep.step_order)
    )
    steps = steps_r.scalars().all()
    step_data = []
    for s in steps:
        await db.refresh(s, ["trigger"])
        step_data.append(_step_dict(s, await _step_count(db, s.trigger_id)))
    return {"id": f.id, "campaign_id": f.campaign_id, "name": f.name,
            "description": f.description, "is_default": f.is_default,
            "is_active": f.is_active, "created_at": f.created_at.isoformat(),
            "steps": step_data}


@app.post("/api/campaigns/{campaign_id}/funnels/auto-generate", status_code=201)
async def auto_generate_funnel(campaign_id: int, user: DashboardUser = Depends(require_user),
                                db: AsyncSession = Depends(get_db)):
    """
    Builds a funnel automatically from the campaign's existing triggers,
    ordered the same way as the trigger config UI (trigger_order).
    Requires at least 2 active triggers configured first.

    Re-running this on a campaign that already has an auto-generated
    funnel REPLACES its steps to match the current trigger set. If the
    user has since hand-edited that funnel (is_default became False),
    this creates a NEW funnel instead of touching their edited one.
    """
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")

    triggers_r = await db.execute(
        select(ConversionTrigger)
        .where(ConversionTrigger.campaign_id == campaign_id, ConversionTrigger.is_active == True)
        .order_by(ConversionTrigger.trigger_order)
    )
    triggers = triggers_r.scalars().all()
    if len(triggers) < 2:
        raise HTTPException(400, "Campaign needs at least 2 active triggers before a funnel can be generated")

    existing_r = await db.execute(
        select(Funnel).where(Funnel.campaign_id == campaign_id, Funnel.is_default == True)
    )
    funnel = existing_r.scalar_one_or_none()

    if funnel:
        old_steps_r = await db.execute(select(FunnelStep).where(FunnelStep.funnel_id == funnel.id))
        for s in old_steps_r.scalars().all():
            await db.delete(s)
        await db.flush()
    else:
        funnel = Funnel(user_id=user.telegram_id, campaign_id=campaign_id,
                        name=f"{camp.name} — Funnel", is_default=True, is_active=True)
        db.add(funnel)
        await db.flush()

    for i, trig in enumerate(triggers):
        db.add(FunnelStep(funnel_id=funnel.id, trigger_id=trig.id, step_order=i + 1))

    await db.commit()
    await db.refresh(funnel)
    return await _funnel_dict(db, funnel)


@app.post("/api/funnels", status_code=201)
async def create_funnel(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    """
    Manually build a funnel by picking specific trigger IDs in order.
    campaign_id is required — a funnel tracks one campaign's journey.
    trigger_ids must each belong to a ConversionTrigger already
    configured on that campaign (create triggers first via
    POST /campaigns/{id}/triggers, then reference them here by id).
    """
    name = body.get("name", "").strip()
    campaign_id = body.get("campaign_id")
    trigger_ids = body.get("trigger_ids", [])

    if not name:
        raise HTTPException(400, "name required")
    if not campaign_id:
        raise HTTPException(400, "campaign_id required — a funnel tracks one campaign's journey")
    if len(trigger_ids) < 2:
        raise HTTPException(400, "A funnel needs at least 2 steps — pick 2+ triggers")

    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")

    triggers_r = await db.execute(
        select(ConversionTrigger).where(
            ConversionTrigger.id.in_(trigger_ids),
            ConversionTrigger.campaign_id == campaign_id,
        )
    )
    found = {t.id: t for t in triggers_r.scalars().all()}
    missing = [tid for tid in trigger_ids if tid not in found]
    if missing:
        raise HTTPException(400, f"Trigger ids not found on this campaign: {missing}")

    labels = body.get("labels") if isinstance(body.get("labels"), dict) else {}

    funnel = Funnel(user_id=user.telegram_id, campaign_id=campaign_id, name=name,
                    description=body.get("description", "") or None,
                    is_default=False, is_active=True)
    db.add(funnel)
    await db.flush()
    for i, tid in enumerate(trigger_ids):
        db.add(FunnelStep(funnel_id=funnel.id, trigger_id=tid, step_order=i + 1,
                          label=labels.get(str(tid))))
    await db.commit()
    return {"ok": True, "id": funnel.id, "funnel_id": funnel.id}


@app.patch("/api/funnels/{funnel_id}/steps")
async def update_funnel_steps(funnel_id: int, body: dict, user: DashboardUser = Depends(require_user),
                               db: AsyncSession = Depends(get_db)):
    """
    Replace a funnel's steps wholesale — used by drag-to-reorder /
    add-step / remove-step UI. Editing steps on an is_default funnel
    flips it to manual (is_default=False) so future auto-generate
    calls create a separate funnel instead of overwriting this edit.
    """
    funnel = await db.get(Funnel, funnel_id)
    if not funnel or funnel.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")

    trigger_ids = body.get("trigger_ids", [])
    if len(trigger_ids) < 2:
        raise HTTPException(400, "A funnel needs at least 2 steps")

    triggers_r = await db.execute(
        select(ConversionTrigger).where(
            ConversionTrigger.id.in_(trigger_ids),
            ConversionTrigger.campaign_id == funnel.campaign_id,
        )
    )
    found_ids = {t.id for t in triggers_r.scalars().all()}
    missing = [tid for tid in trigger_ids if tid not in found_ids]
    if missing:
        raise HTTPException(400, f"Trigger ids not found on this campaign: {missing}")

    old_steps_r = await db.execute(select(FunnelStep).where(FunnelStep.funnel_id == funnel_id))
    for s in old_steps_r.scalars().all():
        await db.delete(s)
    await db.flush()

    for i, tid in enumerate(trigger_ids):
        db.add(FunnelStep(funnel_id=funnel_id, trigger_id=tid, step_order=i + 1))

    funnel.is_default = False
    await db.commit()
    await db.refresh(funnel)
    return await _funnel_dict(db, funnel)


@app.delete("/api/funnels/{funnel_id}")
async def delete_funnel(funnel_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    funnel = await db.get(Funnel, funnel_id)
    if not funnel or funnel.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(funnel)
    await db.commit()
    return {"ok": True}


@app.get("/api/funnels/{funnel_id}/stats")
async def funnel_stats(funnel_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    funnel = await db.get(Funnel, funnel_id)
    if not funnel or funnel.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")

    steps_r = await db.execute(
        select(FunnelStep).where(FunnelStep.funnel_id == funnel_id).order_by(FunnelStep.step_order)
    )
    steps = steps_r.scalars().all()

    step_stats = []
    prev_count = None
    for s in steps:
        await db.refresh(s, ["trigger"])
        count = await _step_count(db, s.trigger_id)
        drop_off = round((1 - count / prev_count) * 100, 1) if prev_count and prev_count > 0 else None
        conv_rate = round(count / prev_count * 100, 1) if prev_count and prev_count > 0 else 100.0
        d = _step_dict(s, count)
        d["drop_off_pct"] = drop_off
        d["conv_rate"] = conv_rate
        step_stats.append(d)
        prev_count = count

    return {"funnel_id": funnel_id, "name": funnel.name, "campaign_id": funnel.campaign_id,
            "steps": step_stats}


@app.get("/api/clicks/pending")
async def pending_clicks(user: DashboardUser = Depends(require_user)):
    """
    Debug helper: shows clicks that are stored in Redis but haven't
    converted yet (user clicked the ad but hasn't opened Telegram).
    Useful for understanding "missing conversions" — these will either
    convert later or expire after REDIS_TTL_HOURS.
    """
    keys = []
    async for key in redis_client.scan_iter(match="click:*"):
        raw = await redis_client.get(key)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if payload.get("user_id") != user.telegram_id:
            continue
        ttl = await redis_client.ttl(key)
        keys.append({
            "short_key": key.replace("click:", ""),
            "campaign_id": payload.get("campaign_id"),
            "event_type": payload.get("event_type"),
            "clicked_at": payload.get("clicked_at"),
            "expires_in_seconds": ttl,
            "fbclid_present": bool(payload.get("fbclid")),
        })
    return {"pending": keys, "count": len(keys)}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Silence browser favicon 404 requests."""
    from fastapi.responses import Response
    # Minimal 1x1 pixel transparent ICO
    ico = b'\x00\x00\x01\x00\x01\x00\x01\x01\x00\x00\x01\x00\x18\x00\x28\x00\x00\x00\x16\x00\x00\x00(\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01\x00\x18\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1f\x6e\xff\x00\x00\x00\x00\x00'
    return Response(content=ico, media_type="image/x-icon")


@app.get("/api/bot-info")
async def bot_info():
    """
    Public, unauthenticated — returns the bot's username so the browser
    login page can render Telegram's Login Widget pointing at the right
    bot. A bot username is not a secret; it's the same string visible in
    the bot's public t.me/<username> link. Returns {"username": null}
    when no bot is configured — the dashboard then just hides the
    "Log in with Telegram" option and shows only username/password.
    """
    if not settings.MASTER_BOT_TOKEN:
        return {"username": None}
    try:
        import aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
            async with s.get(f"https://api.telegram.org/bot{settings.MASTER_BOT_TOKEN}/getMe") as resp:
                data = await resp.json()
                if data.get("ok"):
                    return {"username": data["result"]["username"]}
    except Exception as e:
        logger.warning("bot_info fetch failed: %s", e)
    return {"username": None}


@app.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    """
    Full system health check — open this in your browser to diagnose issues.
    Shows status of DB, Redis, WebSocket connections, and account counts.
    """
    result = {}

    # 1. PostgreSQL
    try:
        await db.execute(text("SELECT 1"))
        from sqlalchemy import func
        user_count = (await db.execute(select(func.count()).select_from(DashboardUser))).scalar()
        acct_count = (await db.execute(select(func.count()).select_from(TelegramAccount))).scalar()
        camp_count = (await db.execute(select(func.count()).select_from(Campaign))).scalar()
        conv_count = (await db.execute(select(func.count()).select_from(ConversionLog))).scalar()
        result["database"] = {
            "status": "connected",
            "users": user_count,
            "accounts": acct_count,
            "campaigns": camp_count,
            "conversions": conv_count,
        }
    except Exception as e:
        result["database"] = {"status": "ERROR", "error": str(e)}

    # 2. Redis
    try:
        await redis_client.ping()
        redis_keys = await redis_client.dbsize()
        click_keys = len([k async for k in redis_client.scan_iter("click:*")])
        result["redis"] = {
            "status": "connected",
            "total_keys": redis_keys,
            "pending_clicks": click_keys,
        }
    except Exception as e:
        result["redis"] = {"status": "ERROR", "error": str(e)}

    # 3. WebSocket connections
    result["websocket"] = {
        "active_connections": ws_manager.total,
    }

    # 4. Config sanity
    result["config"] = {
        "base_url": settings.BASE_URL,
        "is_production": settings.BASE_URL.startswith("https://"),
        "admin_id_set": bool(settings.ADMIN_TELEGRAM_ID),
        "bot_token_set": bool(settings.MASTER_BOT_TOKEN),
        "tg_api_set": bool(settings.TELEGRAM_API_ID),
        "local_login_configured": bool(settings.ADMIN_USERNAME and settings.ADMIN_PASSWORD),
    }

    all_ok = (
        result["database"].get("status") == "connected" and
        result["redis"].get("status") == "connected"
    )
    result["status"] = "ok" if all_ok else "degraded"
    result["version"] = "4.0.0"
    return result


# ═══════════════════════════════════════════════════════
# CONVERSION TRIGGERS
# ═══════════════════════════════════════════════════════

@app.get("/api/campaigns/{campaign_id}/triggers")
async def list_triggers(campaign_id: int, user: DashboardUser = Depends(require_user),
                         db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")
    result = await db.execute(
        select(ConversionTrigger)
        .where(ConversionTrigger.campaign_id == campaign_id)
        .order_by(ConversionTrigger.trigger_order)
    )
    return [_trigger_dict(t) for t in result.scalars().all()]


@app.post("/api/campaigns/{campaign_id}/triggers", status_code=201)
async def add_trigger(campaign_id: int, body: dict,
                       user: DashboardUser = Depends(require_user),
                       db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")
    trigger_type = body.get("trigger_type", "bot_start")
    event_name   = (body.get("event_name") or "").strip() or "Lead"
    match_mode   = body.get("match_mode") or "any"
    if match_mode not in ("any", "all"):
        raise HTTPException(400, "match_mode must be 'any' or 'all'")
    if not trigger_type or not event_name:
        raise HTTPException(400, "trigger_type and event_name required")
    # Get next order
    count_r = await db.execute(
        select(func.count()).select_from(ConversionTrigger)
        .where(ConversionTrigger.campaign_id == campaign_id)
    )
    order = (count_r.scalar() or 0)
    db.add(ConversionTrigger(
        campaign_id=campaign_id,
        trigger_order=order,
        trigger_type=trigger_type,
        event_name=event_name,
        keywords=body.get("keywords") or None,
        match_mode=match_mode,
        value=body.get("value") or None,
        currency=body.get("currency") or None,
        content_name=body.get("content_name") or None,
        content_ids=body.get("content_ids") or None,
        custom_data_json=json.dumps(body["custom_data"]) if body.get("custom_data") else None,
        is_active=True,
    ))
    await db.commit()
    return {"ok": True}


@app.patch("/api/triggers/{trigger_id}")
async def update_trigger(trigger_id: int, body: dict,
                          user: DashboardUser = Depends(require_user),
                          db: AsyncSession = Depends(get_db)):
    t = await db.get(ConversionTrigger, trigger_id)
    if not t:
        raise HTTPException(404, "Not found")
    camp = await db.get(Campaign, t.campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(403, "Not yours")
    for field in ["event_name","trigger_type","keywords","match_mode","value","currency",
                  "content_name","content_ids","is_active"]:
        if field in body:
            val = body[field]
            if field == "match_mode" and val not in ("any", "all"):
                raise HTTPException(400, "match_mode must be 'any' or 'all'")
            if val == "" or val is None:
                val = None
            setattr(t, field, val)
    if "custom_data" in body:
        t.custom_data_json = json.dumps(body["custom_data"]) if body["custom_data"] else None
    await db.commit()
    return {"ok": True}


@app.delete("/api/triggers/{trigger_id}")
async def delete_trigger(trigger_id: int, user: DashboardUser = Depends(require_user),
                          db: AsyncSession = Depends(get_db)):
    t = await db.get(ConversionTrigger, trigger_id)
    if not t:
        raise HTTPException(404, "Not found")
    camp = await db.get(Campaign, t.campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(403, "Not yours")
    await db.delete(t)
    await db.commit()
    return {"ok": True}


def _trigger_dict(t: ConversionTrigger) -> dict:
    return {
        "id": t.id, "campaign_id": t.campaign_id,
        "trigger_order": t.trigger_order,
        "trigger_type": t.trigger_type,
        "event_name": t.event_name,
        "keywords": t.keywords,
        "match_mode": t.match_mode,
        "value": t.value, "currency": t.currency,
        "content_name": t.content_name,
        "content_ids": t.content_ids,
        "custom_data": json.loads(t.custom_data_json) if t.custom_data_json else None,
        "is_active": t.is_active,
    }


# ── Campaign templates ────────────────────────────────────────────────

CAMPAIGN_TEMPLATES = {
    "lead_gen": {
        "name": "Lead Generation",
        "description": "Ad click → bot /start fires Lead",
        "triggers": [
            {"trigger_type": "bot_start", "event_name": "Lead",
             "trigger_order": 0},
        ],
    },
    "chat_qualified": {
        "name": "Chat Qualified Lead",
        "description": "Ad click → /start fires Lead → first DM fires Contact",
        "triggers": [
            {"trigger_type": "bot_start",     "event_name": "Lead",    "trigger_order": 0},
            {"trigger_type": "first_message", "event_name": "Contact", "trigger_order": 1},
        ],
    },
    "purchase": {
        "name": "Purchase Tracking",
        "description": "Ad click → /start fires Lead → keyword fires Purchase",
        "triggers": [
            {"trigger_type": "bot_start", "event_name": "Lead",     "trigger_order": 0},
            {"trigger_type": "keyword",   "event_name": "Purchase", "trigger_order": 1,
             "keywords": "paid,bought,payment done,order confirmed,purchased"},
        ],
    },
    "channel_growth": {
        "name": "Channel Growth",
        "description": "Ad click → join request fires CompleteRegistration",
        "triggers": [
            {"trigger_type": "channel_join", "event_name": "CompleteRegistration",
             "trigger_order": 0},
        ],
    },
    "full_funnel": {
        "name": "Full Funnel",
        "description": "Every step tracked: click → start → chat → purchase",
        "triggers": [
            {"trigger_type": "bot_start",     "event_name": "Lead",                 "trigger_order": 0},
            {"trigger_type": "first_message", "event_name": "Contact",              "trigger_order": 1},
            {"trigger_type": "keyword",       "event_name": "InitiateCheckout",     "trigger_order": 2,
             "keywords": "interested,want,how much,price,cost,buy"},
            {"trigger_type": "keyword",       "event_name": "Purchase",             "trigger_order": 3,
             "keywords": "paid,bought,done,order,payment,purchased,confirmed"},
        ],
    },
}


@app.get("/api/campaign-templates")
async def list_templates(user: DashboardUser = Depends(require_user)):
    return [{"id": k, **v} for k, v in CAMPAIGN_TEMPLATES.items()]


@app.post("/api/campaigns/{campaign_id}/apply-template")
async def apply_template(campaign_id: int, body: dict,
                          user: DashboardUser = Depends(require_user),
                          db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")
    template_id = body.get("template_id")
    if template_id not in CAMPAIGN_TEMPLATES:
        raise HTTPException(400, f"Unknown template. Available: {list(CAMPAIGN_TEMPLATES.keys())}")
    tpl = CAMPAIGN_TEMPLATES[template_id]
    # Remove existing triggers first
    result = await db.execute(
        select(ConversionTrigger).where(ConversionTrigger.campaign_id == campaign_id)
    )
    for existing in result.scalars().all():
        await db.delete(existing)
    # Apply template triggers
    for t in tpl["triggers"]:
        db.add(ConversionTrigger(
            campaign_id=campaign_id,
            trigger_type=t["trigger_type"],
            event_name=t["event_name"],
            trigger_order=t.get("trigger_order", 0),
            keywords=t.get("keywords"),
            value=t.get("value"),
            currency=t.get("currency"),
            content_name=t.get("content_name"),
            is_active=True,
        ))
    await db.commit()

    funnel_info = None
    if len(tpl["triggers"]) >= 2:
        # Auto-build the default funnel from the template's triggers so
        # the user sees a working funnel immediately. They can still
        # change which exact keywords trigger which step afterward via
        # the campaign's trigger config — editing a trigger's keywords
        # doesn't touch the funnel, since steps reference the trigger
        # by id, not by a snapshot of its settings.
        triggers_r = await db.execute(
            select(ConversionTrigger)
            .where(ConversionTrigger.campaign_id == campaign_id, ConversionTrigger.is_active == True)
            .order_by(ConversionTrigger.trigger_order)
        )
        triggers = triggers_r.scalars().all()
        funnel = Funnel(user_id=user.telegram_id, campaign_id=campaign_id,
                        name=f"{camp.name} — Funnel", is_default=True, is_active=True)
        db.add(funnel)
        await db.flush()
        for i, trig in enumerate(triggers):
            db.add(FunnelStep(funnel_id=funnel.id, trigger_id=trig.id, step_order=i + 1))
        await db.commit()
        funnel_info = {"funnel_id": funnel.id, "name": funnel.name}

    return {"ok": True, "template": tpl["name"], "triggers_applied": len(tpl["triggers"]),
            "funnel": funnel_info}


# ═══════════════════════════════════════════════════════
# MANUAL CONVERSION FIRING
# ═══════════════════════════════════════════════════════

@app.post("/api/conversions/manual")
async def fire_manual_conversion(body: dict,
                                  user: DashboardUser = Depends(require_user),
                                  db: AsyncSession = Depends(get_db)):
    """
    Manually fire a CAPI conversion from the dashboard.
    For phone/cash sales, offline deals, or anything automated can't catch.
    """
    from service_b.meta_capi import fire_event, build_user_data
    campaign_id  = body.get("campaign_id")
    tg_user_id   = body.get("telegram_user_id")
    tg_username  = body.get("telegram_username", "")
    tg_phone     = body.get("phone", "")
    event_name   = body.get("event_name", "Lead")
    value        = body.get("value")
    currency     = body.get("currency", "USD")
    content_name = body.get("content_name")
    order_id     = body.get("order_id")

    if not campaign_id:
        raise HTTPException(400, "campaign_id required")
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Campaign not found")
    acct = await db.get(TelegramAccount, camp.account_id)
    if not acct:
        raise HTTPException(404, "Account not found")

    ud = build_user_data(
        telegram_id=tg_user_id,
        username=tg_username.lstrip("@") if tg_username else None,
        phone=tg_phone or None,
    )

    result = await fire_event(
        pixel_id=acct.meta_pixel_id or "",
        capi_token=acct.meta_capi_token or "",
        event_name=event_name,
        user_data=ud,
        value=float(value) if value else None,
        currency=currency,
        content_name=content_name,
        order_id=order_id,
    )

    status = "error" if result.get("error") else "fired"
    db.add(ConversionLog(
        campaign_id=campaign_id, account_id=camp.account_id,
        trigger_type="manual",
        telegram_user_id=tg_user_id,
        telegram_username=tg_username.lstrip("@") if tg_username else None,
        event_type=event_name,
        event_value=float(value) if value else None,
        event_currency=currency,
        content_name=content_name,
        status=ConversionStatus(status),
        error_detail=str(result.get("error")) if result.get("error") else None,
        meta_event_id=result.get("event_id"),
        fbtrace_id=result.get("fbtrace_id"),
        fired_at=datetime.now(timezone.utc),
    ))
    if status == "fired":
        await db.execute(update(Campaign).where(Campaign.id == campaign_id)
                         .values(total_conversions=Campaign.total_conversions + 1))
    await db.commit()

    # Notify dashboard
    await redis_client.publish("tg_notifications", json.dumps({
        "target_user_id": user.telegram_id,
        "type": "conversion",
        "title": f"✅ Manual {event_name}",
        "body": f"{tg_username or tg_user_id or 'Unknown'} — fired manually",
        "data": {"campaign_id": campaign_id, "event_type": event_name},
        "ts": datetime.now(timezone.utc).isoformat(),
    }))

    return {"ok": True, "status": status, "event_id": result.get("event_id"),
            "events_received": result.get("events_received", 0),
            "error": result.get("error")}


# ═══════════════════════════════════════════════════════
# WEBHOOK TOKENS (inbound S2S from external systems)
# ═══════════════════════════════════════════════════════

@app.get("/api/campaigns/{campaign_id}/webhooks")
async def list_webhooks(campaign_id: int, user: DashboardUser = Depends(require_user),
                         db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    result = await db.execute(
        select(WebhookToken).where(WebhookToken.campaign_id == campaign_id)
    )
    return [{"id": w.id, "token": w.token, "label": w.label,
             "is_active": w.is_active, "hit_count": w.hit_count,
             "webhook_url": f"{settings.BASE_URL}/webhook/{w.token}",
             "last_hit_at": w.last_hit_at.isoformat() if w.last_hit_at else None}
            for w in result.scalars().all()]


@app.post("/api/campaigns/{campaign_id}/webhooks", status_code=201)
async def create_webhook(campaign_id: int, body: dict,
                          user: DashboardUser = Depends(require_user),
                          db: AsyncSession = Depends(get_db)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    token_str = secrets.token_urlsafe(24)
    db.add(WebhookToken(campaign_id=campaign_id, token=token_str,
                         label=body.get("label") or None, is_active=True))
    await db.commit()
    return {"ok": True, "token": token_str,
            "webhook_url": f"{settings.BASE_URL}/webhook/{token_str}"}


@app.delete("/api/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: int, user: DashboardUser = Depends(require_user),
                          db: AsyncSession = Depends(get_db)):
    w = await db.get(WebhookToken, webhook_id)
    if not w:
        raise HTTPException(404, "Not found")
    camp = await db.get(Campaign, w.campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(403, "Not yours")
    await db.delete(w)
    await db.commit()
    return {"ok": True}


# ── Inbound webhook endpoint ──────────────────────────────────────────

@app.post("/webhook/{token}")
async def inbound_webhook(token: str, request: Request, db: AsyncSession = Depends(get_db)):
    """
    External systems (ClickFunnels, payment gateways, CRMs) POST here
    to fire a server-side conversion.

    Expected body (all optional except the conversion is useless without
    at least a telegram_user_id or phone):
    {
        "telegram_user_id": 12345678,
        "telegram_username": "@username",
        "phone": "+8801XXXXXXXXX",
        "event_name": "Purchase",       // overrides campaign default
        "value": 49.99,
        "currency": "USD",
        "content_name": "Premium Plan",
        "order_id": "ORD-12345",
        "fbclid": "xxxxx"               // if passed through from landing page
    }
    """
    from service_b.meta_capi import fire_event, build_user_data
    result_wt = await db.execute(
        select(WebhookToken).where(WebhookToken.token == token, WebhookToken.is_active == True)
    )
    wt = result_wt.scalar_one_or_none()
    if not wt:
        raise HTTPException(404, "Webhook not found")

    camp = await db.get(Campaign, wt.campaign_id)
    if not camp or not camp.is_active:
        raise HTTPException(400, "Campaign inactive")
    acct = await db.get(TelegramAccount, camp.account_id)
    if not acct:
        raise HTTPException(400, "Account not configured")

    body = await request.json()
    tg_user_id   = body.get("telegram_user_id")
    tg_username  = body.get("telegram_username", "")
    phone        = body.get("phone", "")
    event_name   = body.get("event_name") or camp.event_type
    value        = body.get("value")
    currency     = body.get("currency", "USD")
    content_name = body.get("content_name")
    order_id     = body.get("order_id")
    fbclid       = body.get("fbclid", "")
    client_ip    = request.headers.get("X-Forwarded-For", request.client.host).split(",")[0].strip()

    ud = build_user_data(
        telegram_id=tg_user_id,
        username=tg_username.lstrip("@") if tg_username else None,
        phone=phone or None,
        client_ip=client_ip,
        fbclid=fbclid or None,
    )

    fire_result = await fire_event(
        pixel_id=acct.meta_pixel_id or "",
        capi_token=acct.meta_capi_token or "",
        event_name=event_name,
        user_data=ud,
        value=float(value) if value else None,
        currency=currency,
        content_name=content_name,
        order_id=order_id,
    )

    status = "error" if fire_result.get("error") else "fired"
    db.add(ConversionLog(
        campaign_id=wt.campaign_id, account_id=camp.account_id,
        trigger_type="webhook",
        telegram_user_id=tg_user_id,
        telegram_username=tg_username.lstrip("@") if tg_username else None,
        fbclid=fbclid,
        client_ip=client_ip,
        event_type=event_name,
        event_value=float(value) if value else None,
        event_currency=currency,
        content_name=content_name,
        status=ConversionStatus(status),
        error_detail=str(fire_result.get("error")) if fire_result.get("error") else None,
        meta_event_id=fire_result.get("event_id"),
        fbtrace_id=fire_result.get("fbtrace_id"),
        fired_at=datetime.now(timezone.utc),
    ))
    if status == "fired":
        await db.execute(update(Campaign).where(Campaign.id == wt.campaign_id)
                         .values(total_conversions=Campaign.total_conversions + 1))
    # Update webhook stats
    await db.execute(update(WebhookToken).where(WebhookToken.id == wt.id)
                     .values(hit_count=WebhookToken.hit_count + 1,
                             last_hit_at=datetime.now(timezone.utc)))
    await db.commit()

    # Real-time notify
    await redis_client.publish("tg_notifications", json.dumps({
        "target_user_id": camp.user_id,
        "type": "conversion",
        "title": f"✅ Webhook {event_name}",
        "body": f"{tg_username or tg_user_id or 'External'} — {wt.label or 'webhook'}",
        "data": {"campaign_id": wt.campaign_id, "event_type": event_name, "trigger": "webhook"},
        "ts": datetime.now(timezone.utc).isoformat(),
    }))

    return {"ok": True, "status": status, "event_id": fire_result.get("event_id")}


# ═══════════════════════════════════════════════════════
# CHANNEL ACCOUNTS
# ═══════════════════════════════════════════════════════

@app.post("/api/accounts/channel", status_code=201)
async def add_channel(body: dict, user: DashboardUser = Depends(require_user),
                       db: AsyncSession = Depends(get_db)):
    """
    Add a channel or group for member-join tracking.
    identifier = @channelusername or https://t.me/... invite link
    monitor_account_id = ID of a connected personal account that will
                         watch this channel for new members.
    """
    identifier = body.get("identifier", "").strip()
    monitor_id = body.get("monitor_account_id")
    if not identifier:
        raise HTTPException(400, "identifier (channel @username or invite link) required")
    # Verify the monitor account belongs to this user
    if monitor_id:
        mon_acct = await db.get(TelegramAccount, int(monitor_id))
        if not mon_acct or mon_acct.user_id != user.telegram_id:
            raise HTTPException(404, "Monitor account not found")
        if mon_acct.account_type not in (AccountType.PERSONAL, AccountType.BOT):
            raise HTTPException(400, "Monitor account must be a BOT or PERSONAL account")
    db.add(TelegramAccount(
        user_id=user.telegram_id,
        account_type=AccountType.CHANNEL,
        identifier=identifier,
        session_name=None,
        meta_pixel_id=body.get("meta_pixel_id") or None,
        meta_capi_token=body.get("meta_capi_token") or None,
        label=body.get("label") or identifier,
        monitor_account_id=int(monitor_id) if monitor_id else None,
        is_active=True,
    ))
    await db.commit()
    return {"ok": True}


# ═══════════════════════════════════════════════════════
# USER SESSIONS (for debugging attribution)
# ═══════════════════════════════════════════════════════

@app.get("/api/campaigns/{campaign_id}/sessions")
async def list_sessions(campaign_id: int, user: DashboardUser = Depends(require_user),
                         db: AsyncSession = Depends(get_db),
                         limit: int = Query(default=50, le=200)):
    camp = await db.get(Campaign, campaign_id)
    if not camp or camp.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    result = await db.execute(
        select(UserSession)
        .where(UserSession.campaign_id == campaign_id)
        .order_by(UserSession.last_seen_at.desc())
        .limit(limit)
    )
    return [{"id": s.id, "tg_user_id": s.tg_user_id,
             "tg_username": s.tg_username, "tg_first_name": s.tg_first_name,
             "has_phone": bool(s.tg_phone), "has_fbclid": bool(s.fbclid),
             "fired_triggers": s.fired_triggers.split(",") if s.fired_triggers else [],
             "first_seen_at": s.first_seen_at.isoformat(),
             "last_seen_at": s.last_seen_at.isoformat()}
            for s in result.scalars().all()]

# ═══════════════════════════════════════════════════════
# V5 EXTENSIONS — Tracking domains / links / clicks / identities / events
# ═══════════════════════════════════════════════════════

# ── Tracking aliases already handled in replaced capture_click ──
# Additional alias routes

@app.get("/c/{code}")
async def capture_click_alias(code: str, request: Request, db: AsyncSession = Depends(get_db)):
    # Extract query params manually to avoid FastAPI Query injection issue when calling capture_click directly
    qp = request.query_params
    return await capture_click(
        code, request,
        fbclid=qp.get("fbclid"),
        fbc=qp.get("fbc"),
        fbp=qp.get("fbp"),
        campaign=qp.get("campaign"),
        campaign_id=qp.get("campaign_id"),
        adset=qp.get("adset"),
        adset_id=qp.get("adset_id"),
        ad=qp.get("ad"),
        ad_id=qp.get("ad_id"),
        creative=qp.get("creative"),
        placement=qp.get("placement"),
        sub1=qp.get("sub1"),
        sub2=qp.get("sub2"),
        sub3=qp.get("sub3"),
        sub4=qp.get("sub4"),
        sub5=qp.get("sub5"),
        sub6=qp.get("sub6"),
        sub7=qp.get("sub7"),
        sub8=qp.get("sub8"),
        sub9=qp.get("sub9"),
        db=db
    )

@app.get("/b/{token}")
async def bridge_redirect(token: str, request: Request, db: AsyncSession = Depends(get_db)):
    from shared.models import Click
    click_public_id = token
    payload = None
    try:
        data = verify_token(token, settings.SECRET_KEY, purpose="click")
        click_public_id = data.get("cid") or token
    except TokenError:
        pass
    try:
        raw = await redis_client.get(f"click:{click_public_id}")
        if raw:
            payload = json.loads(raw)
    except Exception:
        pass
    if not payload:
        r = await db.execute(select(Click).where(Click.click_id == click_public_id))
        click = r.scalar_one_or_none()
        if not click:
            raise HTTPException(404, "Link expired or not found")
        payload = {"campaign_id": click.campaign_id, "click_id": click.id}
        if click.campaign_id:
            camp = await db.get(Campaign, click.campaign_id)
            if camp:
                payload["target_username"] = camp.target_telegram_username
    target = payload.get("target_username", "").lstrip("@") if payload.get("target_username") else ""
    if not target:
        cid = payload.get("campaign_id")
        if cid:
            camp = await db.get(Campaign, cid)
            if camp:
                target = camp.target_telegram_username.lstrip("@")
    if not target:
        # try tracking_link
        from shared.models import TrackingLink
        if payload.get("tracking_link_id"):
            link = await db.get(TrackingLink, payload["tracking_link_id"])
            if link and "t.me/" in link.destination:
                target = link.destination.split("t.me/")[-1].split("?")[0].split("/")[0].lstrip("@")
    if not target:
        raise HTTPException(404, "Destination not found")
    return RedirectResponse(url=f"https://t.me/{target}?start={click_public_id}", status_code=302)

@app.get("/l/{slug}")
async def landing_page(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    from fastapi.responses import HTMLResponse
    from shared.models import TrackingLink, Click, MetaPixel
    camp = None
    link = None
    r = await db.execute(select(Campaign).where(Campaign.slug == slug, Campaign.is_active == True))
    camp = r.scalar_one_or_none()
    if not camp:
        r2 = await db.execute(select(TrackingLink).where(TrackingLink.slug == slug, TrackingLink.is_active == True))
        link = r2.scalar_one_or_none()
        if not link:
            raise HTTPException(404, "Not found")
        if link.campaign_id:
            camp = await db.get(Campaign, link.campaign_id)
    pixel_id = None
    if camp:
        acct = await db.get(TelegramAccount, camp.account_id) if camp.account_id else None
        pixel_id = acct.meta_pixel_id if acct and acct.meta_pixel_id else None
        if not pixel_id:
            rp = await db.execute(select(MetaPixel).where(MetaPixel.user_id == camp.user_id, MetaPixel.is_active == True).limit(1))
            mp = rp.scalar_one_or_none()
            if mp:
                pixel_id = mp.pixel_id
    elif link:
        rp = await db.execute(select(MetaPixel).where(MetaPixel.user_id == link.user_id, MetaPixel.is_active == True).limit(1))
        mp = rp.scalar_one_or_none()
        if mp:
            pixel_id = mp.pixel_id
    query = dict(request.query_params)
    fbclid = query.get("fbclid")
    fbc = query.get("fbc")
    fbp = query.get("fbp") or request.cookies.get("_fbp")
    short = _gen_key(10)
    click_event_id = short
    try:
        from shared.tracking import create_click as tc
        params = {k: query.get(k) for k in ["fbclid","fbc","fbp","campaign","adset","ad","creative","placement","sub1","sub2","sub3","sub4","sub5","sub6","sub7","sub8","sub9"]}
        params["fbclid"] = fbclid
        params["fbc"] = fbc
        params["fbp"] = fbp
        rd = {"ip": request.client.host if request.client else "", "user_agent": request.headers.get("User-Agent",""), "referrer": request.headers.get("Referer"), "landing_page": str(request.url)}
        if camp or link:
            click = await tc(click_id=short, campaign_id=camp.id if camp else (link.campaign_id if link else None), tracking_link_id=link.id if link else None, domain_id=link.domain_id if link else None, params=params, request_data=rd, event_source_url=str(request.url))
            click_event_id = click.event_id
    except Exception as e:
        logger.warning("landing click persist failed: %s", e)
    target = ""
    if camp:
        target = camp.target_telegram_username
    elif link:
        target = link.destination
    if target.startswith("@"):
        target = target[1:]
    if "t.me/" in target:
        target = target.split("t.me/")[-1].split("?")[0].split("/")[0]
    target = target.lstrip("@")
    tg_link = f"https://t.me/{target}?start={short}" if target else "#"
    pixel_block = ""
    if pixel_id and settings.ENABLE_BROWSER_PIXEL:
        pixel_js = f"""!function(f,b,e,v,n,t,s){{if(f.fbq)return;n=f.fbq=function(){{n.callMethod?n.callMethod.apply(n,arguments):n.queue.push(arguments)}};if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}}(window, document,'script','https://connect.facebook.net/en_US/fbevents.js');fbq('init', '{pixel_id}');fbq('track', 'PageView', {{}}, {{eventID: '{click_event_id}'}});"""
        pixel_block = f'<script>{pixel_js}</script><noscript><img height="1" width="1" style="display:none" src="https://www.facebook.com/tr?id={pixel_id}&ev=PageView&noscript=1" /></noscript>'
    html = f"""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Continue to Telegram</title>{pixel_block}</head><body style="font-family:system-ui;background:#0a0a0f;color:#f0f0ff;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0"><div style="text-align:center;max-width:480px;padding:24px"><h2 style="margin:0 0 12px">Continue to Telegram</h2><p style="color:#8888aa;margin:0 0 20px">You clicked an ad — tap below to open Telegram.</p><a href="{tg_link}" style="display:inline-block;background:#4f8ef7;color:#fff;padding:12px 28px;border-radius:10px;text-decoration:none;font-weight:600">Open Telegram</a><p style="margin-top:16px;font-size:12px;color:#555570;word-break:break-all">{tg_link}</p></div></body></html>"""
    return HTMLResponse(html)

# ── Tracking domains ──

@app.get("/api/tracking-domains")
async def list_tracking_domains(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingDomain
    r = await db.execute(select(TrackingDomain).where(TrackingDomain.user_id == user.telegram_id).order_by(TrackingDomain.created_at.desc()))
    return [{"id": d.id, "domain": d.domain, "is_active": d.is_active, "is_verified": d.is_verified, "created_at": d.created_at.isoformat()} for d in r.scalars().all()]

@app.post("/api/tracking-domains", status_code=201)
async def create_tracking_domain(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingDomain
    domain = (body.get("domain") or "").strip().lower()
    if not domain or "." not in domain:
        raise HTTPException(400, "Valid domain required (e.g. track.example.com)")
    # basic SSRF/validation: no private IPs, no localhost
    if domain in ("localhost", "127.0.0.1") or domain.startswith("10.") or domain.startswith("192.168."):
        raise HTTPException(400, "Private/local domains not allowed")
    existing = await db.execute(select(TrackingDomain).where(TrackingDomain.user_id == user.telegram_id, TrackingDomain.domain == domain))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Domain already exists")
    d = TrackingDomain(user_id=user.telegram_id, domain=domain, is_active=True)
    db.add(d)
    await db.commit()
    await db.refresh(d)
    return {"ok": True, "id": d.id, "domain": d.domain}

@app.patch("/api/tracking-domains/{domain_id}")
async def update_tracking_domain(domain_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingDomain
    d = await db.get(TrackingDomain, domain_id)
    if not d or d.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if "is_active" in body:
        d.is_active = bool(body["is_active"])
    if "is_verified" in body and user.is_admin:
        d.is_verified = bool(body["is_verified"])
    await db.commit()
    return {"ok": True}

@app.delete("/api/tracking-domains/{domain_id}")
async def delete_tracking_domain(domain_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingDomain
    d = await db.get(TrackingDomain, domain_id)
    if not d or d.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(d)
    await db.commit()
    return {"ok": True}

# ── Tracking links ──

@app.get("/api/tracking-links")
async def list_tracking_links(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), campaign_id: int | None = Query(default=None), domain_id: int | None = Query(default=None)):
    from shared.models import TrackingLink
    q = select(TrackingLink).where(TrackingLink.user_id == user.telegram_id)
    if campaign_id:
        q = q.where(TrackingLink.campaign_id == campaign_id)
    if domain_id:
        q = q.where(TrackingLink.domain_id == domain_id)
    q = q.order_by(TrackingLink.created_at.desc()).limit(200)
    r = await db.execute(q)
    links = r.scalars().all()
    # enrich with domain
    out = []
    for l in links:
        domain_str = ""
        if l.domain_id:
            from shared.models import TrackingDomain
            dom = await db.get(TrackingDomain, l.domain_id)
            if dom:
                domain_str = dom.domain
        url = f"https://{domain_str}/c/{l.slug}" if domain_str else f"{settings.BASE_URL}/c/{l.slug}"
        out.append({"id": l.id, "slug": l.slug, "campaign_id": l.campaign_id, "domain_id": l.domain_id, "domain": domain_str, "destination": l.destination, "destination_type": l.destination_type, "is_active": l.is_active, "total_clicks": l.total_clicks, "label": l.label, "url": url, "created_at": l.created_at.isoformat()})
    return out

@app.post("/api/tracking-links", status_code=201)
async def create_tracking_link(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingLink, TrackingDomain
    destination = (body.get("destination") or "").strip()
    if not destination:
        raise HTTPException(400, "destination required (e.g. @mybot or https://t.me/mybot or https://t.me/mybot?start=...)")
    dest_type = body.get("destination_type") or "bot"
    try:
        dest_type_enum = ClickDestinationType(dest_type)
    except ValueError:
        dest_type_enum = ClickDestinationType.bot
    campaign_id = body.get("campaign_id")
    if campaign_id:
        camp = await db.get(Campaign, campaign_id)
        if not camp or camp.user_id != user.telegram_id:
            raise HTTPException(404, "Campaign not found")
    domain_id = body.get("domain_id")
    if domain_id:
        dom = await db.get(TrackingDomain, domain_id)
        if not dom or dom.user_id != user.telegram_id:
            raise HTTPException(404, "Domain not found")
    slug = (body.get("slug") or "").strip() or _gen_key(8)
    # ensure unique
    while (await db.execute(select(TrackingLink).where(TrackingLink.slug == slug))).scalar_one_or_none():
        slug = _gen_key(8)
    link = TrackingLink(user_id=user.telegram_id, campaign_id=campaign_id, domain_id=domain_id, slug=slug, destination=destination, destination_type=dest_type_enum, label=body.get("label"), is_active=True)
    db.add(link)
    await db.commit()
    await db.refresh(link)
    domain_str = ""
    if domain_id:
        dom = await db.get(TrackingDomain, domain_id)
        if dom:
            domain_str = dom.domain
    url = f"https://{domain_str}/c/{slug}" if domain_str else f"{settings.BASE_URL}/c/{slug}"
    return {"ok": True, "id": link.id, "slug": slug, "url": url}

@app.patch("/api/tracking-links/{link_id}")
async def update_tracking_link(link_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingLink
    link = await db.get(TrackingLink, link_id)
    if not link or link.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if "destination" in body:
        link.destination = body["destination"]
    if "is_active" in body:
        link.is_active = bool(body["is_active"])
    if "label" in body:
        link.label = body["label"]
    if "destination_type" in body:
        try:
            link.destination_type = ClickDestinationType(body["destination_type"])
        except ValueError:
            pass
    await db.commit()
    return {"ok": True}

@app.delete("/api/tracking-links/{link_id}")
async def delete_tracking_link(link_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TrackingLink
    link = await db.get(TrackingLink, link_id)
    if not link or link.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(link)
    await db.commit()
    return {"ok": True}

# ── Clicks explorer ──

@app.get("/api/clicks")
async def list_clicks(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), campaign_id: int | None = Query(default=None), tracking_link_id: int | None = Query(default=None), limit: int = Query(default=50, le=200), offset: int = Query(default=0), country: str | None = Query(default=None), device_type: str | None = Query(default=None)):
    from shared.models import Click
    # clicks are owner-scoped via campaign or tracking_link ownership. For simplicity, filter by user's campaigns/links.
    # Get user's campaign ids
    camp_ids_r = await db.execute(select(Campaign.id).where(Campaign.user_id == user.telegram_id))
    camp_ids = [row[0] for row in camp_ids_r.all()]
    from shared.models import TrackingLink
    link_ids_r = await db.execute(select(TrackingLink.id).where(TrackingLink.user_id == user.telegram_id))
    link_ids = [row[0] for row in link_ids_r.all()]
    q = select(Click).where((Click.campaign_id.in_(camp_ids) | Click.tracking_link_id.in_(link_ids)) if (camp_ids or link_ids) else False)
    if campaign_id:
        q = q.where(Click.campaign_id == campaign_id)
    if tracking_link_id:
        q = q.where(Click.tracking_link_id == tracking_link_id)
    if country:
        q = q.where(Click.country == country.upper())
    if device_type:
        q = q.where(Click.device_type == device_type)
    q = q.order_by(Click.created_at.desc()).limit(limit).offset(offset)
    r = await db.execute(q)
    clicks = r.scalars().all()
    return [{"id": c.id, "click_id": c.click_id, "campaign_id": c.campaign_id, "tracking_link_id": c.tracking_link_id, "fbclid": c.fbclid[:12]+"…" if c.fbclid else None, "fbc": bool(c.fbc), "fbp": bool(c.fbp), "campaign_name": c.campaign_name, "adset": c.adset, "ad": c.ad, "sub1": c.sub1, "sub2": c.sub2, "ip": c.ip, "country": c.country, "city": c.city, "browser": c.browser, "os": c.os, "device_type": c.device_type, "referrer": c.referrer, "landing_page": c.landing_page, "created_at": c.created_at.isoformat()} for c in clicks]

@app.get("/api/clicks/{click_id}")
async def get_click(click_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Click
    c = await db.get(Click, click_id)
    if not c:
        raise HTTPException(404, "Not found")
    # verify ownership
    allowed = False
    if c.campaign_id:
        camp = await db.get(Campaign, c.campaign_id)
        if camp and camp.user_id == user.telegram_id:
            allowed = True
    if c.tracking_link_id and not allowed:
        from shared.models import TrackingLink
        link = await db.get(TrackingLink, c.tracking_link_id)
        if link and link.user_id == user.telegram_id:
            allowed = True
    if not allowed and not user.is_admin:
        raise HTTPException(404, "Not found")
    return {"id": c.id, "click_id": c.click_id, "campaign_id": c.campaign_id, "tracking_link_id": c.tracking_link_id, "fbclid": c.fbclid, "fbc": c.fbc, "fbp": c.fbp, "campaign_name": c.campaign_name, "adset": c.adset, "adset_id": c.adset_id, "ad": c.ad, "ad_id": c.ad_id, "creative": c.creative, "placement": c.placement, "subs": {f"sub{i}": getattr(c, f"sub{i}") for i in range(1,10)}, "ip": c.ip, "country": c.country, "region": c.region, "city": c.city, "timezone": c.timezone, "language": c.language, "user_agent": c.user_agent, "browser": c.browser, "browser_version": c.browser_version, "os": c.os, "device_type": c.device_type, "referrer": c.referrer, "landing_page": c.landing_page, "event_id": c.event_id, "created_at": c.created_at.isoformat(), "expires_at": c.expires_at.isoformat() if c.expires_at else None}

# ── Telegram identities / CRM ──

@app.get("/api/identities")
async def list_identities(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), search: str | None = Query(default=None), tag_id: int | None = Query(default=None), limit: int = Query(default=50, le=200), offset: int = Query(default=0)):
    from shared.models import TelegramIdentity, ContactTag
    q = select(TelegramIdentity).where(TelegramIdentity.owner_user_id == user.telegram_id)
    if search:
        like = f"%{search.lower()}%"
        q = q.where((func.lower(TelegramIdentity.username).like(like)) | (func.lower(TelegramIdentity.first_name).like(like)) | (func.lower(TelegramIdentity.last_name).like(like)))
    if tag_id:
        # join contact_tags
        q = q.join(ContactTag, ContactTag.identity_id == TelegramIdentity.id).where(ContactTag.tag_id == tag_id)
    q = q.order_by(TelegramIdentity.last_seen.desc()).limit(limit).offset(offset)
    r = await db.execute(q)
    ids = r.scalars().all()
    return [{"id": i.id, "telegram_user_id": i.telegram_user_id, "username": i.username, "first_name": i.first_name, "last_name": i.last_name, "phone": bool(i.phone), "country": i.country, "first_seen": i.first_seen.isoformat(), "last_seen": i.last_seen.isoformat(), "source": i.source} for i in ids]

@app.get("/api/identities/{identity_id}")
async def get_identity(identity_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TelegramIdentity, ContactTag, Tag, TelegramEvent, Click
    ident = await db.get(TelegramIdentity, identity_id)
    if not ident or ident.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    # tags
    tags_r = await db.execute(select(Tag).join(ContactTag, ContactTag.tag_id == Tag.id).where(ContactTag.identity_id == identity_id))
    tags = [{"id": t.id, "name": t.name, "color": t.color} for t in tags_r.scalars().all()]
    # recent events
    ev_r = await db.execute(select(TelegramEvent).where(TelegramEvent.identity_id == identity_id).order_by(TelegramEvent.created_at.desc()).limit(50))
    events = [{"id": e.id, "event_type": e.event_type, "campaign_id": e.campaign_id, "click_id": e.click_id, "created_at": e.created_at.isoformat(), "metadata": json.loads(e.event_metadata) if e.event_metadata else None} for e in ev_r.scalars().all()]
    # clicks
    click_ids = list({e["click_id"] for e in events if e["click_id"]})
    clicks = []
    if click_ids:
        cr = await db.execute(select(Click).where(Click.id.in_(click_ids)))
        clicks = [{"id": c.id, "click_id": c.click_id, "campaign_id": c.campaign_id, "fbclid": c.fbclid, "created_at": c.created_at.isoformat()} for c in cr.scalars().all()]
    return {"identity": {"id": ident.id, "telegram_user_id": ident.telegram_user_id, "username": ident.username, "first_name": ident.first_name, "last_name": ident.last_name, "phone": ident.phone, "country": ident.country, "first_seen": ident.first_seen.isoformat(), "last_seen": ident.last_seen.isoformat(), "source": ident.source}, "tags": tags, "events": events, "clicks": clicks}

@app.get("/api/identities/{identity_id}/journey")
async def get_journey(identity_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TelegramEvent, Click, ConversionLog, MetaEvent
    ident = await db.get(__import__("shared.models", fromlist=["TelegramIdentity"]).TelegramIdentity, identity_id)
    if not ident or ident.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    # chronologically merge clicks + telegram_events + conversions + meta events
    journey = []
    # clicks via events
    ev_r = await db.execute(select(TelegramEvent).where(TelegramEvent.identity_id == identity_id).order_by(TelegramEvent.created_at.asc()))
    events = ev_r.scalars().all()
    for e in events:
        journey.append({"ts": e.created_at.isoformat(), "type": e.event_type, "campaign_id": e.campaign_id, "click_id": e.click_id, "meta": json.loads(e.event_metadata) if e.event_metadata else None, "source": "telegram_event"})
    # clicks
    click_ids = list({e.click_id for e in events if e.click_id})
    if click_ids:
        cr = await db.execute(select(Click).where(Click.id.in_(click_ids)))
        for c in cr.scalars().all():
            journey.append({"ts": c.created_at.isoformat(), "type": "CLICK", "click_id": c.id, "campaign_id": c.campaign_id, "fbclid": c.fbclid, "country": c.country, "device": c.device_type, "source": "click"})
    # conversions
    conv_r = await db.execute(select(ConversionLog).where(ConversionLog.identity_id == identity_id).order_by(ConversionLog.fired_at.asc()))
    for conv in conv_r.scalars().all():
        journey.append({"ts": conv.fired_at.isoformat(), "type": f"CONVERSION:{conv.event_type}", "campaign_id": conv.campaign_id, "trigger_type": conv.trigger_type, "status": conv.status, "source": "conversion"})
    # meta events
    meta_r = await db.execute(select(MetaEvent).where(MetaEvent.identity_id == identity_id).order_by(MetaEvent.created_at.asc()))
    for me in meta_r.scalars().all():
        journey.append({"ts": me.created_at.isoformat(), "type": f"META:{me.event_name}", "pixel_id": me.pixel_id, "status": me.status, "fbtrace_id": me.fbtrace_id, "source": "meta_event"})
    journey.sort(key=lambda x: x["ts"])
    return {"identity_id": identity_id, "journey": journey}

# ── Tags ──

@app.get("/api/tags")
async def list_tags(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Tag
    r = await db.execute(select(Tag).where(Tag.user_id == user.telegram_id).order_by(Tag.name))
    return [{"id": t.id, "name": t.name, "color": t.color, "created_at": t.created_at.isoformat()} for t in r.scalars().all()]

@app.post("/api/tags", status_code=201)
async def create_tag(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Tag
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    existing = await db.execute(select(Tag).where(Tag.user_id == user.telegram_id, func.lower(Tag.name) == name.lower()))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Tag already exists")
    t = Tag(user_id=user.telegram_id, name=name, color=body.get("color"))
    db.add(t)
    await db.commit()
    await db.refresh(t)
    return {"ok": True, "id": t.id}

@app.delete("/api/tags/{tag_id}")
async def delete_tag(tag_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Tag
    t = await db.get(Tag, tag_id)
    if not t or t.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(t)
    await db.commit()
    return {"ok": True}

@app.post("/api/identities/{identity_id}/tags")
async def add_tag_to_identity(identity_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TelegramIdentity, Tag, ContactTag
    ident = await db.get(TelegramIdentity, identity_id)
    if not ident or ident.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Identity not found")
    tag_id = body.get("tag_id")
    if not tag_id:
        raise HTTPException(400, "tag_id required")
    tag = await db.get(Tag, tag_id)
    if not tag or tag.user_id != user.telegram_id:
        raise HTTPException(404, "Tag not found")
    existing = await db.execute(select(ContactTag).where(ContactTag.tag_id == tag_id, ContactTag.identity_id == identity_id))
    if existing.scalar_one_or_none():
        return {"ok": True, "already": True}
    ct = ContactTag(tag_id=tag_id, identity_id=identity_id)
    db.add(ct)
    await db.commit()
    return {"ok": True}

@app.delete("/api/identities/{identity_id}/tags/{tag_id}")
async def remove_tag_from_identity(identity_id: int, tag_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import ContactTag, TelegramIdentity
    ident = await db.get(TelegramIdentity, identity_id)
    if not ident or ident.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Identity not found")
    r = await db.execute(select(ContactTag).where(ContactTag.tag_id == tag_id, ContactTag.identity_id == identity_id))
    ct = r.scalar_one_or_none()
    if not ct:
        raise HTTPException(404, "Tag not linked")
    await db.delete(ct)
    await db.commit()
    return {"ok": True}

# ── Event explorer ──

@app.get("/api/events")
async def list_events(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), event_type: str | None = Query(default=None), campaign_id: int | None = Query(default=None), telegram_user_id: int | None = Query(default=None), limit: int = Query(default=50, le=200), offset: int = Query(default=0), start_date: str | None = Query(default=None), end_date: str | None = Query(default=None)):
    from shared.models import TelegramEvent
    q = select(TelegramEvent).where(TelegramEvent.owner_user_id == user.telegram_id)
    if event_type:
        q = q.where(TelegramEvent.event_type == event_type)
    if campaign_id:
        q = q.where(TelegramEvent.campaign_id == campaign_id)
    if telegram_user_id:
        q = q.where(TelegramEvent.telegram_user_id == telegram_user_id)
    if start_date:
        try:
            sd = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            q = q.where(TelegramEvent.created_at >= sd)
        except Exception:
            pass
    if end_date:
        try:
            ed = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            q = q.where(TelegramEvent.created_at <= ed)
        except Exception:
            pass
    q = q.order_by(TelegramEvent.created_at.desc()).limit(limit).offset(offset)
    r = await db.execute(q)
    events = r.scalars().all()
    return [{"id": e.id, "event_id": e.event_id, "event_type": e.event_type, "telegram_user_id": e.telegram_user_id, "identity_id": e.identity_id, "click_id": e.click_id, "campaign_id": e.campaign_id, "fbclid": e.fbclid, "created_at": e.created_at.isoformat(), "metadata": json.loads(e.event_metadata) if e.event_metadata else None} for e in events]

@app.get("/api/events/{event_id}")
async def get_event(event_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import TelegramEvent
    e = await db.get(TelegramEvent, event_id)
    if not e or e.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    return {"id": e.id, "event_id": e.event_id, "event_type": e.event_type, "telegram_user_id": e.telegram_user_id, "identity_id": e.identity_id, "click_id": e.click_id, "campaign_id": e.campaign_id, "account_id": e.account_id, "trigger_id": e.trigger_id, "fbclid": e.fbclid, "fbc": e.fbc, "fbp": e.fbp, "meta_event_id": e.meta_event_id, "created_at": e.created_at.isoformat(), "metadata": json.loads(e.event_metadata) if e.event_metadata else None}

# ── Meta pixels ──

@app.get("/api/meta-pixels")
async def list_meta_pixels(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaPixel
    r = await db.execute(select(MetaPixel).where(MetaPixel.user_id == user.telegram_id).order_by(MetaPixel.created_at.desc()))
    return [{"id": p.id, "pixel_id": p.pixel_id, "has_token": bool(p.access_token), "test_event_code": p.test_event_code, "is_active": p.is_active, "label": p.label, "created_at": p.created_at.isoformat()} for p in r.scalars().all()]

@app.post("/api/meta-pixels", status_code=201)
async def create_meta_pixel(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaPixel
    from shared.security import encrypt_secret
    pixel_id = (body.get("pixel_id") or "").strip()
    if not pixel_id or not pixel_id.isdigit():
        raise HTTPException(400, "Valid pixel_id required (numeric)")
    existing = await db.execute(select(MetaPixel).where(MetaPixel.user_id == user.telegram_id, MetaPixel.pixel_id == pixel_id))
    if existing.scalar_one_or_none():
        raise HTTPException(409, "Pixel already configured")
    token = body.get("access_token")
    enc = encrypt_secret(token, settings.ENCRYPTION_KEY) if token else None
    p = MetaPixel(user_id=user.telegram_id, pixel_id=pixel_id, access_token=enc, test_event_code=body.get("test_event_code"), label=body.get("label"), is_active=True)
    db.add(p)
    await db.commit()
    await db.refresh(p)
    return {"ok": True, "id": p.id}

@app.patch("/api/meta-pixels/{pixel_id}")
async def update_meta_pixel(pixel_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaPixel
    from shared.security import encrypt_secret
    p = await db.get(MetaPixel, pixel_id)
    if not p or p.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if "access_token" in body:
        tok = body["access_token"]
        p.access_token = encrypt_secret(tok, settings.ENCRYPTION_KEY) if tok else None
    if "test_event_code" in body:
        p.test_event_code = body["test_event_code"] or None
    if "is_active" in body:
        p.is_active = bool(body["is_active"])
    if "label" in body:
        p.label = body["label"]
    await db.commit()
    return {"ok": True}

@app.delete("/api/meta-pixels/{pixel_id}")
async def delete_meta_pixel(pixel_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaPixel
    p = await db.get(MetaPixel, pixel_id)
    if not p or p.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(p)
    await db.commit()
    return {"ok": True}

# ── Meta events (CAPI log) ──

@app.get("/api/meta-events")
async def list_meta_events(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), status: str | None = Query(default=None), event_name: str | None = Query(default=None), limit: int = Query(default=50, le=200), offset: int = Query(default=0)):
    from shared.models import MetaEvent
    q = select(MetaEvent).where(MetaEvent.owner_user_id == user.telegram_id)
    if status:
        q = q.where(MetaEvent.status == status)
    if event_name:
        q = q.where(MetaEvent.event_name == event_name)
    q = q.order_by(MetaEvent.created_at.desc()).limit(limit).offset(offset)
    r = await db.execute(q)
    return [{"id": m.id, "event_id": m.event_id, "event_name": m.event_name, "pixel_id": m.pixel_id, "campaign_id": m.campaign_id, "telegram_user_id": m.telegram_user_id, "status": m.status, "attempt_count": m.attempt_count, "http_status": m.http_status, "fbtrace_id": m.fbtrace_id, "error_message": m.error_message, "created_at": m.created_at.isoformat(), "sent_at": m.sent_at.isoformat() if m.sent_at else None} for m in r.scalars().all()]

@app.get("/api/meta-events/{event_id}")
async def get_meta_event(event_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaEvent
    m = await db.get(MetaEvent, event_id)
    if not m or m.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    return {"id": m.id, "event_id": m.event_id, "event_name": m.event_name, "pixel_id": m.pixel_id, "campaign_id": m.campaign_id, "click_id": m.click_id, "telegram_user_id": m.telegram_user_id, "fbc": m.fbc, "fbp": m.fbp, "event_time": m.event_time, "custom_data": json.loads(m.custom_data) if m.custom_data else None, "user_data": json.loads(m.user_data) if m.user_data else None, "status": m.status, "attempt_count": m.attempt_count, "http_status": m.http_status, "meta_response": json.loads(m.meta_response) if m.meta_response else None, "fbtrace_id": m.fbtrace_id, "error_message": m.error_message, "dedup_key": m.dedup_key, "created_at": m.created_at.isoformat(), "sent_at": m.sent_at.isoformat() if m.sent_at else None}

@app.post("/api/meta-events/{event_id}/retry")
async def retry_meta_event(event_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaEvent, MetaEventStatus
    m = await db.get(MetaEvent, event_id)
    if not m or m.owner_user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if m.status not in (MetaEventStatus.FAILED, MetaEventStatus.DEAD_LETTER, MetaEventStatus.RETRYING):
        raise HTTPException(400, "Only failed events can be retried")
    m.status = MetaEventStatus.QUEUED
    m.next_attempt_at = datetime.now(timezone.utc)
    await db.commit()
    # enqueue
    try:
        from shared.queue import enqueue
        await enqueue(settings.QUEUE_META_CAPI, {"meta_event_id": m.id, "event_id": m.event_id})
    except Exception:
        pass
    return {"ok": True}

@app.post("/api/meta-events/test", status_code=201)
async def send_test_meta_event(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import MetaPixel, MetaEvent, MetaEventStatus
    pixel_id = body.get("pixel_id")
    event_name = body.get("event_name") or "Lead"
    test_code = body.get("test_event_code")
    if not pixel_id:
        # use first active pixel
        r = await db.execute(select(MetaPixel).where(MetaPixel.user_id == user.telegram_id, MetaPixel.is_active == True).limit(1))
        mp = r.scalar_one_or_none()
        if not mp:
            raise HTTPException(400, "No pixel configured")
        pixel_id = mp.pixel_id
        test_code = test_code or mp.test_event_code
    # create a test meta event directly (bypassing attribution)
    import uuid as _uuid
    event_id = str(_uuid.uuid4())
    from shared.meta import build_user_data, build_custom_data
    ud = build_user_data(telegram_id=user.telegram_id, first_name=user.first_name)
    cd = build_custom_data()
    me = MetaEvent(event_id=event_id, event_name=event_name, owner_user_id=user.telegram_id, pixel_id=pixel_id, event_time=int(datetime.now(timezone.utc).timestamp()), custom_data=json.dumps(cd), user_data=json.dumps(ud), status=MetaEventStatus.QUEUED, test_event_code=test_code)
    db.add(me)
    await db.commit()
    await db.refresh(me)
    try:
        from shared.queue import enqueue
        await enqueue(settings.QUEUE_META_CAPI, {"meta_event_id": me.id, "event_id": me.event_id})
    except Exception:
        pass
    return {"ok": True, "event_id": event_id, "meta_event_id": me.id}

# ── Flows ──

@app.get("/api/flows")
async def list_flows(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow
    r = await db.execute(select(Flow).where(Flow.user_id == user.telegram_id).order_by(Flow.created_at.desc()))
    return [{"id": f.id, "name": f.name, "description": f.description, "is_active": f.is_active, "campaign_id": f.campaign_id, "trigger_type": f.trigger_type, "created_at": f.created_at.isoformat()} for f in r.scalars().all()]

@app.post("/api/flows", status_code=201)
async def create_flow(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    campaign_id = body.get("campaign_id")
    if campaign_id:
        camp = await db.get(Campaign, campaign_id)
        if not camp or camp.user_id != user.telegram_id:
            raise HTTPException(404, "Campaign not found")
    f = Flow(user_id=user.telegram_id, name=name, description=body.get("description"), campaign_id=campaign_id, trigger_type=body.get("trigger_type"), is_active=body.get("is_active", True))
    db.add(f)
    await db.commit()
    await db.refresh(f)
    return {"ok": True, "id": f.id}

@app.get("/api/flows/{flow_id}")
async def get_flow(flow_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow, FlowNode, FlowEdge
    f = await db.get(Flow, flow_id)
    if not f or f.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    nodes_r = await db.execute(select(FlowNode).where(FlowNode.flow_id == flow_id))
    edges_r = await db.execute(select(FlowEdge).where(FlowEdge.flow_id == flow_id))
    nodes = [{"id": n.id, "node_type": n.node_type, "action_type": n.action_type, "config": json.loads(n.config) if n.config else None, "position_x": n.position_x, "position_y": n.position_y} for n in nodes_r.scalars().all()]
    edges = [{"id": e.id, "source_node_id": e.source_node_id, "target_node_id": e.target_node_id, "label": e.label} for e in edges_r.scalars().all()]
    return {"id": f.id, "name": f.name, "description": f.description, "is_active": f.is_active, "campaign_id": f.campaign_id, "trigger_type": f.trigger_type, "nodes": nodes, "edges": edges}

@app.patch("/api/flows/{flow_id}")
async def update_flow(flow_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow
    f = await db.get(Flow, flow_id)
    if not f or f.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    if "name" in body:
        f.name = body["name"]
    if "description" in body:
        f.description = body["description"]
    if "is_active" in body:
        f.is_active = bool(body["is_active"])
    if "campaign_id" in body:
        f.campaign_id = body["campaign_id"]
    if "trigger_type" in body:
        f.trigger_type = body["trigger_type"]
    await db.commit()
    return {"ok": True}

@app.delete("/api/flows/{flow_id}")
async def delete_flow(flow_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow
    f = await db.get(Flow, flow_id)
    if not f or f.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(f)
    await db.commit()
    return {"ok": True}

@app.post("/api/flows/{flow_id}/nodes", status_code=201)
async def create_flow_node(flow_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow, FlowNode
    f = await db.get(Flow, flow_id)
    if not f or f.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    node_type = body.get("node_type") or "action"
    n = FlowNode(flow_id=flow_id, node_type=node_type, action_type=body.get("action_type"), config=json.dumps(body.get("config")) if body.get("config") else None, position_x=body.get("position_x", 0), position_y=body.get("position_y", 0))
    db.add(n)
    await db.commit()
    await db.refresh(n)
    return {"ok": True, "id": n.id}

@app.post("/api/flows/{flow_id}/edges", status_code=201)
async def create_flow_edge(flow_id: int, body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import Flow, FlowEdge
    f = await db.get(Flow, flow_id)
    if not f or f.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    e = FlowEdge(flow_id=flow_id, source_node_id=body["source_node_id"], target_node_id=body["target_node_id"], label=body.get("label"))
    db.add(e)
    await db.commit()
    await db.refresh(e)
    return {"ok": True, "id": e.id}

# ── Attribution settings ──

@app.get("/api/settings/attribution")
async def get_attribution_settings(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import AttributionSetting
    r = await db.execute(select(AttributionSetting).where(AttributionSetting.user_id == user.telegram_id))
    s = r.scalar_one_or_none()
    if not s:
        from shared.config import get_settings as gs
        g = gs()
        return {"model": g.ATTRIBUTION_MODEL, "window_hours": g.ATTRIBUTION_WINDOW_HOURS, "include_organic": g.ATTRIBUTION_INCLUDE_ORGANIC}
    return {"model": s.model, "window_hours": s.window_hours, "include_organic": s.include_organic}

@app.put("/api/settings/attribution")
async def put_attribution_settings(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import AttributionSetting
    model = body.get("model") or "last_touch"
    if model not in ("last_touch", "first_touch", "last_non_direct"):
        raise HTTPException(400, "Invalid model")
    window = int(body.get("window_hours") or 168)
    if window < 1 or window > 720:
        raise HTTPException(400, "window_hours must be 1..720")
    inc = bool(body.get("include_organic", False))
    r = await db.execute(select(AttributionSetting).where(AttributionSetting.user_id == user.telegram_id))
    s = r.scalar_one_or_none()
    if not s:
        s = AttributionSetting(user_id=user.telegram_id, model=model, window_hours=window, include_organic=inc)
        db.add(s)
    else:
        s.model = model; s.window_hours = window; s.include_organic = inc
    await db.commit()
    return {"ok": True}

# ── API keys ──

@app.get("/api/api-keys")
async def list_api_keys(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import ApiKey
    r = await db.execute(select(ApiKey).where(ApiKey.user_id == user.telegram_id).order_by(ApiKey.created_at.desc()))
    return [{"id": k.id, "prefix": k.prefix, "label": k.label, "is_active": k.is_active, "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None, "created_at": k.created_at.isoformat()} for k in r.scalars().all()]

@app.post("/api/api-keys", status_code=201)
async def create_api_key(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import ApiKey
    import hashlib, secrets
    raw = secrets.token_urlsafe(32)
    prefix = raw[:8]
    h = hashlib.sha256(raw.encode()).hexdigest()
    k = ApiKey(user_id=user.telegram_id, key_hash=h, prefix=prefix, label=body.get("label"))
    db.add(k)
    await db.commit()
    await db.refresh(k)
    return {"ok": True, "id": k.id, "key": raw, "prefix": prefix}

@app.delete("/api/api-keys/{key_id}")
async def delete_api_key(key_id: int, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from shared.models import ApiKey
    k = await db.get(ApiKey, key_id)
    if not k or k.user_id != user.telegram_id:
        raise HTTPException(404, "Not found")
    await db.delete(k)
    await db.commit()
    return {"ok": True}

# ── Enhanced analytics ──

@app.get("/api/analytics/overview")
async def analytics_overview(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), days: int = Query(default=30, le=365), campaign_id: int | None = Query(default=None)):
    from shared.models import Click, TelegramEvent, ConversionLog, MetaEvent
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    # clicks
    camp_ids_r = await db.execute(select(Campaign.id).where(Campaign.user_id == user.telegram_id))
    camp_ids = [r[0] for r in camp_ids_r.all()]
    if not camp_ids:
        return {"clicks": 0, "users": 0, "conversions": 0, "meta_sent": 0, "meta_failed": 0, "series": []}
    q_clicks = select(func.count()).select_from(Click).where(Click.campaign_id.in_(camp_ids), Click.created_at >= cutoff)
    if campaign_id:
        q_clicks = q_clicks.where(Click.campaign_id == campaign_id)
    clicks = (await db.execute(q_clicks)).scalar() or 0
    # telegram users
    q_users = select(func.count(func.distinct(TelegramEvent.telegram_user_id))).where(TelegramEvent.owner_user_id == user.telegram_id, TelegramEvent.created_at >= cutoff)
    if campaign_id:
        q_users = q_users.where(TelegramEvent.campaign_id == campaign_id)
    users = (await db.execute(q_users)).scalar() or 0
    q_conv = select(func.count()).select_from(ConversionLog).join(Campaign, ConversionLog.campaign_id == Campaign.id).where(Campaign.user_id == user.telegram_id, ConversionLog.fired_at >= cutoff)
    if campaign_id:
        q_conv = q_conv.where(ConversionLog.campaign_id == campaign_id)
    convs = (await db.execute(q_conv)).scalar() or 0
    q_meta_sent = select(func.count()).select_from(MetaEvent).where(MetaEvent.owner_user_id == user.telegram_id, MetaEvent.status == "SENT", MetaEvent.created_at >= cutoff)
    meta_sent = (await db.execute(q_meta_sent)).scalar() or 0
    q_meta_failed = select(func.count()).select_from(MetaEvent).where(MetaEvent.owner_user_id == user.telegram_id, MetaEvent.status.in_(["FAILED","DEAD_LETTER"]), MetaEvent.created_at >= cutoff)
    meta_failed = (await db.execute(q_meta_failed)).scalar() or 0
    # series per day
    # Use date grouping — sqlite vs postgres
    from shared.config import get_settings as gs
    is_sqlite = gs().is_sqlite
    if is_sqlite:
        date_expr = func.date(Click.created_at)
    else:
        date_expr = func.date_trunc("day", Click.created_at)
    q_series = select(date_expr.label("d"), func.count().label("c")).where(Click.campaign_id.in_(camp_ids), Click.created_at >= cutoff).group_by(date_expr).order_by(date_expr)
    series_r = await db.execute(q_series)
    series = [{"date": str(row[0]), "clicks": row[1]} for row in series_r.all()]
    return {"clicks": clicks, "users": users, "conversions": convs, "meta_sent": meta_sent, "meta_failed": meta_failed, "series": series, "rate": round(convs/clicks*100,2) if clicks else 0}

@app.get("/api/analytics/breakdown")
async def analytics_breakdown(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db), by: str = Query(default="country"), days: int = Query(default=30), campaign_id: int | None = Query(default=None)):
    from shared.models import Click
    from datetime import timedelta
    if by not in ("country","device_type","browser","os"):
        raise HTTPException(400, "by must be country|device_type|browser|os")
    camp_ids_r = await db.execute(select(Campaign.id).where(Campaign.user_id == user.telegram_id))
    camp_ids = [r[0] for r in camp_ids_r.all()]
    if not camp_ids:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    col = getattr(Click, by)
    q = select(col, func.count().label("c")).where(Click.campaign_id.in_(camp_ids), Click.created_at >= cutoff)
    if campaign_id:
        q = q.where(Click.campaign_id == campaign_id)
    q = q.group_by(col).order_by(func.count().desc()).limit(20)
    r = await db.execute(q)
    return [{"value": row[0] or "Unknown", "count": row[1]} for row in r.all()]

@app.get("/api/export/clicks.csv")
async def export_clicks_csv(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from fastapi.responses import StreamingResponse
    import csv, io
    from shared.models import Click
    camp_ids_r = await db.execute(select(Campaign.id).where(Campaign.user_id == user.telegram_id))
    camp_ids = [r[0] for r in camp_ids_r.all()]
    q = select(Click).where(Click.campaign_id.in_(camp_ids) if camp_ids else False).order_by(Click.created_at.desc()).limit(10000)
    r = await db.execute(q)
    clicks = r.scalars().all()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["click_id","campaign_id","fbclid","fbc","fbp","country","device_type","browser","os","ip","created_at"])
    for c in clicks:
        w.writerow([c.click_id, c.campaign_id, c.fbclid or "", (c.fbc[:20]+"…" if c.fbc else ""), bool(c.fbp), c.country or "", c.device_type or "", c.browser or "", c.os or "", c.ip or "", c.created_at.isoformat()])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=clicks.csv"})

@app.get("/api/export/conversions.csv")
async def export_conversions_csv(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    from fastapi.responses import StreamingResponse
    import csv, io
    q = select(ConversionLog, Campaign.name.label("campaign_name")).join(Campaign, ConversionLog.campaign_id == Campaign.id).where(Campaign.user_id == user.telegram_id).order_by(ConversionLog.fired_at.desc()).limit(10000)
    r = await db.execute(q)
    rows = r.all()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["id","campaign","event_type","trigger_type","telegram_user_id","fbclid","status","fired_at"])
    for row in rows:
        c = row[0]
        w.writerow([c.id, row[1], c.event_type, c.trigger_type, c.telegram_user_id, c.fbclid or "", c.status, c.fired_at.isoformat()])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=conversions.csv"})

# ── Health extended (already exists as /health, add granular) ──

@app.get("/api/health/detailed")
async def detailed_health(user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    # reuse health logic inline
    from sqlalchemy import text as _text
    h = {}
    try:
        await db.execute(_text("SELECT 1"))
        h["database"] = {"status": "connected"}
    except Exception as e:
        h["database"] = {"status": "ERROR", "error": str(e)}
    try:
        await redis_client.ping()
        h["redis"] = {"status": "connected"}
    except Exception as e:
        h["redis"] = {"status": "ERROR", "error": str(e)}
    h["config"] = {"base_url": settings.BASE_URL}
    # add queue depths
    try:
        from shared.queue import queue_depth, delayed_depth
        h["queues"] = {
            "meta_capi": await queue_depth(settings.QUEUE_META_CAPI),
            "meta_capi_delayed": await delayed_depth(settings.QUEUE_META_CAPI),
        }
    except Exception as e:
        h["queues"] = {"error": str(e)}
    # worker heartbeats
    try:
        hb = await redis_client.get("worker:heartbeat:meta_capi")
        h["workers"] = {"meta_capi_last_heartbeat": hb}
    except Exception:
        pass
    return h

# ── Internal: reload channels (for hot-reload without worker restart) ──

@app.post("/api/internal/reload-channels")
async def reload_channels(user: DashboardUser = Depends(require_user)):
    try:
        await redis_client.publish("tg:reload_channels", json.dumps({"user_id": user.telegram_id, "ts": datetime.now(timezone.utc).isoformat()}))
    except Exception as e:
        logger.warning("reload publish failed: %s", e)
    return {"ok": True}

# ── Custom event ingestion (authenticated) ──

@app.post("/api/events/custom")
async def ingest_custom_event(body: dict, user: DashboardUser = Depends(require_user), db: AsyncSession = Depends(get_db)):
    telegram_user_id = body.get("telegram_user_id")
    if not telegram_user_id:
        raise HTTPException(400, "telegram_user_id required")
    try:
        telegram_user_id = int(telegram_user_id)
    except ValueError:
        raise HTTPException(400, "telegram_user_id must be int")
    event_name = body.get("event_name") or body.get("event_type") or "Custom"
    campaign_id = body.get("campaign_id")
    if campaign_id:
        camp = await db.get(Campaign, campaign_id)
        if not camp or camp.user_id != user.telegram_id:
            raise HTTPException(404, "Campaign not found")
    # find attribution
    from shared.attribution import resolve_attribution
    attr = await resolve_attribution(user.telegram_id, telegram_user_id)
    click_id = attr["click_id"] if attr else None
    campaign_id = campaign_id or (attr["campaign_id"] if attr else None)
    from shared.models import TelegramEvent, TelegramEventType
    # record
    from shared.events import record_event
    result = await record_event(owner_user_id=user.telegram_id, telegram_user_id=telegram_user_id, event_type=TelegramEventType.CUSTOM_EVENT, campaign_id=campaign_id, click_id=click_id, metadata={"event_name": event_name, "value": body.get("value"), "currency": body.get("currency")}, sender=None)
    # Create conversion log + meta event via queue (always, if pixel exists)
    if campaign_id and event_name:
        from shared.models import ConversionTrigger, TriggerType
        trig_r = await db.execute(select(ConversionTrigger).where(ConversionTrigger.campaign_id == campaign_id, ConversionTrigger.trigger_type == TriggerType.custom_event, ConversionTrigger.event_name == event_name))
        trig = trig_r.scalar_one_or_none()
        # Use trigger's value/currency if exists, otherwise body values
        trig_value = trig.value if trig else body.get("value")
        trig_currency = trig.currency if trig else body.get("currency")
        trig_content = trig.content_name if trig else body.get("content_name")
        from shared.models import ConversionLog, ConversionStatus, MetaEvent, MetaEventStatus
        from shared.meta import build_user_data, build_custom_data, new_event_id, dedup_key
        fbc = attr["fbc"] if attr else None
        fbp = attr["fbp"] if attr else None
        event_id = new_event_id()
        ud = build_user_data(telegram_id=telegram_user_id, fbc=fbc, fbp=fbp)
        cd = build_custom_data(value=trig_value, currency=trig_currency, content_name=trig_content)
        pixel_id = None
        if campaign_id:
            camp = await db.get(Campaign, campaign_id)
            if camp:
                acct = await db.get(TelegramAccount, camp.account_id) if camp.account_id else None
                pixel_id = acct.meta_pixel_id if acct and acct.meta_pixel_id else None
                if not pixel_id:
                    from shared.models import MetaPixel
                    r = await db.execute(select(MetaPixel).where(MetaPixel.user_id == user.telegram_id, MetaPixel.is_active == True).limit(1))
                    mp = r.scalar_one_or_none()
                    if mp:
                        pixel_id = mp.pixel_id
        if pixel_id:
            dedup = dedup_key(event_name, event_id, pixel_id)
            # check dedup
            existing = await db.execute(select(MetaEvent).where(MetaEvent.dedup_key == dedup))
            if not existing.scalar_one_or_none():
                me = MetaEvent(event_id=event_id, event_name=event_name, owner_user_id=user.telegram_id, pixel_id=pixel_id, campaign_id=campaign_id, click_id=click_id, telegram_user_id=telegram_user_id, fbc=fbc, fbp=fbp, event_time=int(datetime.now(timezone.utc).timestamp()), custom_data=json.dumps(cd), user_data=json.dumps(ud), status=MetaEventStatus.QUEUED, dedup_key=dedup)
                db.add(me)
                # also log conversion
                db.add(ConversionLog(campaign_id=campaign_id, account_id=camp.account_id if camp else 0, trigger_id=trig.id if trig else None, trigger_type="custom_event", telegram_user_id=telegram_user_id, event_type=event_name, event_value=float(trig_value) if trig_value else None, event_currency=trig_currency, content_name=trig_content, status=ConversionStatus.fired, meta_event_id=event_id, fbclid=attr["fbclid"] if attr else None, fbc=fbc, fbp=fbp, click_id=click_id, fired_at=datetime.now(timezone.utc)))
                await db.commit()
                await db.refresh(me)
                try:
                    from shared.queue import enqueue
                    await enqueue(settings.QUEUE_META_CAPI, {"meta_event_id": me.id})
                except Exception:
                    pass
    return {"ok": True, "event_name": event_name}


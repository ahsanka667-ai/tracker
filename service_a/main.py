"""
service_a/main.py — FastAPI Web Router + WebSocket hub
Full production API: click tracking, auth, accounts, campaigns,
conversions, messages, funnels, real-time WebSocket notifications.
"""
import hashlib, hmac, json, logging, os, secrets, string, sys, urllib.parse
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
    AccessToken, AccountType, Campaign, ConversionLog, ConversionStatus,
    ConversionTrigger, DashboardUser, EventType, Funnel, FunnelStep,
    Message, MessageDirection, TelegramAccount, TriggerType, UserSession, WebhookToken
)
from shared.security import hash_password, verify_password
from service_a.websocket_manager import ws_manager

setup_logging("service_a")
validate_or_exit("dashboard")

logger = logging.getLogger(__name__)
settings = get_settings()
redis_client: aioredis.Redis | None = None

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
    token = secrets.token_urlsafe(32)
    await redis_client.setex(f"session:{token}", SESSION_TTL_SECONDS, str(telegram_id))
    return token


async def _resolve_session(token: str) -> int | None:
    raw = await redis_client.get(f"session:{token}")
    if raw is None:
        return None
    # Sliding expiry — active users stay logged in
    await redis_client.expire(f"session:{token}", SESSION_TTL_SECONDS)
    return int(raw)
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
                         db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Campaign).where(Campaign.slug == slug, Campaign.is_active == True)
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(404, "Campaign not found")

    acct = await db.get(TelegramAccount, campaign.account_id)
    client_ip = request.headers.get("X-Forwarded-For", request.client.host).split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "")
    target = campaign.target_telegram_username.lstrip("@")

    # ── Link-preview crawler detection ──────────────────────────────
    # When this URL is shared in Telegram/WhatsApp/etc, their servers
    # fetch it to build a preview card. That is NOT a real ad click.
    # We still redirect (so the preview shows something sensible) but
    # we do NOT count a click, write a Redis key, or fire a notification.
    if _is_crawler(user_agent):
        return RedirectResponse(url=f"https://t.me/{target}", status_code=302)

    # ── Rate limiting per IP ─────────────────────────────────────────
    # Protects against click-fraud / abuse — a script hammering a tracking
    # URL would otherwise inflate click counts and burn through the Redis
    # TTL window with fake entries. Real users never hit this; a single
    # person clicking an ad a few times (double-click, retry) is fine.
    rl_key = f"ratelimit:click:{client_ip}"
    try:
        current = await redis_client.incr(rl_key)
        if current == 1:
            await redis_client.expire(rl_key, settings.CLICK_RATE_LIMIT_WINDOW_SECONDS)
        if current > settings.CLICK_RATE_LIMIT_MAX:
            logger.warning("Rate limit exceeded for IP %s on campaign %s", client_ip, slug)
            # Still redirect (don't reveal rate limiting to the client / give
            # away tracking infrastructure details) but skip counting.
            return RedirectResponse(url=f"https://t.me/{target}", status_code=302)
    except Exception:
        pass  # Redis hiccup shouldn't block real clicks

    short_key = _gen_key(8)
    payload = {
        "fbclid": fbclid or "",
        "campaign_id": campaign.id,
        "account_id": campaign.account_id,
        "user_id": campaign.user_id,
        "event_type": campaign.event_type,
        "meta_pixel_id": acct.meta_pixel_id if acct else "",
        "meta_capi_token": acct.meta_capi_token if acct else "",
        "target_username": campaign.target_telegram_username,
        "client_ip": client_ip,
        "user_agent": user_agent,
        "clicked_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis_client.setex(f"click:{short_key}", settings.REDIS_TTL_HOURS * 3600, json.dumps(payload))
    await db.execute(update(Campaign).where(Campaign.id == campaign.id)
                     .values(total_clicks=Campaign.total_clicks + 1))
    await db.commit()

    # Notify dashboard of new click
    await redis_client.publish("tg_notifications", json.dumps({
        "target_user_id": campaign.user_id,
        "type": "click",
        "title": "New Click",
        "body": f"{campaign.name} — {client_ip}",
        "data": {"campaign_id": campaign.id, "campaign_name": campaign.name},
        "ts": datetime.now(timezone.utc).isoformat(),
    }))

    return RedirectResponse(url=f"https://t.me/{target}?start={short_key}", status_code=302)


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
            await redis_client.delete(f"session:{token}")
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
    return {"ok": True, "funnel_id": funnel.id}


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
        if mon_acct.account_type != AccountType.PERSONAL:
            raise HTTPException(400, "Monitor account must be a personal account")
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

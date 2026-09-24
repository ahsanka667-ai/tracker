"""
service_b/worker.py — Telethon Worker Engine v4

Fixed version addressing SQLite session lock issues.
- Temporary client is disconnected before renaming/booting.
- Per-account asyncio.Lock prevents concurrent boot attempts.
- Retry logic on database locked errors.
- Clean session file handling.

Every trigger type now wired:
  1. Bot /start <key>      → fires configured event (default: Lead)
  2. First DM from user    → fires first_message trigger if configured
  3. Any DM keyword match  → fires keyword trigger (Purchase, etc.)
  4. Channel join          → fires channel_join trigger
  5. Manual (via API)      → handled in service_a, logged here
  6. Webhook (via API)     → handled in service_a, logged here

Session-based attribution:
  - /start stores a UserSession linking tg_user_id → campaign + fbclid
  - Later triggers (keyword, channel join) look up the session to fire
    with the original fbclid/ip even days after the initial click

Account types supported: BOT, PERSONAL, CHANNEL (monitors member joins)
"""
import asyncio, json, logging, os, re, sqlite3, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis
from sqlalchemy import select, update, and_, or_
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyError, ChannelPrivateError, FloodWaitError,
    PhoneCodeExpiredError, PhoneCodeInvalidError,
    SessionPasswordNeededError, UserDeactivatedError,
)
from telethon.tl.types import UpdateBotChatInviteRequester, UpdateChannel

from shared.config import get_settings, validate_or_exit
from shared.logging_config import setup_logging
from shared.database import AsyncSessionLocal, init_db
from shared.models import (
    AccountType, Campaign, ConversionLog, ConversionStatus,
    ConversionTrigger, Message, MessageDirection,
    TelegramAccount, TriggerType, UserSession
)
from service_b.meta_capi import fire_event, build_user_data, fire_lead_event

logger = logging.getLogger(__name__)

settings = get_settings()
Path(settings.SESSIONS_DIR).mkdir(parents=True, exist_ok=True)

active_clients: dict[int, TelegramClient] = {}
redis: aioredis.Redis | None = None

_reconnect_failures: dict[int, int] = {}
_alerted_down: set[int] = set()
_ALERT_AFTER_FAILURES = 3

# Per-account lock to prevent concurrent boot attempts
_account_boot_locks: dict[int, asyncio.Lock] = {}


# ── Admin alert ───────────────────────────────────────────────────────

async def alert_admin(text: str):
    if not settings.ADMIN_TELEGRAM_ID or not settings.MASTER_BOT_TOKEN:
        return
    try:
        import aiohttp
        url = f"https://api.telegram.org/bot{settings.MASTER_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            await s.post(url, json={"chat_id": settings.ADMIN_TELEGRAM_ID,
                                    "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        logger.warning("alert_admin failed: %s", e)


# ── Real-time dashboard notification ─────────────────────────────────

async def _mark_account_inactive(account_id: int):
    """Permanently marks a broken account inactive so heartbeat stops retrying it."""
    try:
        async with AsyncSessionLocal() as db:
            acct = await db.get(TelegramAccount, account_id)
            if acct:
                acct.is_active = False
                await db.commit()
                logger.info("Account %d marked inactive in DB", account_id)
    except Exception as e:
        logger.error("Failed to mark account %d inactive: %s", account_id, e)


async def notify(user_id: int, type: str, title: str, body: str, data: dict = {}):
    try:
        await redis.publish("tg_notifications", json.dumps({
            "target_user_id": user_id, "type": type,
            "title": title, "body": body, "data": data,
            "ts": datetime.now(timezone.utc).isoformat(),
        }))
    except Exception as e:
        logger.warning("notify failed: %s", e)


# ── Click payload (atomic consume) ────────────────────────────────────

async def consume_click_payload(short_key: str) -> dict | None:
    try:
        raw = await redis.getdel(f"click:{short_key}")
    except Exception:
        raw = await redis.get(f"click:{short_key}")
        if raw:
            await redis.delete(f"click:{short_key}")
    return json.loads(raw) if raw else None


# ── Session management ────────────────────────────────────────────────

async def get_or_create_session(campaign_id: int, tg_user_id: int,
                                  payload: dict | None, sender) -> UserSession | None:
    """
    Find an existing session for this user+campaign, or create one from
    the click payload. Returns None if no session and no payload.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(UserSession).where(
                and_(UserSession.tg_user_id == tg_user_id,
                     UserSession.campaign_id == campaign_id)
            )
        )
        session = result.scalar_one_or_none()
        if session:
            # Update with any new info
            if sender:
                if getattr(sender, "username", None) and not session.tg_username:
                    session.tg_username = sender.username
                if getattr(sender, "first_name", None) and not session.tg_first_name:
                    session.tg_first_name = sender.first_name
                if getattr(sender, "phone", None) and not session.tg_phone:
                    session.tg_phone = sender.phone
            session.last_seen_at = datetime.now(timezone.utc)
            await db.commit()
            return session

        if not payload:
            return None

        new_session = UserSession(
            campaign_id=campaign_id,
            tg_user_id=tg_user_id,
            tg_username=getattr(sender, "username", None) if sender else None,
            tg_first_name=getattr(sender, "first_name", None) if sender else None,
            tg_phone=getattr(sender, "phone", None) if sender else None,
            session_key=payload.get("session_key", ""),
            fbclid=payload.get("fbclid", ""),
            fbc=payload.get("fbc"),
            fbp=payload.get("fbp"),
            client_ip=payload.get("client_ip", ""),
            user_agent=payload.get("user_agent", ""),
            click_id=payload.get("click_id"),
            fired_triggers="",
        )
        db.add(new_session)
        await db.commit()
        await db.refresh(new_session)
        return new_session


async def mark_trigger_fired(session: UserSession, trigger_id: int):
    """
    Record that a SPECIFIC trigger (by id) has fired for this session.

    Deliberately keyed by trigger_id, not trigger_type. A campaign can
    have multiple triggers of the same type — e.g. two keyword triggers,
    one firing InitiateCheckout on "interested", another firing Purchase
    on "paid". Deduping by type string would mean the first keyword
    match blocks the second trigger from ever firing for that user,
    silently dropping real conversions. Keying by id keeps each
    configured trigger independent.
    """
    async with AsyncSessionLocal() as db:
        s = await db.get(UserSession, session.id)
        if s:
            fired = set(s.fired_triggers.split(",")) if s.fired_triggers else set()
            fired.discard("")
            fired.add(str(trigger_id))
            s.fired_triggers = ",".join(fired)
            await db.commit()


def trigger_already_fired(session: UserSession, trigger_id: int) -> bool:
    if not session.fired_triggers:
        return False
    return str(trigger_id) in session.fired_triggers.split(",")


# ── Trigger resolver ──────────────────────────────────────────────────

async def get_triggers_for_campaign(campaign_id: int, trigger_type: TriggerType) -> list[ConversionTrigger]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ConversionTrigger).where(
                and_(ConversionTrigger.campaign_id == campaign_id,
                     ConversionTrigger.trigger_type == trigger_type,
                     ConversionTrigger.is_active == True)
            ).order_by(ConversionTrigger.trigger_order)
        )
        return result.scalars().all()


# ── Conversion firing ─────────────────────────────────────────────────

async def fire_trigger(
    trigger: ConversionTrigger,
    account: TelegramAccount,
    session: UserSession,
    tg_user_id: int,
    campaign: Campaign | None = None,
    skip_if_fired: bool = True,
):
    """
    Enqueue a Meta CAPI event instead of firing inline.
    This keeps Telegram handlers responsive even if Meta is slow.
    """
    if skip_if_fired and trigger_already_fired(session, trigger.id):
        logger.debug("Trigger id=%d (%s) already fired for user %d campaign %d — skipping",
                     trigger.id, trigger.trigger_type, tg_user_id, trigger.campaign_id)
        return

    # Resolve identity + attribution for proper linking
    from shared.models import MetaEvent, MetaEventStatus
    from shared.meta import build_user_data as _build_ud, build_custom_data as _build_cd, new_event_id, dedup_key
    from shared.security import decrypt_secret
    import json as _json

    # Use stored fbc/fbp verbatim from session (click-time values)
    fbc = getattr(session, "fbc", None) or None
    fbp = getattr(session, "fbp", None) or None
    # Fallback to Click's fbc/fbp if session doesn't have it (migration)
    if not fbc and getattr(session, "click_id", None):
        try:
            from shared.models import Click as _Click
            async with AsyncSessionLocal() as db:
                cl = await db.get(_Click, session.click_id)
                if cl:
                    fbc = cl.fbc or fbc
                    fbp = cl.fbp or fbp
        except Exception:
            pass

    ud = _build_ud(
        telegram_id=tg_user_id,
        first_name=session.tg_first_name,
        phone=session.tg_phone,
        username=session.tg_username,
        client_ip=session.client_ip,
        user_agent=session.user_agent,
        fbc=fbc,
        fbp=fbp,
    )

    content_ids = [x.strip() for x in trigger.content_ids.split(",")] if trigger.content_ids else None
    extra = _json.loads(trigger.custom_data_json) if trigger.custom_data_json else None
    cd = _build_cd(value=trigger.value, currency=trigger.currency, content_name=trigger.content_name, content_ids=content_ids, extra=extra)

    # Find pixel id: account's pixel or MetaPixel table
    pixel_id = account.meta_pixel_id or ""
    if not pixel_id:
        try:
            from shared.models import MetaPixel
            async with AsyncSessionLocal() as db:
                r = await db.execute(select(MetaPixel).where(MetaPixel.user_id == account.user_id, MetaPixel.is_active == True).limit(1))
                mp = r.scalar_one_or_none()
                if mp:
                    pixel_id = mp.pixel_id
        except Exception:
            pass
    if not pixel_id:
        logger.warning("No pixel for campaign %d — skipping CAPI", trigger.campaign_id)
        pixel_id = "unknown"

    event_id = new_event_id()
    dedup = dedup_key(trigger.event_name, event_id, pixel_id)

    # Create conversion log + telegram event + meta event atomically
    camp_owner_id = 0
    meta_id = None
    conv_id = None
    async with AsyncSessionLocal() as db:
        camp = campaign or await db.get(Campaign, trigger.campaign_id)
        if camp:
            camp_owner_id = camp.user_id
        # Find identity for linking
        identity_id = None
        try:
            from shared.models import TelegramIdentity
            r = await db.execute(select(TelegramIdentity).where(TelegramIdentity.owner_user_id == camp_owner_id, TelegramIdentity.telegram_user_id == tg_user_id).limit(1))
            ident = r.scalar_one_or_none()
            if ident:
                identity_id = ident.id
        except Exception:
            pass

        # Idempotency: if dedup already exists, skip
        existing = await db.execute(select(MetaEvent).where(MetaEvent.dedup_key == dedup))
        if existing.scalar_one_or_none():
            logger.info("Meta dedup skip [event=%s id=%s]", trigger.event_name, event_id)
            return

        # Create conversion log (status QUEUED until CAPI confirms)
        clog = ConversionLog(
            campaign_id=trigger.campaign_id,
            account_id=account.id,
            trigger_id=trigger.id,
            trigger_type=trigger.trigger_type.value,
            telegram_user_id=tg_user_id,
            telegram_username=session.tg_username,
            fbclid=session.fbclid,
            fbc=fbc,
            fbp=fbp,
            client_ip=session.client_ip,
            user_agent=session.user_agent,
            event_type=trigger.event_name,
            event_value=trigger.value,
            event_currency=trigger.currency,
            content_name=trigger.content_name,
            status=ConversionStatus.fired,
            meta_event_id=event_id,
            fired_at=datetime.now(timezone.utc),
            click_id=getattr(session, "click_id", None),
            identity_id=identity_id,
        )
        db.add(clog)
        await db.flush()
        conv_id = clog.id
        if camp:
            await db.execute(update(Campaign).where(Campaign.id == trigger.campaign_id).values(total_conversions=Campaign.total_conversions + 1))

        # Create meta_events row
        test_code = None
        try:
            from shared.models import MetaPixel
            r = await db.execute(select(MetaPixel).where(MetaPixel.pixel_id == pixel_id).limit(1))
            mp = r.scalar_one_or_none()
            if mp:
                test_code = mp.test_event_code
        except Exception:
            pass

        # also create TelegramEvent
        from shared.models import TelegramEvent, TelegramEventType
        try:
            # map trigger type to event type
            etype = TelegramEventType.CUSTOM_EVENT
            try:
                etype = TelegramEventType(trigger.trigger_type.value.upper())
            except Exception:
                etype = TelegramEventType.CUSTOM_EVENT
            # normalize
            if trigger.trigger_type == TriggerType.bot_start:
                etype = TelegramEventType.BOT_START
            elif trigger.trigger_type == TriggerType.channel_join:
                etype = TelegramEventType.CHANNEL_JOIN
            elif trigger.trigger_type == TriggerType.keyword:
                etype = TelegramEventType.KEYWORD_MATCH

            te = TelegramEvent(
                event_id=event_id,
                event_type=etype,
                owner_user_id=camp_owner_id,
                telegram_user_id=tg_user_id,
                identity_id=identity_id,
                click_id=getattr(session, "click_id", None),
                campaign_id=trigger.campaign_id,
                account_id=account.id,
                trigger_id=trigger.id,
                fbclid=session.fbclid,
                fbc=fbc,
                fbp=fbp,
                meta_event_id=event_id,
                event_metadata=_json.dumps({"trigger_type": trigger.trigger_type.value, "event_name": trigger.event_name}),
                created_at=datetime.now(timezone.utc),
            )
            db.add(te)
            await db.flush()
            te_id = te.id
        except Exception as e:
            logger.warning("Failed to create TelegramEvent: %s", e)
            te_id = None

        me = MetaEvent(
            event_id=event_id,
            event_name=trigger.event_name,
            owner_user_id=camp_owner_id,
            pixel_id=pixel_id,
            campaign_id=trigger.campaign_id,
            click_id=getattr(session, "click_id", None),
            identity_id=identity_id,
            telegram_user_id=tg_user_id,
            telegram_event_id=te_id,
            conversion_log_id=conv_id,
            fbc=fbc,
            fbp=fbp,
            event_time=int(datetime.now(timezone.utc).timestamp()),
            event_source_url="https://t.me",
            custom_data=_json.dumps(cd) if cd else None,
            user_data=_json.dumps(ud),
            status=MetaEventStatus.QUEUED,
            test_event_code=test_code,
            dedup_key=dedup,
        )
        db.add(me)
        await db.flush()
        meta_id = me.id
        # link back
        clog.meta_event_row_id = meta_id
        if te_id:
            te.meta_event_id = event_id
        await db.commit()

    # Mark fired + enqueue
    await mark_trigger_fired(session, trigger.id)
    try:
        from shared.queue import enqueue
        await enqueue(settings.QUEUE_META_CAPI, {"meta_event_id": meta_id, "event_id": event_id})
        log_event(logger, "META_EVENT_QUEUED", meta_event_id=meta_id, event_name=trigger.event_name, dedup=dedup)
    except Exception as e:
        logger.warning("Failed to enqueue CAPI job: %s", e)

    uname = session.tg_username or str(tg_user_id)
    # Don't wait for Meta — notify immediately that event is queued
    await notify(
        camp_owner_id,
        "conversion",
        f"✅ {trigger.event_name}",
        f"@{uname} — {trigger.trigger_type.value} (queued)",
        {"campaign_id": trigger.campaign_id, "event_type": trigger.event_name, "trigger_type": trigger.trigger_type.value, "telegram_user_id": tg_user_id}
    )
    logger.info("Trigger queued [type=%s event=%s user=%d meta_id=%s]", trigger.trigger_type, trigger.event_name, tg_user_id, meta_id)


# ── Campaign lookup by account ────────────────────────────────────────

async def get_active_campaigns(account_id: int) -> list[Campaign]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Campaign).where(
                and_(Campaign.account_id == account_id, Campaign.is_active == True)
            )
        )
        return result.scalars().all()


async def get_channel_accounts_for_monitor(monitor_account_id: int) -> list[TelegramAccount]:
    """CHANNEL-type accounts whose 'linked personal account' is this one."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(TelegramAccount).where(
                and_(
                    TelegramAccount.monitor_account_id == monitor_account_id,
                    TelegramAccount.account_type == AccountType.CHANNEL,
                    TelegramAccount.is_active == True,
                )
            )
        )
        return result.scalars().all()


def _keyword_matches(text: str, keywords_csv: str | None, match_mode: str = "any") -> bool:
    """
    Whole-word/phrase keyword matching (NOT substring — "pay" no longer
    matches inside "repay"), with any/all mode:
      - "any" (default): fires if AT LEAST ONE keyword/phrase is present
        as a whole word/phrase anywhere in the text (OR).
      - "all": fires only if EVERY keyword/phrase in the list is present
        somewhere in the text (AND) — each is matched independently as a
        whole word/phrase; they don't need to be adjacent to each other.
    A "keyword" can itself be a multi-word phrase (e.g. "not interested")
    — it's still whole-word-bounded on each end, so it won't match inside
    a longer unrelated word.
    """
    if not keywords_csv:
        return False
    keywords = [k.strip() for k in keywords_csv.split(",") if k.strip()]
    if not keywords:
        return False
    text = text or ""
    hits = [bool(re.search(r"\b" + re.escape(kw) + r"\b", text, re.IGNORECASE | re.UNICODE))
            for kw in keywords]
    return all(hits) if match_mode == "all" else any(hits)


# ── Message saving ────────────────────────────────────────────────────

async def save_message(account: TelegramAccount, sender,
                        text: str, direction=MessageDirection.inbound,
                        campaign_id: int | None = None):
    try:
        async with AsyncSessionLocal() as db:
            db.add(Message(
                account_id=account.id,
                tg_chat_id=getattr(sender, "id", 0),
                tg_user_id=getattr(sender, "id", None),
                tg_username=getattr(sender, "username", None),
                tg_first_name=getattr(sender, "first_name", None),
                direction=direction,
                text=text[:4096] if text else None,
                is_read=False,
                campaign_id=campaign_id,
                received_at=datetime.now(timezone.utc),
            ))
            await db.commit()
    except Exception as e:
        logger.error("save_message error: %s", e)


# ── Proxy parser ──────────────────────────────────────────────────────

def _parse_proxy(proxy_string: str | None) -> tuple | None:
    if not proxy_string:
        return None
    try:
        import urllib.parse
        p = urllib.parse.urlparse(proxy_string)
        return ({"socks5":2,"socks4":1,"http":3}.get(p.scheme.lower(),2),
                p.hostname, p.port, True, p.username, p.password)
    except Exception:
        return None


# ── Event handlers per account ────────────────────────────────────────

def _register_handlers(client: TelegramClient, account: TelegramAccount):

    # ── BOT: /start <key> ────────────────────────────────────────────
    @client.on(events.NewMessage(pattern=r"^/start\s*(\w{0,20})$"))
    async def handle_bot_start(event):
        if account.account_type != AccountType.BOT:
            return
        try:
            sender = await event.get_sender()
            short_key = event.pattern_match.group(1)
            text = f"/start {short_key}".strip()

            # Save to inbox (ALL /start messages)
            await save_message(account, sender, text)

            if not short_key:
                return

            # short_key may be a signed correlation token
            token_to_consume = short_key
            click_public_id = short_key
            try:
                from shared.security import verify_token, TokenError
                from shared.config import get_settings as _gs
                data = verify_token(short_key, _gs().SECRET_KEY, purpose="click")
                click_public_id = data.get("cid") or short_key
                token_to_consume = click_public_id
            except Exception:
                pass

            payload = await consume_click_payload(token_to_consume)
            # Account check: campaign's account must match, but also allow if payload has no account_id (new clicks)
            if not payload:
                return
            # strict check only if payload has account_id
            if payload.get("account_id") and str(payload.get("account_id")) != str(account.id):
                # check if campaign belongs to same owner even if account differs (tracking_link vs campaign)
                try:
                    from shared.models import Campaign as _Camp
                    async with AsyncSessionLocal() as db:
                        camp = await db.get(_Camp, payload.get("campaign_id"))
                        if not camp or camp.user_id != account.user_id:
                            return
                except Exception:
                    return

            payload["session_key"] = short_key
            campaign_id = payload["campaign_id"]

            # Create/update user session
            session = await get_or_create_session(campaign_id, sender.id, payload, sender)
            if not session:
                return

            # Fire all bot_start triggers for this campaign
            triggers = await get_triggers_for_campaign(campaign_id, TriggerType.bot_start)
            if not triggers:
                # Fallback: use campaign's default event_type
                async with AsyncSessionLocal() as db:
                    camp = await db.get(Campaign, campaign_id)
                if camp:
                    result = await fire_event(
                        pixel_id=account.meta_pixel_id or payload.get("meta_pixel_id",""),
                        capi_token=account.meta_capi_token or payload.get("meta_capi_token",""),
                        event_name=camp.event_type,
                        user_data=build_user_data(
                            telegram_id=sender.id,
                            first_name=getattr(sender,"first_name",None),
                            phone=getattr(sender,"phone",None),
                            username=getattr(sender,"username",None),
                            client_ip=payload.get("client_ip",""),
                            user_agent=payload.get("user_agent",""),
                            fbclid=payload.get("fbclid",""),
                        ),
                    )
                    status = "error" if result.get("error") else "fired"
                    async with AsyncSessionLocal() as db:
                        db.add(ConversionLog(
                            campaign_id=campaign_id, account_id=account.id,
                            trigger_type="bot_start", telegram_user_id=sender.id,
                            telegram_username=getattr(sender,"username",None),
                            fbclid=payload.get("fbclid",""), client_ip=payload.get("client_ip",""),
                            user_agent=payload.get("user_agent",""),
                            event_type=camp.event_type,
                            status=ConversionStatus(status),
                            error_detail=str(result.get("error")) if result.get("error") else None,
                            meta_event_id=result.get("event_id"),
                            fbtrace_id=result.get("fbtrace_id"),
                            fired_at=datetime.now(timezone.utc),
                        ))
                        if status == "fired":
                            await db.execute(update(Campaign).where(Campaign.id==campaign_id)
                                             .values(total_conversions=Campaign.total_conversions+1))
                        await db.commit()
                    if status == "fired":
                        # dedup via trigger id — fallback path had a string bug, now use first trigger id or skip
                        fallback_id = triggers[0].id if triggers else 0
                        if fallback_id:
                            await mark_trigger_fired(session, fallback_id)
                        await notify(payload.get("user_id",0), "conversion",
                                     f"✅ {camp.event_type}",
                                     f"@{getattr(sender,'username','') or sender.id} via /start",
                                     {"campaign_id":campaign_id,"event_type":camp.event_type})
            else:
                for trig in triggers:
                    await fire_trigger(trig, account, session, sender.id)

            logger.info("Bot /start handled [acct=%d key=%s user=%d]", account.id, short_key, sender.id)

        except FloodWaitError as e: await asyncio.sleep(e.seconds)
        except Exception as e: logger.exception("handle_bot_start: %s", e)

    # ── BOT/PERSONAL: ALL incoming private messages ───────────────────
    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def handle_inbound_dm(event):
        try:
            sender = await event.get_sender()
            text = event.raw_text or ""
            tg_user_id = sender.id

            # Save to inbox
            await save_message(account, sender, text)

            # Notify dashboard
            uname = getattr(sender,"username",None) or getattr(sender,"first_name","User")
            await notify(account.user_id, "message",
                         f"💬 New message from @{uname}",
                         text[:80],
                         {"account_id":account.id,"tg_user_id":tg_user_id})

            # Find all active campaigns for this account
            campaigns = await get_active_campaigns(account.id)

            for camp in campaigns:
                # Look up existing session for this user
                async with AsyncSessionLocal() as db:
                    r = await db.execute(select(UserSession).where(
                        and_(UserSession.tg_user_id==tg_user_id, UserSession.campaign_id==camp.id)
                    ))
                    session = r.scalar_one_or_none()

                if not session:
                    # Check if message contains a click key (personal accounts)
                    _KEY_RE = re.compile(r"\b([A-Za-z0-9]{6,20})\b")
                    for key in _KEY_RE.findall(text):
                        payload = await consume_click_payload(key)
                        if payload and str(payload.get("account_id")) == str(account.id) and payload.get("campaign_id") == camp.id:
                            payload["session_key"] = key
                            session = await get_or_create_session(camp.id, tg_user_id, payload, sender)
                            # Fire bot_start equivalent for personal accounts
                            triggers = await get_triggers_for_campaign(camp.id, TriggerType.bot_start)
                            for trig in triggers:
                                await fire_trigger(trig, account, session, tg_user_id)
                            if not triggers:
                                # fire default event
                                result = await fire_event(
                                    pixel_id=account.meta_pixel_id or "",
                                    capi_token=account.meta_capi_token or "",
                                    event_name=camp.event_type,
                                    user_data=build_user_data(telegram_id=tg_user_id,
                                        first_name=getattr(sender,"first_name",None),
                                        phone=getattr(sender,"phone",None),
                                        username=getattr(sender,"username",None),
                                        client_ip=payload.get("client_ip",""),
                                        fbclid=payload.get("fbclid","")),
                                )
                                status = "error" if result.get("error") else "fired"
                                async with AsyncSessionLocal() as db:
                                    db.add(ConversionLog(
                                        campaign_id=camp.id, account_id=account.id,
                                        trigger_type="bot_start", telegram_user_id=tg_user_id,
                                        telegram_username=getattr(sender,"username",None),
                                        fbclid=payload.get("fbclid",""), client_ip=payload.get("client_ip",""),
                                        event_type=camp.event_type,
                                        status=ConversionStatus(status),
                                        error_detail=str(result.get("error")) if result.get("error") else None,
                                        meta_event_id=result.get("event_id"),
                                        fired_at=datetime.now(timezone.utc),
                                    ))
                                    if status == "fired":
                                        await db.execute(update(Campaign).where(Campaign.id==camp.id)
                                                         .values(total_conversions=Campaign.total_conversions+1))
                                    await db.commit()
                            break
                    if not session:
                        continue  # unknown user, no campaign link

                # ── first_message trigger ─────────────────────────────
                ft = await get_triggers_for_campaign(camp.id, TriggerType.first_message)
                for trig in ft:
                    await fire_trigger(trig, account, session, tg_user_id, skip_if_fired=True)

                # ── keyword trigger ───────────────────────────────────
                kt = await get_triggers_for_campaign(camp.id, TriggerType.keyword)
                for trig in kt:
                    if _keyword_matches(text, trig.keywords, trig.match_mode):
                        # Keywords don't have skip_if_fired — same user can
                        # trigger keyword multiple times (e.g. multiple purchases)
                        await fire_trigger(trig, account, session, tg_user_id, skip_if_fired=False)

        except FloodWaitError as e: await asyncio.sleep(e.seconds)
        except Exception as e: logger.exception("handle_inbound_dm: %s", e)

    # ── Channel/group join tracking ───────────────────────────────────
    @client.on(events.Raw(UpdateBotChatInviteRequester))
    async def handle_join_request(update_obj):
        try:
            link_str = str(getattr(getattr(update_obj,"invite",None),"link",""))
            m = re.search(r"([A-Za-z0-9]{6,20})$", link_str)
            if not m:
                return
            short_key = m.group(1)
            payload = await consume_click_payload(short_key)
            if not payload or str(payload.get("account_id")) != str(account.id):
                return

            user_peer = update_obj.peer
            user_id = getattr(user_peer, "user_id", 0)
            campaign_id = payload["campaign_id"]
            payload["session_key"] = short_key

            session = await get_or_create_session(campaign_id, user_id, payload, None)
            if not session:
                return

            # Auto-approve
            try:
                from telethon.tl.functions.channels import HideChatJoinRequestRequest
                await client(HideChatJoinRequestRequest(channel=update_obj.chat_id,
                                                         user_id=update_obj.peer, approved=True))
            except Exception as ae:
                logger.error("Auto-approve failed: %s", ae)

            triggers = await get_triggers_for_campaign(campaign_id, TriggerType.channel_join)
            for trig in triggers:
                await fire_trigger(trig, account, session, user_id, skip_if_fired=True)

            if not triggers:
                async with AsyncSessionLocal() as db:
                    camp = await db.get(Campaign, campaign_id)
                if camp:
                    result = await fire_event(
                        pixel_id=account.meta_pixel_id or "",
                        capi_token=account.meta_capi_token or "",
                        event_name=camp.event_type,
                        user_data=build_user_data(telegram_id=user_id, fbclid=payload.get("fbclid",""),
                                                   client_ip=payload.get("client_ip","")),
                    )
                    status = "error" if result.get("error") else "fired"
                    async with AsyncSessionLocal() as db:
                        db.add(ConversionLog(
                            campaign_id=campaign_id, account_id=account.id,
                            trigger_type="channel_join", telegram_user_id=user_id,
                            fbclid=payload.get("fbclid",""), event_type=camp.event_type,
                            status=ConversionStatus(status),
                            error_detail=str(result.get("error")) if result.get("error") else None,
                            meta_event_id=result.get("event_id"),
                            fired_at=datetime.now(timezone.utc),
                        ))
                        if status == "fired":
                            await db.execute(update(Campaign).where(Campaign.id==campaign_id)
                                             .values(total_conversions=Campaign.total_conversions+1))
                        await db.commit()

        except FloodWaitError as e: await asyncio.sleep(e.seconds)
        except Exception as e: logger.exception("handle_join_request: %s", e)


# ── CHANNEL/GROUP monitoring ────────────────────────────────────────
#
# A CHANNEL-type account has no Telethon client of its own (Telegram only
# allows one active MTProto session per phone number) — instead the
# *linked* PERSONAL account (TelegramAccount.monitor_account_id) listens
# on its behalf, scoped to just that chat. This function is called once
# the monitor account's client is booted, with the list of CHANNEL rows
# pointing at it.
#
# Requirement: the monitor personal account must already be a MEMBER of
# the target channel/group (admin recommended, so it can see the "user
# joined" service messages reliably) — this worker does not join chats
# automatically.

async def _register_channel_handlers(client: TelegramClient, channel_accounts: list[TelegramAccount]):
    targets: dict[int, TelegramAccount] = {}  # resolved chat id -> CHANNEL account row
    for ch in channel_accounts:
        try:
            entity = await client.get_entity(ch.identifier)
            targets[entity.id] = ch
            logger.info("Channel account %d ('%s') resolved -> chat_id=%d, now monitored",
                        ch.id, ch.identifier, entity.id)
        except Exception as e:
            logger.warning(
                "Channel account %d: could not resolve '%s' (%s). The monitor account must "
                "already be a member of this channel/group — join it first, then restart the worker.",
                ch.id, ch.identifier, e,
            )

    if not targets:
        return

    async def _fire_default_or_triggers(ch_account: TelegramAccount, campaign_id: int,
                                          trigger_type: TriggerType, session: UserSession, user_id: int):
        triggers = await get_triggers_for_campaign(campaign_id, trigger_type)
        if triggers:
            for trig in triggers:
                await fire_trigger(trig, ch_account, session, user_id, skip_if_fired=True)
            return
        if trigger_type != TriggerType.channel_join:
            return  # only channel_join has a "fire the campaign default" fallback
        async with AsyncSessionLocal() as db:
            camp = await db.get(Campaign, campaign_id)
        if not camp:
            return
        result = await fire_event(
            pixel_id=ch_account.meta_pixel_id or "", capi_token=ch_account.meta_capi_token or "",
            event_name=camp.event_type,
            user_data=build_user_data(telegram_id=user_id, fbclid=session.fbclid, client_ip=session.client_ip),
        )
        status = "error" if result.get("error") else "fired"
        async with AsyncSessionLocal() as db:
            db.add(ConversionLog(
                campaign_id=campaign_id, account_id=ch_account.id,
                trigger_type="channel_join", telegram_user_id=user_id,
                fbclid=session.fbclid, event_type=camp.event_type,
                status=ConversionStatus(status),
                error_detail=str(result.get("error")) if result.get("error") else None,
                meta_event_id=result.get("event_id"), fired_at=datetime.now(timezone.utc),
            ))
            if status == "fired":
                await db.execute(update(Campaign).where(Campaign.id == campaign_id)
                                 .values(total_conversions=Campaign.total_conversions + 1))
            await db.commit()

    @client.on(events.ChatAction)
    async def handle_channel_join(event):
        """Plain 'user joined the channel/group' — no join-request/approval needed."""
        try:
            if not (event.user_joined or event.user_added):
                return
            chat = await event.get_chat()
            ch_account = targets.get(getattr(chat, "id", None))
            if not ch_account:
                return
            user_id = event.user_id
            if user_id is None:
                return

            campaigns = await get_active_campaigns(ch_account.id)
            for camp in campaigns:
                # Try to recover attribution via central engine first (bot→channel journey)
                session = None
                try:
                    # check existing session
                    async with AsyncSessionLocal() as db:
                        r = await db.execute(select(UserSession).where(and_(UserSession.tg_user_id == user_id, UserSession.campaign_id == camp.id)))
                        session = r.scalar_one_or_none()
                    if not session:
                        # try attribution engine: any prior click for this identity?
                        from shared.attribution import resolve_attribution
                        attr = await resolve_attribution(ch_account.user_id, user_id)
                        if attr and attr.get("campaign_id") == camp.id:
                            click = attr["click"]
                            session = await get_or_create_session(camp.id, user_id, {"session_key": click.click_id, "fbclid": click.fbclid, "fbc": click.fbc, "fbp": click.fbp, "client_ip": click.ip, "user_agent": click.user_agent, "campaign_id": click.campaign_id}, None)
                            if session and not session.fbc and click.fbc:
                                session.fbc = click.fbc
                            if session and not session.fbp and click.fbp:
                                session.fbp = click.fbp
                        else:
                            # also try any click for this user (cross-campaign recovery)
                            if attr:
                                click = attr["click"]
                                session = await get_or_create_session(camp.id, user_id, {"session_key": click.click_id, "fbclid": click.fbclid, "fbc": click.fbc, "fbp": click.fbp, "client_ip": click.ip, "user_agent": click.user_agent}, None)
                except Exception as e:
                    logger.debug("attribution recovery failed: %s", e)
                if not session:
                    session = await get_or_create_session(camp.id, user_id, {"session_key": "organic"}, None)
                if not session:
                    continue
                await _fire_default_or_triggers(ch_account, camp.id, TriggerType.channel_join, session, user_id)

            logger.info("Channel join handled [channel_acct=%d user=%d]", ch_account.id, user_id)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.exception("handle_channel_join: %s", e)

    @client.on(events.NewMessage(incoming=True))
    async def handle_channel_message(event):
        """Keyword-trigger matching for messages posted IN the monitored group/channel."""
        try:
            if event.is_private:
                return  # DMs are handled separately by handle_inbound_dm
            ch_account = targets.get(event.chat_id)
            if not ch_account:
                return

            sender = await event.get_sender()
            tg_user_id = getattr(sender, "id", None)
            if tg_user_id is None or getattr(sender, "bot", False):
                return
            text = event.raw_text or ""

            campaigns = await get_active_campaigns(ch_account.id)
            for camp in campaigns:
                async with AsyncSessionLocal() as db:
                    r = await db.execute(select(UserSession).where(
                        and_(UserSession.tg_user_id == tg_user_id, UserSession.campaign_id == camp.id)
                    ))
                    session = r.scalar_one_or_none()
                if not session:
                    # try attribution recovery for channel messages too
                    try:
                        from shared.attribution import resolve_attribution as _ra
                        _attr = await _ra(ch_account.user_id, tg_user_id)
                        if _attr:
                            _cl = _attr["click"]
                            session = await get_or_create_session(camp.id, tg_user_id, {"session_key": _cl.click_id, "fbclid": _cl.fbclid, "fbc": _cl.fbc, "fbp": _cl.fbp, "client_ip": _cl.ip, "user_agent": _cl.user_agent}, sender)
                    except Exception:
                        pass
                    if not session:
                        session = await get_or_create_session(camp.id, tg_user_id, {"session_key": "organic"}, sender)
                    if not session:
                        continue

                kt = await get_triggers_for_campaign(camp.id, TriggerType.keyword)
                for trig in kt:
                    if _keyword_matches(text, trig.keywords, trig.match_mode):
                        await fire_trigger(trig, ch_account, session, tg_user_id, skip_if_fired=False)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.exception("handle_channel_message: %s", e)

    logger.info("Registered channel/group monitoring for %d linked account(s)", len(targets))


# Funnel step counts are now derived live from ConversionLog by the
# dashboard API (grouped by trigger_id) — there's nothing for the worker
# to write here anymore. Every trigger fire already goes through
# fire_trigger() / log_conversion(), which is the single source of truth
# both Conversions and Funnels read from.


# ── Account boot ──────────────────────────────────────────────────────

async def boot_account(account: TelegramAccount):
    # Acquire per-account lock to prevent concurrent boot attempts
    lock = _account_boot_locks.setdefault(account.id, asyncio.Lock())
    async with lock:
        for attempt in range(3):
            try:
                session_path = os.path.join(settings.SESSIONS_DIR,
                                             account.session_name or f"account_{account.id}")
                client = TelegramClient(session=session_path, api_id=settings.TELEGRAM_API_ID,
                                        api_hash=settings.TELEGRAM_API_HASH,
                                        proxy=_parse_proxy(account.proxy_string))

                if account.account_type == AccountType.BOT:
                    await client.start(bot_token=account.identifier)
                    await client.catch_up()
                elif account.account_type == AccountType.PERSONAL:
                    await client.connect()
                    if not await client.is_user_authorized():
                        logger.error("Account %d not authorized", account.id)
                        await client.disconnect()
                        await alert_admin(f"🔴 *Account #{account.id} ({account.label or 'PERSONAL'}) needs re-authentication.*")
                        # Mark as inactive to stop retries
                        await _mark_account_inactive(account.id)
                        return
                    await client.catch_up()
                elif account.account_type == AccountType.CHANNEL:
                    # CHANNEL type uses a linked personal account to monitor
                    # (TelegramAccount.monitor_account_id). The identifier is
                    # the channel/group username or invite link. No dedicated
                    # client is created here — actual event handlers get
                    # registered by _register_channel_handlers() once the
                    # linked monitor account boots (see the PERSONAL/BOT
                    # branch above, after _register_handlers()). If the
                    # linked account hasn't booted yet or has no
                    # monitor_account_id set, this channel silently tracks
                    # nothing — check the logs for "could not resolve".
                    logger.info("Channel account %d registered (monitoring via linked account, see logs for wiring)", account.id)
                    active_clients[account.id] = None  # marker
                    _reconnect_failures.pop(account.id, None)
                    if account.id in _alerted_down:
                        _alerted_down.discard(account.id)
                    return

                _register_handlers(client, account)
                active_clients[account.id] = client

                # Wire up any CHANNEL accounts that use THIS account to monitor
                # their channel/group — see _register_channel_handlers() above.
                # (Bots can be linked as monitors too, e.g. a bot that's an
                # admin in a group, not just personal accounts.)
                linked_channels = await get_channel_accounts_for_monitor(account.id)
                if linked_channels:
                    await _register_channel_handlers(client, linked_channels)

                logger.info("Booted [id=%d type=%s label=%s]",
                            account.id, account.account_type, account.label or "—")

                if account.id in _alerted_down:
                    _alerted_down.discard(account.id)
                    await alert_admin(f"🟢 *Account #{account.id} ({account.label or account.account_type}) is back online.*")
                _reconnect_failures.pop(account.id, None)
                return

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    wait = (attempt + 1) * 2
                    logger.warning("Account %d session locked, retry in %ds (attempt %d)",
                                   account.id, wait, attempt+1)
                    await asyncio.sleep(wait)
                    continue
                else:
                    raise
            except AuthKeyError:
                logger.error("Account %d: auth key revoked", account.id)
                await alert_admin(f"🔴 *Account #{account.id}: auth key revoked.* Re-connect from dashboard.")
                await _mark_account_inactive(account.id)
                return
            except UserDeactivatedError:
                logger.error("Account %d: deactivated", account.id)
                await alert_admin(f"🔴 *Account #{account.id}: banned/deactivated by Telegram.*")
                await _mark_account_inactive(account.id)
                return
            except FloodWaitError as e:
                logger.warning("Account %d: FloodWait %ds", account.id, e.seconds)
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.exception("Account %d boot failed (attempt %d): %s", account.id, attempt+1, e)
                await asyncio.sleep(5)



async def channel_reload_listener():
    """Listens for tg:reload_channels pub/sub to hot-reload channel wiring without restart."""
    try:
        import redis.asyncio as aioredis
        from shared.config import get_settings as _gs2
        r = aioredis.from_url(_gs2().REDIS_URL, encoding="utf-8", decode_responses=True)
        pubsub = r.pubsub()
        await pubsub.subscribe("tg:reload_channels")
        logger.info("Channel reload listener subscribed")
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            try:
                data = json.loads(msg.get("data") or "{}")
                logger.info("Reload channels signal received: %s", data)
                # re-wire all active PERSONAL/BOT clients
                for acct_id, client in list(active_clients.items()):
                    if not client:
                        continue
                    try:
                        from shared.models import TelegramAccount as _TA, AccountType as _AT
                        async with AsyncSessionLocal() as db:
                            from sqlalchemy import select as _select
                            res = await db.execute(_select(_TA).where(_TA.id == acct_id))
                            acct = res.scalar_one_or_none()
                            if not acct:
                                continue
                        linked = await get_channel_accounts_for_monitor(acct_id)
                        if linked:
                            await _register_channel_handlers(client, linked)
                            logger.info("Hot-reloaded %d channel handlers for monitor %d", len(linked), acct_id)
                    except Exception as e:
                        logger.warning("Hot reload failed for %d: %s", acct_id, e)
            except Exception as e:
                logger.warning("Reload listener error: %s", e)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning("Channel reload listener died: %s", e)
        await asyncio.sleep(5)

async def boot_all_accounts():
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(TelegramAccount).where(TelegramAccount.is_active == True))
        accounts = result.scalars().all()
    if not accounts:
        logger.warning("No active accounts in DB yet")
        return
    logger.info("Booting %d accounts...", len(accounts))
    await asyncio.gather(*[boot_account(a) for a in accounts], return_exceptions=True)
    logger.info("Boot complete — %d live clients", sum(1 for c in active_clients.values() if c))


async def heartbeat_loop():
    while True:
        await asyncio.sleep(60)
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(TelegramAccount).where(TelegramAccount.is_active == True))
                accounts = result.scalars().all()
            for acct in accounts:
                if acct.account_type == AccountType.CHANNEL:
                    continue  # no direct client to check
                client = active_clients.get(acct.id)
                if client and not client.is_connected():
                    logger.warning("Account %d dropped — reconnecting", acct.id)
                    active_clients.pop(acct.id, None)
                    await boot_account(acct)
                elif not client:
                    await boot_account(acct)
                if acct.id not in active_clients:
                    _reconnect_failures[acct.id] = _reconnect_failures.get(acct.id, 0) + 1
                    if _reconnect_failures[acct.id] >= _ALERT_AFTER_FAILURES and acct.id not in _alerted_down:
                        _alerted_down.add(acct.id)
                        await alert_admin(
                            f"🔴 *Account #{acct.id} ({acct.label or acct.account_type}) "
                            f"offline for ~{_reconnect_failures[acct.id]} minutes.*"
                        )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("Heartbeat error: %s", e)


async def process_signin_queue():
    logger.info("Sign-in queue processor started")
    pending_clients: dict[str, TelegramClient] = {}
    pending_hashes:  dict[str, str] = {}

    while True:
        try:
            # Use timeout=2 so the loop can check for cancellation regularly.
            # If Redis drops (e.g. Docker restart), blpop raises ConnectionError
            # which is caught below and retried after a short sleep.
            item = await redis.blpop("signin_queue", timeout=2)
            if not item:
                continue
            _, raw = item
            task = json.loads(raw)
            action = task.get("action")
            flow_token = task.get("flow_token")
            phone = task.get("phone")

            if action == "send_code":
                try:
                    session_path = os.path.join(settings.SESSIONS_DIR, f"temp_{flow_token}")
                    temp = TelegramClient(session=session_path, api_id=settings.TELEGRAM_API_ID,
                                         api_hash=settings.TELEGRAM_API_HASH)
                    await temp.connect()
                    result = await temp.send_code_request(phone)
                    pending_clients[flow_token] = temp
                    pending_hashes[flow_token] = result.phone_code_hash
                    await _update_signin(flow_token, {"status":"code_sent"})
                    logger.info("Code sent to %s***", phone[:7])
                except Exception as e:
                    await _update_signin(flow_token, {"status":"error","error":str(e)})

            elif action == "verify_code":
                temp = pending_clients.get(flow_token)
                p_hash = pending_hashes.get(flow_token)
                code = task.get("code","")
                if not temp or not p_hash:
                    await _update_signin(flow_token, {"status":"error","error":"Session expired"})
                    continue
                try:
                    await temp.sign_in(phone=phone, code=code, phone_code_hash=p_hash)
                    me = await temp.get_me()

                    # 🔽 CRITICAL FIX: Disconnect temp client before renaming/booting
                    await temp.disconnect()
                    pending_clients.pop(flow_token, None)
                    pending_hashes.pop(flow_token, None)

                    session_name = f"personal_{me.id}"
                    final_path = os.path.join(settings.SESSIONS_DIR, session_name + ".session")
                    temp_path  = os.path.join(settings.SESSIONS_DIR, f"temp_{flow_token}.session")
                    if os.path.exists(temp_path):
                        os.rename(temp_path, final_path)
                        # Also remove WAL/SHM files left by temp session
                        for ext in ["-wal", "-shm"]:
                            tmp_extra = temp_path + ext
                            final_extra = final_path + ext
                            if os.path.exists(tmp_extra):
                                try: os.rename(tmp_extra, final_extra)
                                except: pass

                    # Give OS time to release file locks before opening session
                    await asyncio.sleep(2)

                    async with AsyncSessionLocal() as db:
                        new_acct = TelegramAccount(
                            user_id=task.get("user_id"), account_type=AccountType.PERSONAL,
                            identifier=phone, session_name=session_name,
                            meta_pixel_id=task.get("meta_pixel_id") or None,
                            meta_capi_token=task.get("meta_capi_token") or None,
                            proxy_string=task.get("proxy_string"), is_active=True,
                        )
                        db.add(new_acct)
                        await db.commit()
                        await db.refresh(new_acct)
                        account_id = new_acct.id

                    await _update_signin(flow_token, {"status":"completed","account_id":account_id})
                    logger.info("Personal signed in: %s*** (id=%d)", phone[:7], me.id)
                    # Boot in background so it doesn't block the signin confirmation
                    asyncio.create_task(boot_account(new_acct))
                except PhoneCodeInvalidError:
                    await _update_signin(flow_token, {"status":"error","error":"Invalid code"})
                except PhoneCodeExpiredError:
                    await _update_signin(flow_token, {"status":"error","error":"Code expired — restart"})
                    pending_clients.pop(flow_token, None); pending_hashes.pop(flow_token, None)
                except SessionPasswordNeededError:
                    await _update_signin(flow_token, {"status":"error","error":"2FA enabled — disable it first"})
                except Exception as e:
                    await _update_signin(flow_token, {"status":"error","error":str(e)})

        except asyncio.CancelledError:
            break
        except Exception as e:
            if "Connection" in str(e) or "refused" in str(e) or "closed" in str(e):
                logger.warning("Signin queue: Redis disconnected (%s) — retrying in 5s", e)
                await asyncio.sleep(5)
            else:
                logger.exception("Signin queue error: %s", e)
                await asyncio.sleep(1)


async def _update_signin(flow_token: str, updates: dict):
    raw = await redis.get(f"signin:{flow_token}")
    if raw:
        state = json.loads(raw)
        state.update(updates)
        await redis.setex(f"signin:{flow_token}", 300, json.dumps(state))


async def main():
    global redis
    setup_logging("worker")
    validate_or_exit("worker")
    logger.info("=== TG Tracker Worker Engine v4 Starting ===")
    await init_db()
    redis = aioredis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("Redis connected")
    await boot_all_accounts()

    signin_task    = asyncio.create_task(process_signin_queue())
    heartbeat_task = asyncio.create_task(heartbeat_loop())
    reload_task    = asyncio.create_task(channel_reload_listener())

    stop_event = asyncio.Event()
    def _shutdown():
        logger.info("Shutdown signal — disconnecting...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        import signal
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _shutdown)
    except (NotImplementedError, AttributeError):
        pass

    logger.info("Worker running — %d live clients", sum(1 for c in active_clients.values() if c))
    try:
        tasks = [signin_task, heartbeat_task, asyncio.create_task(stop_event.wait())]
        tasks += [asyncio.create_task(c.run_until_disconnected())
                  for c in active_clients.values() if c]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except KeyboardInterrupt:
        pass
    finally:
        signin_task.cancel(); heartbeat_task.cancel(); reload_task.cancel()
        for c in active_clients.values():
            if c:
                try: await c.disconnect()
                except: pass
        if redis: await redis.aclose()
        logger.info("Worker stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
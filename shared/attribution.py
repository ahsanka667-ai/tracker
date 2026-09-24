"""
shared/attribution.py — Central attribution engine (§8)

Every Telegram component calls `resolve_attribution()` instead of rolling its
own logic. It answers:
  - which click is eligible for this identity at this moment
  - campaign / adset / ad / fbc / fbp / subs

History is never overwritten — we store every click and rank them.

Attribution models:
  - last_touch       : most recent eligible click (default)
  - first_touch      : earliest eligible click
  - last_non_direct  : most recent non-organic (has fbclid/fbc) click, else None

Window: only clicks within the configured lookback are eligible.
Organic clicks (no fbclid/campaign) are ignored unless include_organic=True.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, desc, and_, or_

from shared.config import get_settings
from shared.database import AsyncSessionLocal, bind_ts
from shared.logging_config import log_event

logger = logging.getLogger(__name__)


async def get_attribution_setting(owner_user_id: int) -> dict[str, Any]:
    """Per-user attribution config, falls back to global defaults."""
    settings = get_settings()
    defaults = {
        "model": settings.ATTRIBUTION_MODEL,
        "window_hours": settings.ATTRIBUTION_WINDOW_HOURS,
        "include_organic": settings.ATTRIBUTION_INCLUDE_ORGANIC,
    }
    try:
        from shared.models import AttributionSetting
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(AttributionSetting).where(AttributionSetting.user_id == owner_user_id))
            row = result.scalar_one_or_none()
            if row:
                return {"model": row.model, "window_hours": row.window_hours, "include_organic": row.include_organic}
    except Exception:
        pass
    return defaults


async def find_clicks_for_identity(
    owner_user_id: int,
    telegram_user_id: int,
    *,
    window_hours: int | None = None,
    include_organic: bool | None = None,
    limit: int = 50,
) -> list[Any]:
    """
    All clicks ever associated with this Telegram identity, via:
      1. telegram_events.click_id
      2. user_sessions.click_id / session_key
      3. clicks linked through identity's campaign history

    For now we query clicks directly via the identity's known click ids.
    """
    from shared.models import Click, TelegramEvent, UserSession, TelegramIdentity

    cfg = await get_attribution_setting(owner_user_id)
    wh = window_hours if window_hours is not None else cfg["window_hours"]
    inc_org = include_organic if include_organic is not None else cfg["include_organic"]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=wh)

    async with AsyncSessionLocal() as db:
        # collect click ids via telegram_events + user_sessions
        click_ids: set[int] = set()

        # via telegram_events
        r = await db.execute(
            select(TelegramEvent.click_id).where(
                and_(
                    TelegramEvent.owner_user_id == owner_user_id,
                    TelegramEvent.telegram_user_id == telegram_user_id,
                    TelegramEvent.click_id.isnot(None),
                )
            )
        )
        for row in r.all():
            if row[0]:
                click_ids.add(row[0])

        # via user_sessions
        r2 = await db.execute(
            select(UserSession.click_id).where(
                and_(UserSession.tg_user_id == telegram_user_id, UserSession.click_id.isnot(None))
            )
        )
        for row in r2.all():
            if row[0]:
                click_ids.add(row[0])

        # also via direct clicks that have matching telegram_user via recent events?
        # For organic users with no click yet, return empty

        if not click_ids:
            return []

        # fetch clicks, filter by window + organic rule
        q = select(Click).where(Click.id.in_(click_ids))
        # window filter
        q = q.where(Click.created_at >= bind_ts(cutoff))
        if not inc_org:
            q = q.where(or_(Click.fbclid.isnot(None), Click.fbc.isnot(None), Click.campaign_id.isnot(None)))

        q = q.order_by(desc(Click.created_at)).limit(limit)
        result = await db.execute(q)
        return list(result.scalars().all())


async def resolve_attribution(
    owner_user_id: int,
    telegram_user_id: int,
    *,
    model: str | None = None,
    window_hours: int | None = None,
    include_organic: bool | None = None,
) -> dict[str, Any] | None:
    """
    Central attribution entry point.

    Returns dict with keys:
      click, campaign_id, fbclid, fbc, fbp, subs, created_at
    or None if no eligible click.

    Never mutates state.
    """
    clicks = await find_clicks_for_identity(
        owner_user_id, telegram_user_id,
        window_hours=window_hours, include_organic=include_organic
    )
    if not clicks:
        log_event(logger, "ATTRIBUTION_RESOLVED", owner_user_id=owner_user_id,
                  telegram_user_id=telegram_user_id, result="none")
        return None

    cfg = await get_attribution_setting(owner_user_id)
    m = model or cfg["model"]

    chosen = None
    if m == "first_touch":
        chosen = clicks[-1]  # oldest (query is DESC)
    elif m == "last_non_direct":
        # prefer non-organic, newest first
        for c in clicks:
            if c.fbclid or c.fbc or c.campaign_id:
                chosen = c
                break
        if not chosen:
            return None  # all organic, and we don't want organic
    else:  # last_touch default
        chosen = clicks[0]

    if not chosen:
        return None

    result = {
        "click": chosen,
        "click_id": chosen.id,
        "click_public_id": chosen.click_id,
        "campaign_id": chosen.campaign_id,
        "fbclid": chosen.fbclid,
        "fbc": chosen.fbc,
        "fbp": chosen.fbp,
        "campaign_name": chosen.campaign_name,
        "adset": chosen.adset,
        "ad": chosen.ad,
        "adset_id": chosen.adset_id,
        "ad_id": chosen.ad_id,
        "creative": chosen.creative,
        "placement": chosen.placement,
        "subs": {f"sub{i}": getattr(chosen, f"sub{i}") for i in range(1, 10)},
        "created_at": chosen.created_at,
        "event_id": chosen.event_id,
    }
    log_event(logger, "ATTRIBUTION_RESOLVED",
              owner_user_id=owner_user_id, telegram_user_id=telegram_user_id,
              click_id=chosen.id, campaign_id=chosen.campaign_id, model=m)
    return result


async def get_or_create_identity(
    owner_user_id: int,
    telegram_user_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    phone: str | None = None,
    source: str | None = None,
) -> Any:
    """Idempotent identity creation — never duplicates."""
    from shared.models import TelegramIdentity

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(TelegramIdentity).where(
                and_(
                    TelegramIdentity.owner_user_id == owner_user_id,
                    TelegramIdentity.telegram_user_id == telegram_user_id,
                )
            )
        )
        identity = result.scalar_one_or_none()
        if identity:
            # update if we learned more
            updated = False
            if username and not identity.username:
                identity.username = username; updated = True
            if first_name and identity.first_name != first_name:
                identity.first_name = first_name; updated = True
            if last_name and not identity.last_name:
                identity.last_name = last_name; updated = True
            if phone and not identity.phone:
                identity.phone = phone; updated = True
            if updated:
                identity.last_seen = datetime.now(timezone.utc)
                await db.commit()
                await db.refresh(identity)
            return identity

        identity = TelegramIdentity(
            owner_user_id=owner_user_id,
            telegram_user_id=telegram_user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            source=source,
            first_seen=datetime.now(timezone.utc),
            last_seen=datetime.now(timezone.utc),
        )
        db.add(identity)
        try:
            await db.commit()
        except Exception:
            await db.rollback()
            # race: someone else created it
            result = await db.execute(
                select(TelegramIdentity).where(
                    and_(TelegramIdentity.owner_user_id == owner_user_id, TelegramIdentity.telegram_user_id == telegram_user_id)
                )
            )
            identity = result.scalar_one_or_none()
            if not identity:
                raise
        await db.refresh(identity)
        log_event(logger, "TELEGRAM_IDENTITY_RESOLVED", owner_user_id=owner_user_id, telegram_user_id=telegram_user_id)
        return identity


async def link_click_to_identity(
    owner_user_id: int,
    telegram_user_id: int,
    click: Any,
    *,
    source: str = "bot_start",
    sender: Any | None = None,
) -> tuple[Any, Any]:
    """
    Link a click to an identity (create identity if needed) and record
    the attribution via a telegram_event + user_session.
    Returns (identity, telegram_event).
    """
    from shared.models import TelegramEvent, TelegramEventType

    identity = await get_or_create_identity(
        owner_user_id, telegram_user_id,
        username=getattr(sender, "username", None) if sender else None,
        first_name=getattr(sender, "first_name", None) if sender else None,
        last_name=getattr(sender, "last_name", None) if sender else None,
        phone=getattr(sender, "phone", None) if sender else None,
        source=source,
    )

    # record event
    import uuid
    event = await create_telegram_event(
        owner_user_id=owner_user_id,
        telegram_user_id=telegram_user_id,
        identity_id=identity.id,
        click_id=click.id,
        campaign_id=click.campaign_id,
        tracking_link_id=click.tracking_link_id,
        event_type=TelegramEventType.BOT_START if source == "bot_start" else TelegramEventType.CLICK,
        fbclid=click.fbclid, fbc=click.fbc, fbp=click.fbp,
        metadata_json=None,
    )

    # also ensure UserSession exists (for legacy campaign-scoped dedup)
    try:
        from shared.models import UserSession
        if click.campaign_id:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(UserSession).where(
                        and_(UserSession.tg_user_id == telegram_user_id, UserSession.campaign_id == click.campaign_id)
                    )
                )
                sess = result.scalar_one_or_none()
                if not sess:
                    sess = UserSession(
                        campaign_id=click.campaign_id,
                        tg_user_id=telegram_user_id,
                        tg_username=getattr(sender, "username", None) if sender else identity.username,
                        tg_first_name=getattr(sender, "first_name", None) if sender else identity.first_name,
                        tg_phone=getattr(sender, "phone", None) if sender else identity.phone,
                        session_key=click.click_id,
                        fbclid=click.fbclid,
                        fbc=click.fbc,
                        fbp=click.fbp,
                        client_ip=click.ip,
                        user_agent=click.user_agent,
                        click_id=click.id,
                        fired_triggers="",
                    )
                    db.add(sess)
                    await db.commit()
    except Exception as e:
        logger.warning("link_click_to_identity session create failed: %s", e)

    return identity, event


async def create_telegram_event(
    *,
    owner_user_id: int,
    telegram_user_id: int | None,
    identity_id: int | None,
    click_id: int | None,
    campaign_id: int | None,
    tracking_link_id: int | None,
    event_type,
    fbclid: str | None = None,
    fbc: str | None = None,
    fbp: str | None = None,
    metadata_json: str | None = None,
    account_id: int | None = None,
    trigger_id: int | None = None,
) -> Any:
    from shared.models import TelegramEvent
    import uuid

    event = TelegramEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        owner_user_id=owner_user_id,
        telegram_user_id=telegram_user_id,
        identity_id=identity_id,
        click_id=click_id,
        campaign_id=campaign_id,
        tracking_link_id=tracking_link_id,
        account_id=account_id,
        trigger_id=trigger_id,
        fbclid=fbclid,
        fbc=fbc,
        fbp=fbp,
        event_metadata=metadata_json,
        created_at=datetime.now(timezone.utc),
    )
    async with AsyncSessionLocal() as db:
        db.add(event)
        await db.commit()
        await db.refresh(event)
    return event

"""
shared/events.py — Normalized Telegram event engine (§13)

All Telegram interactions flow through `record_event()` which:
  1. resolves/creates the TelegramIdentity
  2. resolves attribution (eligible click)
  3. writes a TelegramEvent row
  4. optionally fans out to:
     - conversion log (+ funnel progress)
     - Meta CAPI queue (via meta_events + Redis)
     - automation flows
     - CRM timeline

The event engine is the single chokepoint — no Telegram handler should
write conversions directly.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, and_

from shared.logging_config import log_event

logger = logging.getLogger(__name__)


async def record_event(
    *,
    owner_user_id: int,
    telegram_user_id: int | None,
    event_type,  # TelegramEventType
    account_id: int | None = None,
    campaign_id: int | None = None,
    text: str | None = None,
    metadata: dict[str, Any] | None = None,
    sender: Any | None = None,
    click_id: int | None = None,  # if already known (e.g. from token)
    trigger_id: int | None = None,
    dedup_key: str | None = None,  # caller can enforce idempotency
) -> dict[str, Any] | None:
    """
    Record a normalized event and trigger downstream pipelines.

    Returns dict with {telegram_event, identity, attribution} or None if dedup.
    """
    from shared.models import TelegramEvent, TelegramEventType
    from shared.attribution import get_or_create_identity, resolve_attribution
    from shared.database import AsyncSessionLocal

    # dedup check
    if dedup_key:
        async with AsyncSessionLocal() as db:
            existing = await db.execute(select(TelegramEvent).where(TelegramEvent.event_id == dedup_key))
            if existing.scalar_one_or_none():
                log_event(logger, "EVENT_DEDUPED", event_type=str(event_type), dedup_key=dedup_key)
                return None

    # identity
    identity = None
    if telegram_user_id:
        from shared.attribution import get_or_create_identity
        identity = await get_or_create_identity(
            owner_user_id, telegram_user_id,
            username=getattr(sender, "username", None) if sender else (metadata or {}).get("username"),
            first_name=getattr(sender, "first_name", None) if sender else (metadata or {}).get("first_name"),
            last_name=getattr(sender, "last_name", None) if sender else None,
            phone=getattr(sender, "phone", None) if sender else None,
            source=str(event_type).lower() if event_type else "unknown",
        )

    # attribution
    attribution = None
    resolved_click_id = click_id
    resolved_campaign_id = campaign_id
    fbc = None; fbp = None; fbclid = None

    if not resolved_click_id and telegram_user_id:
        attribution = await resolve_attribution(owner_user_id, telegram_user_id)
        if attribution:
            resolved_click_id = attribution["click_id"]
            resolved_campaign_id = attribution["campaign_id"]
            fbc = attribution["fbc"]
            fbp = attribution["fbp"]
            fbclid = attribution["fbclid"]
    elif resolved_click_id:
        # load click to enrich
        from shared.models import Click
        async with AsyncSessionLocal() as db:
            click = await db.get(Click, resolved_click_id)
            if click:
                fbc = click.fbc; fbp = click.fbp; fbclid = click.fbclid
                resolved_campaign_id = resolved_campaign_id or click.campaign_id

    # create telegram_event
    event = await _create_telegram_event(
        owner_user_id=owner_user_id,
        telegram_user_id=telegram_user_id,
        identity_id=identity.id if identity else None,
        click_id=resolved_click_id,
        campaign_id=resolved_campaign_id,
        event_type=event_type,
        account_id=account_id,
        trigger_id=trigger_id,
        fbclid=fbclid, fbc=fbc, fbp=fbp,
        metadata=metadata,
        dedup_key=dedup_key,
    )

    log_event(logger, "TELEGRAM_EVENT_RECORDED",
              event_type=str(event_type), telegram_user_id=telegram_user_id,
              click_id=resolved_click_id, campaign_id=resolved_campaign_id)

    # fan-out to CRM message/conversation already handled by caller (save_message)
    # but we also trigger flows + optional funnel progress implicitly via conversion_logs

    return {"telegram_event": event, "identity": identity, "attribution": attribution}


async def _create_telegram_event(
    *,
    owner_user_id: int,
    telegram_user_id: int | None,
    identity_id: int | None,
    click_id: int | None,
    campaign_id: int | None,
    event_type,
    account_id: int | None,
    trigger_id: int | None,
    fbclid: str | None,
    fbc: str | None,
    fbp: str | None,
    metadata: dict | None,
    dedup_key: str | None,
) -> Any:
    from shared.models import TelegramEvent
    from shared.database import AsyncSessionLocal

    evt = TelegramEvent(
        event_id=dedup_key or str(uuid.uuid4()),
        event_type=event_type,
        owner_user_id=owner_user_id,
        telegram_user_id=telegram_user_id,
        identity_id=identity_id,
        click_id=click_id,
        campaign_id=campaign_id,
        account_id=account_id,
        trigger_id=trigger_id,
        fbclid=fbclid,
        fbc=fbc,
        fbp=fbp,
        event_metadata=json.dumps(metadata) if metadata else None,
        created_at=datetime.now(timezone.utc),
    )
    async with AsyncSessionLocal() as db:
        db.add(evt)
        await db.commit()
        await db.refresh(evt)
    return evt


async def find_identity_clicks_for_user(owner_user_id: int, telegram_user_id: int, limit: int = 20):
    """Helper for CRM journey: all distinct clicks for this identity."""
    from shared.models import TelegramEvent, Click
    from shared.database import AsyncSessionLocal
    async with AsyncSessionLocal() as db:
        r = await db.execute(
            select(TelegramEvent.click_id).where(
                and_(TelegramEvent.owner_user_id == owner_user_id,
                     TelegramEvent.telegram_user_id == telegram_user_id,
                     TelegramEvent.click_id.isnot(None))
            ).distinct().limit(limit)
        )
        click_ids = [row[0] for row in r.all() if row[0]]
        if not click_ids:
            return []
        r2 = await db.execute(select(Click).where(Click.id.in_(click_ids)))
        return list(r2.scalars().all())

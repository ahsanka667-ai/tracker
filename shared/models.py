"""
shared/models.py — Full SQLAlchemy ORM models v4
New in v4:
  - AccountType.CHANNEL for channel/group monitoring
  - ConversionTrigger — per-campaign trigger rules (what fires what event)
  - UserSession — links a Telegram user to an ad click across multiple steps
  - WebhookToken — for inbound /webhook/{token} conversion endpoint
"""
import enum
from datetime import datetime
from sqlalchemy import (
    BigInteger, Boolean, DateTime, Enum, Float,
    ForeignKey, Index, Integer, String, Text, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from shared.database import Base


# ── Enums ─────────────────────────────────────────────────────────────

class AccountType(str, enum.Enum):
    BOT      = "BOT"
    PERSONAL = "PERSONAL"
    CHANNEL  = "CHANNEL"   # NEW: channel / group monitoring


class EventType(str, enum.Enum):
    Lead                  = "Lead"
    CompleteRegistration  = "CompleteRegistration"
    Subscribe             = "Subscribe"
    Purchase              = "Purchase"
    ViewContent           = "ViewContent"
    InitiateCheckout      = "InitiateCheckout"
    Contact               = "Contact"
    CustomizeProduct      = "CustomizeProduct"
    FindLocation          = "FindLocation"
    AddToCart             = "AddToCart"
    AddPaymentInfo        = "AddPaymentInfo"
    StartTrial            = "StartTrial"
    Schedule              = "Schedule"


class ConversionStatus(str, enum.Enum):
    fired   = "fired"
    error   = "error"
    skipped = "skipped"


class MessageDirection(str, enum.Enum):
    inbound  = "inbound"
    outbound = "outbound"


class TriggerType(str, enum.Enum):
    """What Telegram event fires a conversion."""
    click          = "click"           # URL hit on /t/{slug}
    bot_start      = "bot_start"       # /start <key> received by bot
    first_message  = "first_message"   # first DM from user (not /start)
    any_message    = "any_message"     # any DM from tracked user
    keyword        = "keyword"         # message contains configured keyword(s)
    channel_join   = "channel_join"    # user joins a channel/group
    manual         = "manual"          # admin fires from dashboard
    webhook        = "webhook"         # external POST to /webhook/{token}


# ── TABLE 1: dashboard_users ──────────────────────────────────────────
class DashboardUser(Base):
    __tablename__ = "dashboard_users"

    telegram_id: Mapped[int]          = mapped_column(BigInteger, primary_key=True)
    username:    Mapped[str | None]   = mapped_column(String(64),  nullable=True)
    first_name:  Mapped[str]          = mapped_column(String(128), nullable=False)
    is_active:   Mapped[bool]         = mapped_column(Boolean, default=True,  nullable=False)
    is_admin:    Mapped[bool]         = mapped_column(Boolean, default=False, nullable=False)
    created_at:  Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_login:  Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Username/password login (no Telegram account/bot needed to sign in).
    # telegram_id doubles as the row's identity for users created this way —
    # they get a synthetic NEGATIVE id (real Telegram ids are always
    # positive), so both login paths can share one table/session mechanism.
    login_username: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    password_hash:  Mapped[str | None] = mapped_column(String(256), nullable=True)

    accounts:  Mapped[list["TelegramAccount"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    campaigns: Mapped[list["Campaign"]]        = relationship(back_populates="user", cascade="all, delete-orphan")
    funnels:   Mapped[list["Funnel"]]          = relationship(back_populates="user", cascade="all, delete-orphan")


# ── TABLE 2: access_tokens ────────────────────────────────────────────
class AccessToken(Base):
    __tablename__ = "access_tokens"

    id:         Mapped[int]             = mapped_column(Integer, primary_key=True, autoincrement=True)
    token:      Mapped[str]             = mapped_column(String(64), unique=True, nullable=False)
    created_by: Mapped[int]             = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime]        = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_used:    Mapped[bool]            = mapped_column(Boolean, default=False, nullable=False)
    used_by:    Mapped[int | None]      = mapped_column(BigInteger, nullable=True)
    used_at:    Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ── TABLE 3: telegram_accounts ────────────────────────────────────────
class TelegramAccount(Base):
    __tablename__ = "telegram_accounts"

    id:              Mapped[int]         = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id:         Mapped[int]         = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    account_type:    Mapped[AccountType] = mapped_column(Enum(AccountType), nullable=False)
    identifier:      Mapped[str]         = mapped_column(String(256), nullable=False)  # bot token / phone / @channel
    session_name:    Mapped[str | None]  = mapped_column(String(256), nullable=True)
    meta_pixel_id:   Mapped[str | None]  = mapped_column(String(64),  nullable=True)
    meta_capi_token: Mapped[str | None]  = mapped_column(Text,        nullable=True)
    proxy_string:    Mapped[str | None]  = mapped_column(String(512), nullable=True)
    label:           Mapped[str | None]  = mapped_column(String(128), nullable=True)
    # CHANNEL-specific: the linked personal account used to monitor it
    monitor_account_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="SET NULL"), nullable=True)
    is_active:       Mapped[bool]        = mapped_column(Boolean, default=True, nullable=False)
    created_at:      Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now())

    user:      Mapped["DashboardUser"]  = relationship(back_populates="accounts")
    campaigns: Mapped[list["Campaign"]] = relationship(back_populates="account")
    messages:  Mapped[list["Message"]]  = relationship(back_populates="account", cascade="all, delete-orphan")


# ── TABLE 4: campaigns ────────────────────────────────────────────────
class Campaign(Base):
    __tablename__ = "campaigns"

    id:                       Mapped[int]       = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id:                  Mapped[int]       = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    account_id:               Mapped[int]       = mapped_column(Integer,    ForeignKey("telegram_accounts.id",        ondelete="CASCADE"))
    name:                     Mapped[str]       = mapped_column(String(256), nullable=False)
    slug:                     Mapped[str]       = mapped_column(String(64),  unique=True, nullable=False)
    target_telegram_username: Mapped[str]       = mapped_column(String(256), nullable=False)
    event_type:               Mapped[EventType] = mapped_column(Enum(EventType), nullable=False, default=EventType.Lead)
    is_active:                Mapped[bool]      = mapped_column(Boolean, default=True, nullable=False)
    created_at:               Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now())
    total_clicks:             Mapped[int]       = mapped_column(Integer, default=0, nullable=False)
    total_conversions:        Mapped[int]       = mapped_column(Integer, default=0, nullable=False)

    user:     Mapped["DashboardUser"]           = relationship(back_populates="campaigns")
    account:  Mapped["TelegramAccount"]         = relationship(back_populates="campaigns")
    logs:     Mapped[list["ConversionLog"]]     = relationship(back_populates="campaign", cascade="all, delete-orphan")
    triggers: Mapped[list["ConversionTrigger"]] = relationship(back_populates="campaign", cascade="all, delete-orphan",
                                                                order_by="ConversionTrigger.trigger_order")
    sessions: Mapped[list["UserSession"]]       = relationship(back_populates="campaign", cascade="all, delete-orphan")
    webhook_tokens: Mapped[list["WebhookToken"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")


# ── TABLE 5: conversion_triggers ─────────────────────────────────────
class ConversionTrigger(Base):
    """
    One row per trigger rule on a campaign.
    A campaign can have multiple triggers (e.g. /start fires Lead,
    keyword fires Purchase). Each fires independently when its
    condition is met.
    """
    __tablename__ = "conversion_triggers"

    id:           Mapped[int]         = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id:  Mapped[int]         = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    trigger_order: Mapped[int]        = mapped_column(Integer, default=0, nullable=False)  # display order
    trigger_type: Mapped[TriggerType] = mapped_column(Enum(TriggerType), nullable=False)
    is_active:    Mapped[bool]        = mapped_column(Boolean, default=True, nullable=False)

    # What CAPI event this trigger fires
    event_name:   Mapped[str]         = mapped_column(String(64), nullable=False)  # e.g. "Lead", "Purchase"

    # Keyword trigger: comma-separated list of keywords
    keywords:     Mapped[str | None]  = mapped_column(Text, nullable=True)
    # "any" (default) = fires if ANY keyword matches (OR).
    # "all" = fires only if EVERY keyword is present in the same message (AND).
    match_mode:   Mapped[str]         = mapped_column(String(8), default="any", nullable=False)

    # Meta CAPI custom_data fields for this trigger
    value:        Mapped[float | None]  = mapped_column(Float,        nullable=True)   # Purchase value
    currency:     Mapped[str | None]    = mapped_column(String(8),    nullable=True)   # "USD", "BDT", etc.
    content_name: Mapped[str | None]    = mapped_column(String(256),  nullable=True)   # product/offer name
    content_ids:  Mapped[str | None]    = mapped_column(String(512),  nullable=True)   # comma-separated SKUs
    custom_data_json: Mapped[str | None] = mapped_column(Text,        nullable=True)   # any extra k/v as JSON

    campaign: Mapped["Campaign"] = relationship(back_populates="triggers")


# ── TABLE 6: user_sessions ────────────────────────────────────────────
class UserSession(Base):
    """
    Links a Telegram user to a specific ad click (session_key),
    so that later conversion events (keyword, channel join) can be
    attributed back to the original campaign + fbclid even though
    they happen long after the initial /start.
    """
    __tablename__ = "user_sessions"

    id:              Mapped[int]       = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id:     Mapped[int]       = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    tg_user_id:      Mapped[int]       = mapped_column(BigInteger, nullable=False)
    tg_username:     Mapped[str|None]  = mapped_column(String(64), nullable=True)
    tg_first_name:   Mapped[str|None]  = mapped_column(String(128), nullable=True)
    tg_phone:        Mapped[str|None]  = mapped_column(String(32),  nullable=True)  # if available
    session_key:     Mapped[str]       = mapped_column(String(64),  nullable=False)  # original click key
    fbclid:          Mapped[str|None]  = mapped_column(String(256), nullable=True)
    client_ip:       Mapped[str|None]  = mapped_column(String(64),  nullable=True)
    user_agent:      Mapped[str|None]  = mapped_column(Text,        nullable=True)
    first_seen_at:   Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at:    Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    # Bitmask of which trigger types have already fired for this user
    # Prevents firing the same trigger type twice for one user/campaign
    fired_triggers:  Mapped[str]       = mapped_column(String(256), default="", nullable=False)

    campaign: Mapped["Campaign"] = relationship(back_populates="sessions")

    __table_args__ = (
        Index("idx_us_tg_campaign", "tg_user_id", "campaign_id", unique=True),
        Index("idx_us_session_key", "session_key"),
    )


# ── TABLE 7: conversion_logs ──────────────────────────────────────────
class ConversionLog(Base):
    __tablename__ = "conversion_logs"

    id:                Mapped[int]              = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id:       Mapped[int]              = mapped_column(Integer, ForeignKey("campaigns.id",         ondelete="CASCADE"))
    account_id:        Mapped[int]              = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    trigger_id:        Mapped[int | None]       = mapped_column(Integer, ForeignKey("conversion_triggers.id", ondelete="SET NULL"), nullable=True)
    trigger_type:      Mapped[str | None]       = mapped_column(String(32), nullable=True)   # for display
    telegram_user_id:  Mapped[int | None]       = mapped_column(BigInteger, nullable=True)
    telegram_username: Mapped[str | None]       = mapped_column(String(64), nullable=True)
    fbclid:            Mapped[str | None]       = mapped_column(String(256), nullable=True)
    client_ip:         Mapped[str | None]       = mapped_column(String(64),  nullable=True)
    user_agent:        Mapped[str | None]       = mapped_column(Text,        nullable=True)
    event_type:        Mapped[str]              = mapped_column(String(64),  nullable=False)
    event_value:       Mapped[float | None]     = mapped_column(Float,       nullable=True)   # Purchase value
    event_currency:    Mapped[str | None]       = mapped_column(String(8),   nullable=True)
    content_name:      Mapped[str | None]       = mapped_column(String(256), nullable=True)
    status:            Mapped[ConversionStatus] = mapped_column(Enum(ConversionStatus), nullable=False, default=ConversionStatus.fired)
    error_detail:      Mapped[str | None]       = mapped_column(Text, nullable=True)
    meta_event_id:     Mapped[str | None]       = mapped_column(String(64),  nullable=True)   # UUID sent for dedup
    fbtrace_id:        Mapped[str | None]       = mapped_column(String(64),  nullable=True)   # Meta's trace ID
    fired_at:          Mapped[datetime]         = mapped_column(DateTime(timezone=True), server_default=func.now())

    campaign: Mapped["Campaign"] = relationship(back_populates="logs")

    __table_args__ = (
        Index("idx_conv_campaign",  "campaign_id"),
        Index("idx_conv_fired_at",  "fired_at"),
        Index("idx_conv_tg_user",   "telegram_user_id"),
        Index("idx_conv_trigger",   "trigger_type"),
    )


# ── TABLE 8: webhook_tokens ───────────────────────────────────────────
class WebhookToken(Base):
    """
    Gives external systems (payment gateways, CRMs, ClickFunnels)
    a secret URL to POST conversion events to.
    URL: POST /webhook/{token}
    """
    __tablename__ = "webhook_tokens"

    id:          Mapped[int]       = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int]       = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    token:       Mapped[str]       = mapped_column(String(64), unique=True, nullable=False)
    label:       Mapped[str|None]  = mapped_column(String(128), nullable=True)
    is_active:   Mapped[bool]      = mapped_column(Boolean, default=True, nullable=False)
    created_at:  Mapped[datetime]  = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_hit_at: Mapped[datetime|None] = mapped_column(DateTime(timezone=True), nullable=True)
    hit_count:   Mapped[int]       = mapped_column(Integer, default=0, nullable=False)

    campaign: Mapped["Campaign"] = relationship(back_populates="webhook_tokens")


# ── TABLE 9: messages ─────────────────────────────────────────────────
class Message(Base):
    __tablename__ = "messages"

    id:            Mapped[int]              = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id:    Mapped[int]              = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    tg_message_id: Mapped[int|None]         = mapped_column(BigInteger, nullable=True)
    tg_chat_id:    Mapped[int]              = mapped_column(BigInteger, nullable=False)
    tg_user_id:    Mapped[int|None]         = mapped_column(BigInteger, nullable=True)
    tg_username:   Mapped[str|None]         = mapped_column(String(64), nullable=True)
    tg_first_name: Mapped[str|None]         = mapped_column(String(128), nullable=True)
    direction:     Mapped[MessageDirection] = mapped_column(Enum(MessageDirection), nullable=False, default=MessageDirection.inbound)
    text:          Mapped[str|None]         = mapped_column(Text, nullable=True)
    is_read:       Mapped[bool]             = mapped_column(Boolean, default=False, nullable=False)
    campaign_id:   Mapped[int|None]         = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    received_at:   Mapped[datetime]         = mapped_column(DateTime(timezone=True), server_default=func.now())

    account: Mapped["TelegramAccount"] = relationship(back_populates="messages")

    __table_args__ = (
        Index("idx_msg_account",  "account_id"),
        Index("idx_msg_chat",     "tg_chat_id"),
        Index("idx_msg_received", "received_at"),
    )


# ── TABLE 10: funnels ─────────────────────────────────────────────────
class Funnel(Base):
    __tablename__ = "funnels"

    id:          Mapped[int]      = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id:     Mapped[int]      = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    campaign_id: Mapped[int]      = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    name:        Mapped[str]      = mapped_column(String(256), nullable=False)
    description: Mapped[str|None] = mapped_column(Text, nullable=True)
    # Auto-generated funnels are rebuilt whenever the campaign's triggers
    # change (add/remove/reorder a trigger → funnel steps follow automatically).
    # A user who edits steps manually has that edit preserved — is_default
    # flips to False on first manual edit, after which auto-rebuild stops
    # touching this funnel.
    is_default:  Mapped[bool]     = mapped_column(Boolean, default=False, nullable=False)
    is_active:   Mapped[bool]     = mapped_column(Boolean, default=True, nullable=False)
    created_at:  Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user:    Mapped["DashboardUser"]    = relationship(back_populates="funnels")
    steps:   Mapped[list["FunnelStep"]] = relationship(back_populates="funnel", cascade="all, delete-orphan", order_by="FunnelStep.step_order")

    __table_args__ = (Index("idx_funnel_campaign", "campaign_id"),)


# ── TABLE 11: funnel_steps ────────────────────────────────────────────
class FunnelStep(Base):
    """
    Each step points at one specific ConversionTrigger row — not a loose
    trigger_type string. This matters because a campaign can have several
    triggers of the same type (e.g. two different keyword triggers: one
    firing InitiateCheckout on "interested,price", another firing
    Purchase on "paid,done"). Linking by trigger_id means a funnel step
    unambiguously means "count of times THIS configured trigger fired" —
    the exact same number you'd see filtering the Conversions tab by
    that trigger, never a different aggregate.
    """
    __tablename__ = "funnel_steps"

    id:         Mapped[int]      = mapped_column(Integer, primary_key=True, autoincrement=True)
    funnel_id:  Mapped[int]      = mapped_column(Integer, ForeignKey("funnels.id", ondelete="CASCADE"))
    trigger_id: Mapped[int]      = mapped_column(Integer, ForeignKey("conversion_triggers.id", ondelete="CASCADE"))
    step_order: Mapped[int]      = mapped_column(Integer, nullable=False)
    # Optional override — if blank, display falls back to the trigger's
    # own event_name / trigger_type for the step label.
    label:      Mapped[str|None] = mapped_column(String(128), nullable=True)

    funnel:  Mapped["Funnel"]           = relationship(back_populates="steps")
    trigger: Mapped["ConversionTrigger"] = relationship()

    __table_args__ = (
        Index("idx_fs_funnel", "funnel_id"),
        Index("idx_fs_trigger", "trigger_id"),
    )


# Funnel step counts are derived live from ConversionLog (grouped by
# trigger_id and distinct telegram_user_id) — there is no separate
# "funnel completion" table to keep in sync. One source of truth: every
# trigger fire is already recorded in ConversionLog when it happens.

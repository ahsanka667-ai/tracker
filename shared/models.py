"""
shared/models.py — Full SQLAlchemy ORM models v5

Preserves all v4 tables exactly (no breaking migration) and adds the
production schema required by the spec:

  NEW TABLES:
    tracking_domains, tracking_links, clicks,
    telegram_identities, telegram_events,
    meta_pixels, meta_events,
    tags, contact_tags, contacts (CRM sugar), conversations,
    flows, flow_nodes, flow_edges, flow_runs,
    api_keys, attribution_settings

  EXTENDED COLUMNS (nullable, safe to migrate):
    clicks fields, identity linkage, privacy fields, etc.

All tables are created by init_db() on first run and by scripts/migrate.py
on existing installs.
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Enum, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.database import Base, ts_column, now_server_default

# ── Enums ─────────────────────────────────────────────────────────────

class AccountType(str, enum.Enum):
    BOT = "BOT"
    PERSONAL = "PERSONAL"
    CHANNEL = "CHANNEL"


class EventType(str, enum.Enum):
    Lead = "Lead"
    CompleteRegistration = "CompleteRegistration"
    Subscribe = "Subscribe"
    Purchase = "Purchase"
    ViewContent = "ViewContent"
    InitiateCheckout = "InitiateCheckout"
    Contact = "Contact"
    CustomizeProduct = "CustomizeProduct"
    FindLocation = "FindLocation"
    AddToCart = "AddToCart"
    AddPaymentInfo = "AddPaymentInfo"
    StartTrial = "StartTrial"
    Schedule = "Schedule"


class ConversionStatus(str, enum.Enum):
    fired = "fired"
    error = "error"
    skipped = "skipped"


class MessageDirection(str, enum.Enum):
    inbound = "inbound"
    outbound = "outbound"


class TriggerType(str, enum.Enum):
    click = "click"
    bot_start = "bot_start"
    first_message = "first_message"
    any_message = "any_message"
    keyword = "keyword"
    channel_join = "channel_join"
    channel_join_request = "channel_join_request"
    group_join = "group_join"
    button_click = "button_click"
    callback_query = "callback_query"
    mini_app_open = "mini_app_open"
    first_dm = "first_dm"
    message_received = "message_received"
    keyword_match = "keyword_match"
    custom_event = "custom_event"
    lead = "lead"
    registration = "registration"
    purchase = "purchase"
    deposit = "deposit"
    manual = "manual"
    webhook = "webhook"


class ClickDestinationType(str, enum.Enum):
    bot = "bot"
    channel = "channel"
    group = "group"
    dm_bridge = "dm_bridge"
    mini_app = "mini_app"
    custom = "custom"


class TelegramEventType(str, enum.Enum):
    CLICK = "CLICK"
    BOT_START = "BOT_START"
    BOT_MESSAGE = "BOT_MESSAGE"
    BUTTON_CLICK = "BUTTON_CLICK"
    CALLBACK_CLICK = "CALLBACK_CLICK"
    MINI_APP_OPEN = "MINI_APP_OPEN"
    CHANNEL_JOIN_REQUEST = "CHANNEL_JOIN_REQUEST"
    CHANNEL_JOIN = "CHANNEL_JOIN"
    GROUP_JOIN = "GROUP_JOIN"
    FIRST_DM = "FIRST_DM"
    MESSAGE_RECEIVED = "MESSAGE_RECEIVED"
    KEYWORD_MATCH = "KEYWORD_MATCH"
    CUSTOM_EVENT = "CUSTOM_EVENT"
    LEAD = "LEAD"
    REGISTRATION = "REGISTRATION"
    PURCHASE = "PURCHASE"
    DEPOSIT = "DEPOSIT"


class MetaEventStatus(str, enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    SENT = "SENT"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class FlowTriggerType(str, enum.Enum):
    bot_start = "bot_start"
    channel_join = "channel_join"
    channel_join_request = "channel_join_request"
    first_dm = "first_dm"
    keyword = "keyword"
    button_click = "button_click"
    custom_event = "custom_event"
    purchase = "purchase"
    tag_added = "tag_added"
    manual = "manual"


class FlowActionType(str, enum.Enum):
    send_message = "send_message"
    add_tag = "add_tag"
    remove_tag = "remove_tag"
    trigger_event = "trigger_event"
    send_meta_event = "send_meta_event"
    wait = "wait"
    condition = "condition"
    branch = "branch"
    webhook = "webhook"


# ── TABLE 1: dashboard_users ──────────────────────────────────────────
class DashboardUser(Base):
    __tablename__ = "dashboard_users"

    telegram_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    last_login: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)

    login_username: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    password_hash: Mapped[str | None] = mapped_column(String(256), nullable=True)

    accounts: Mapped[list["TelegramAccount"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    campaigns: Mapped[list["Campaign"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    funnels: Mapped[list["Funnel"]] = relationship(back_populates="user", cascade="all, delete-orphan")


# ── TABLE 2: access_tokens ────────────────────────────────────────────
class AccessToken(Base):
    __tablename__ = "access_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_by: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    expires_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    is_used: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    used_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    used_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)


# ── TABLE 3: telegram_accounts ────────────────────────────────────────
class TelegramAccount(Base):
    __tablename__ = "telegram_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    account_type: Mapped[AccountType] = mapped_column(Enum(AccountType), nullable=False)
    identifier: Mapped[str] = mapped_column(String(512), nullable=False)
    session_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    meta_pixel_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    meta_capi_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    proxy_string: Mapped[str | None] = mapped_column(String(512), nullable=True)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    monitor_account_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="SET NULL"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    # v5: welcome message + auto-reply
    welcome_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    welcome_button_text: Mapped[str | None] = mapped_column(String(128), nullable=True)
    welcome_button_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    user: Mapped["DashboardUser"] = relationship(back_populates="accounts")
    campaigns: Mapped[list["Campaign"]] = relationship(back_populates="account")
    messages: Mapped[list["Message"]] = relationship(back_populates="account", cascade="all, delete-orphan")


# ── NEW: tracking_domains ─────────────────────────────────────────────
class TrackingDomain(Base):
    __tablename__ = "tracking_domains"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    domain: Mapped[str] = mapped_column(String(256), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (UniqueConstraint("user_id", "domain", name="uq_tracking_domain_user_domain"), Index("idx_tracking_domain_user", "user_id"))

# ── NEW: tracking_links ───────────────────────────────────────────────
class TrackingLink(Base):
    __tablename__ = "tracking_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    domain_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tracking_domains.id", ondelete="SET NULL"), nullable=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    destination_type: Mapped[ClickDestinationType] = mapped_column(Enum(ClickDestinationType), nullable=False, default=ClickDestinationType.bot)
    destination: Mapped[str] = mapped_column(String(512), nullable=False)  # e.g. t.me/mybot or https://t.me/...
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    total_clicks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # optional overrides
    label: Mapped[str | None] = mapped_column(String(256), nullable=True)

    __table_args__ = (Index("idx_tracking_link_user", "user_id"), Index("idx_tracking_link_campaign", "campaign_id"), Index("idx_tracking_link_slug", "slug"))


# ── TABLE 4: campaigns (extended) ────────────────────────────────────
class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    slug: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    target_telegram_username: Mapped[str] = mapped_column(String(256), nullable=False)
    event_type: Mapped[EventType] = mapped_column(Enum(EventType), nullable=False, default=EventType.Lead)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    total_clicks: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_conversions: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # v5: campaign hierarchy
    adset_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ad_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    creative_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    placement: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # v5: attribution window override per campaign
    attribution_window_hours: Mapped[int | None] = mapped_column(Integer, nullable=True)

    user: Mapped["DashboardUser"] = relationship(back_populates="campaigns")
    account: Mapped["TelegramAccount"] = relationship(back_populates="campaigns")
    logs: Mapped[list["ConversionLog"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")
    triggers: Mapped[list["ConversionTrigger"]] = relationship(back_populates="campaign", cascade="all, delete-orphan", order_by="ConversionTrigger.trigger_order")
    sessions: Mapped[list["UserSession"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")
    webhook_tokens: Mapped[list["WebhookToken"]] = relationship(back_populates="campaign", cascade="all, delete-orphan")


# ── TABLE 5: conversion_triggers ─────────────────────────────────────
class ConversionTrigger(Base):
    __tablename__ = "conversion_triggers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    trigger_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    trigger_type: Mapped[TriggerType] = mapped_column(Enum(TriggerType), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    event_name: Mapped[str] = mapped_column(String(64), nullable=False)
    keywords: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_mode: Mapped[str] = mapped_column(String(8), default="any", nullable=False)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    content_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    content_ids: Mapped[str | None] = mapped_column(String(512), nullable=True)
    custom_data_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    campaign: Mapped["Campaign"] = relationship(back_populates="triggers")


# ── TABLE 6: user_sessions ────────────────────────────────────────────
class UserSession(Base):
    __tablename__ = "user_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    tg_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tg_username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tg_first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tg_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    session_key: Mapped[str] = mapped_column(String(64), nullable=False)
    fbclid: Mapped[str | None] = mapped_column(String(256), nullable=True)
    fbc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbp: Mapped[str | None] = mapped_column(String(128), nullable=True)
    client_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    last_seen_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())
    fired_triggers: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    click_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clicks.id", ondelete="SET NULL"), nullable=True)

    campaign: Mapped["Campaign"] = relationship(back_populates="sessions")

    __table_args__ = (
        Index("idx_us_tg_campaign", "tg_user_id", "campaign_id", unique=True),
        Index("idx_us_session_key", "session_key"),
        Index("idx_us_click", "click_id"),
    )


# ── NEW: clicks ───────────────────────────────────────────────────────
class Click(Base):
    __tablename__ = "clicks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    click_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)  # public opaque id (short_key)
    tracking_link_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tracking_links.id", ondelete="SET NULL"), nullable=True)
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    domain_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tracking_domains.id", ondelete="SET NULL"), nullable=True)

    # Meta attribution
    fbclid: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbp: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Campaign hierarchy + subs
    campaign_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    adset: Mapped[str | None] = mapped_column(String(256), nullable=True)
    adset_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ad: Mapped[str | None] = mapped_column(String(256), nullable=True)
    ad_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    creative: Mapped[str | None] = mapped_column(String(256), nullable=True)
    placement: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sub1: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub2: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub3: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub4: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub5: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub6: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub7: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub8: Mapped[str | None] = mapped_column(String(256), nullable=True)
    sub9: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Request capture
    ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country: Mapped[str | None] = mapped_column(String(2), nullable=True)
    region: Mapped[str | None] = mapped_column(String(128), nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    language: Mapped[str | None] = mapped_column(String(32), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    browser: Mapped[str | None] = mapped_column(String(64), nullable=True)
    browser_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    os: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    screen: Mapped[str | None] = mapped_column(String(64), nullable=True)
    referrer: Mapped[str | None] = mapped_column(Text, nullable=True)
    landing_page: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Meta event id for dedup
    event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    expires_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)

    __table_args__ = (
        Index("idx_click_campaign", "campaign_id"),
        Index("idx_click_link", "tracking_link_id"),
        Index("idx_click_created", "created_at"),
        Index("idx_click_fbclid", "fbclid"),
        Index("idx_click_click_id", "click_id"),
        Index("idx_click_ip", "ip"),
    )


# ── NEW: telegram_identities ──────────────────────────────────────────
class TelegramIdentity(Base):
    __tablename__ = "telegram_identities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    language_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    first_seen: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    last_seen: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())
    source: Mapped[str | None] = mapped_column(String(32), nullable=True)  # bot_start, dm, channel_join, etc.
    country: Mapped[str | None] = mapped_column(String(2), nullable=True)
    device: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        UniqueConstraint("owner_user_id", "telegram_user_id", name="uq_identity_owner_tg"),
        Index("idx_identity_owner", "owner_user_id"),
        Index("idx_identity_tg", "telegram_user_id"),
        Index("idx_identity_username", "username"),
    )


# ── NEW: telegram_events ──────────────────────────────────────────────
class TelegramEvent(Base):
    __tablename__ = "telegram_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    event_type: Mapped[TelegramEventType] = mapped_column(Enum(TelegramEventType), nullable=False)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    click_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clicks.id", ondelete="SET NULL"), nullable=True)
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    tracking_link_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("tracking_links.id", ondelete="SET NULL"), nullable=True)
    account_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="SET NULL"), nullable=True)
    trigger_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("conversion_triggers.id", ondelete="SET NULL"), nullable=True)
    # denormalized attribution for fast queries
    fbclid: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbp: Mapped[str | None] = mapped_column(String(128), nullable=True)
    meta_event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_metadata: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (
        Index("idx_tgevent_owner", "owner_user_id"),
        Index("idx_tgevent_tg_user", "telegram_user_id"),
        Index("idx_tgevent_click", "click_id"),
        Index("idx_tgevent_campaign", "campaign_id"),
        Index("idx_tgevent_type", "event_type"),
        Index("idx_tgevent_created", "created_at"),
        Index("idx_tgevent_identity", "identity_id"),
    )


# ── TABLE 7: conversion_logs (legacy + extended) ──────────────────────
class ConversionLog(Base):
    __tablename__ = "conversion_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    trigger_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("conversion_triggers.id", ondelete="SET NULL"), nullable=True)
    trigger_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    telegram_username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fbclid: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbp: Mapped[str | None] = mapped_column(String(128), nullable=True)
    client_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    event_currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    content_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[ConversionStatus] = mapped_column(Enum(ConversionStatus), nullable=False, default=ConversionStatus.fired)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fbtrace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fired_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    # v5 linkage
    click_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clicks.id", ondelete="SET NULL"), nullable=True)
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    telegram_event_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_events.id", ondelete="SET NULL"), nullable=True)
    meta_event_row_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("meta_events.id", ondelete="SET NULL"), nullable=True)

    campaign: Mapped["Campaign"] = relationship(back_populates="logs")

    __table_args__ = (
        Index("idx_conv_campaign", "campaign_id"),
        Index("idx_conv_fired_at", "fired_at"),
        Index("idx_conv_tg_user", "telegram_user_id"),
        Index("idx_conv_trigger", "trigger_type"),
        Index("idx_conv_click", "click_id"),
        Index("idx_conv_identity", "identity_id"),
        Index("idx_conv_meta_event_id", "meta_event_id"),
    )


# ── NEW: meta_pixels ──────────────────────────────────────────────────
class MetaPixel(Base):
    __tablename__ = "meta_pixels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    pixel_id: Mapped[str] = mapped_column(String(64), nullable=False)
    access_token: Mapped[str | None] = mapped_column(Text, nullable=True)  # encrypted at rest
    test_event_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (UniqueConstraint("user_id", "pixel_id", name="uq_meta_pixel_user_pixel"), Index("idx_meta_pixel_user", "user_id"))


# ── NEW: meta_events (CAPI queue) ─────────────────────────────────────
class MetaEvent(Base):
    __tablename__ = "meta_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    event_name: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    pixel_id: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    click_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clicks.id", ondelete="SET NULL"), nullable=True)
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    telegram_event_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_events.id", ondelete="SET NULL"), nullable=True)
    conversion_log_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("conversion_logs.id", ondelete="SET NULL"), nullable=True)

    fbc: Mapped[str | None] = mapped_column(String(512), nullable=True)
    fbp: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_time: Mapped[int] = mapped_column(Integer, nullable=False)  # unix seconds
    event_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    action_source: Mapped[str] = mapped_column(String(32), default="website", nullable=False)
    custom_data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    user_data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON (hashed)

    status: Mapped[MetaEventStatus] = mapped_column(Enum(MetaEventStatus), nullable=False, default=MetaEventStatus.PENDING)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    meta_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    fbtrace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    test_event_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dedup_key: Mapped[str | None] = mapped_column(String(128), nullable=True)  # for idempotency

    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    sent_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())

    __table_args__ = (
        Index("idx_meta_event_owner", "owner_user_id"),
        Index("idx_meta_event_status", "status"),
        Index("idx_meta_event_pixel", "pixel_id"),
        Index("idx_meta_event_campaign", "campaign_id"),
        Index("idx_meta_event_click", "click_id"),
        Index("idx_meta_event_created", "created_at"),
        Index("idx_meta_event_next_attempt", "next_attempt_at"),
        Index("idx_meta_event_dedup", "dedup_key"),
        UniqueConstraint("dedup_key", name="uq_meta_event_dedup"),
    )


# ── TABLE 8: webhook_tokens ───────────────────────────────────────────
class WebhookToken(Base):
    __tablename__ = "webhook_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    last_hit_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    hit_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    campaign: Mapped["Campaign"] = relationship(back_populates="webhook_tokens")


# ── TABLE 9: messages ─────────────────────────────────────────────────
class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    tg_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    tg_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tg_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    tg_username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tg_first_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    direction: Mapped[MessageDirection] = mapped_column(Enum(MessageDirection), nullable=False, default=MessageDirection.inbound)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    received_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    # v5
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    click_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("clicks.id", ondelete="SET NULL"), nullable=True)

    account: Mapped["TelegramAccount"] = relationship(back_populates="messages")

    __table_args__ = (
        Index("idx_msg_account", "account_id"),
        Index("idx_msg_chat", "tg_chat_id"),
        Index("idx_msg_received", "received_at"),
        Index("idx_msg_identity", "identity_id"),
        Index("idx_msg_tg_user", "tg_user_id"),
    )


# ── NEW: conversations (inbox grouping) ───────────────────────────────
class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    account_id: Mapped[int] = mapped_column(Integer, ForeignKey("telegram_accounts.id", ondelete="CASCADE"))
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    tg_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tg_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_message_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    last_message_preview: Mapped[str | None] = mapped_column(String(256), nullable=True)
    unread_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    updated_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("account_id", "tg_chat_id", name="uq_conversation_account_chat"),
        Index("idx_conversation_owner", "owner_user_id"),
        Index("idx_conversation_identity", "identity_id"),
    )


# ── TABLE 10: funnels ─────────────────────────────────────────────────
class Funnel(Base):
    __tablename__ = "funnels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    # v5: time window per funnel
    window_hours: Mapped[int | None] = mapped_column(Integer, nullable=True)

    user: Mapped["DashboardUser"] = relationship(back_populates="funnels")
    steps: Mapped[list["FunnelStep"]] = relationship(back_populates="funnel", cascade="all, delete-orphan", order_by="FunnelStep.step_order")

    __table_args__ = (Index("idx_funnel_campaign", "campaign_id"), Index("idx_funnel_user", "user_id"))


# ── TABLE 11: funnel_steps ────────────────────────────────────────────
class FunnelStep(Base):
    __tablename__ = "funnel_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    funnel_id: Mapped[int] = mapped_column(Integer, ForeignKey("funnels.id", ondelete="CASCADE"))
    trigger_id: Mapped[int] = mapped_column(Integer, ForeignKey("conversion_triggers.id", ondelete="CASCADE"))
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # v5: time constraint per step
    max_delay_hours: Mapped[int | None] = mapped_column(Integer, nullable=True)

    funnel: Mapped["Funnel"] = relationship(back_populates="steps")
    trigger: Mapped["ConversionTrigger"] = relationship()

    __table_args__ = (Index("idx_fs_funnel", "funnel_id"), Index("idx_fs_trigger", "trigger_id"))


# ── NEW: tags ─────────────────────────────────────────────────────────
class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    color: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_tag_user_name"), Index("idx_tag_user", "user_id"))


class ContactTag(Base):
    __tablename__ = "contact_tags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tags.id", ondelete="CASCADE"))
    identity_id: Mapped[int] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (UniqueConstraint("tag_id", "identity_id", name="uq_contact_tag"), Index("idx_contact_tag_identity", "identity_id"), Index("idx_contact_tag_tag", "tag_id"))


# ── NEW: flows (automation) ───────────────────────────────────────────
class Flow(Base):
    __tablename__ = "flows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    campaign_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    trigger_type: Mapped[str | None] = mapped_column(String(32), nullable=True)  # legacy simple trigger
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    updated_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())

    nodes: Mapped[list["FlowNode"]] = relationship(back_populates="flow", cascade="all, delete-orphan")
    edges: Mapped[list["FlowEdge"]] = relationship(back_populates="flow", cascade="all, delete-orphan")

    __table_args__ = (Index("idx_flow_user", "user_id"), Index("idx_flow_campaign", "campaign_id"))


class FlowNode(Base):
    __tablename__ = "flow_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[int] = mapped_column(Integer, ForeignKey("flows.id", ondelete="CASCADE"))
    node_type: Mapped[str] = mapped_column(String(32), nullable=False)  # trigger, action, condition, wait, branch
    action_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    config: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    position_x: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    position_y: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    flow: Mapped["Flow"] = relationship(back_populates="nodes")

    __table_args__ = (Index("idx_flow_node_flow", "flow_id"),)


class FlowEdge(Base):
    __tablename__ = "flow_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[int] = mapped_column(Integer, ForeignKey("flows.id", ondelete="CASCADE"))
    source_node_id: Mapped[int] = mapped_column(Integer, ForeignKey("flow_nodes.id", ondelete="CASCADE"))
    target_node_id: Mapped[int] = mapped_column(Integer, ForeignKey("flow_nodes.id", ondelete="CASCADE"))
    label: Mapped[str | None] = mapped_column(String(64), nullable=True)  # yes/no, condition branch
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    flow: Mapped["Flow"] = relationship(back_populates="edges")

    __table_args__ = (Index("idx_flow_edge_flow", "flow_id"),)


class FlowRun(Base):
    __tablename__ = "flow_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[int] = mapped_column(Integer, ForeignKey("flows.id", ondelete="CASCADE"))
    identity_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_identities.id", ondelete="SET NULL"), nullable=True)
    telegram_event_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("telegram_events.id", ondelete="SET NULL"), nullable=True)
    current_node_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("flow_nodes.id", ondelete="SET NULL"), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)  # running, completed, failed, waiting
    context: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())
    updated_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())

    __table_args__ = (Index("idx_flow_run_flow", "flow_id"), Index("idx_flow_run_identity", "identity_id"))


# ── NEW: api_keys ─────────────────────────────────────────────────────
class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"))
    key_hash: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    prefix: Mapped[str] = mapped_column(String(16), nullable=False)  # for display e.g. tk_abc123...
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(ts_column(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default())

    __table_args__ = (Index("idx_api_key_user", "user_id"),)


# ── NEW: attribution_settings per user ────────────────────────────────
class AttributionSetting(Base):
    __tablename__ = "attribution_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dashboard_users.telegram_id", ondelete="CASCADE"), unique=True)
    model: Mapped[str] = mapped_column(String(32), default="last_touch", nullable=False)
    window_hours: Mapped[int] = mapped_column(Integer, default=168, nullable=False)
    include_organic: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(ts_column(), server_default=now_server_default(), onupdate=func.now())

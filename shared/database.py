"""
shared/database.py — Async SQLAlchemy engine + session factory

v5 change: the engine is created lazily on first use (and can be reset) so a
test run can point DATABASE_URL at SQLite without re-importing half the app.
`engine` / `AsyncSessionLocal` keep their historical import shape:

    from shared.database import engine, AsyncSessionLocal, get_db, init_db

still work exactly as before.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, func, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool

from shared.config import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Declarative base for every ORM model (shared/models.py)."""


_state: dict[str, Any] = {"engine": None, "sessionmaker": None, "url": None}


def get_engine() -> AsyncEngine:
    settings = get_settings()
    url = settings.DATABASE_URL
    if _state["engine"] is not None and _state["url"] == url:
        return _state["engine"]

    kwargs: dict[str, Any] = dict(
        pool_pre_ping=True,
        echo=settings.DB_ECHO,
        future=True,
    )
    if settings.is_sqlite:
        # SQLite is used by the test-suite (and by anyone who wants a zero-dep
        # local trial). StaticPool keeps one connection so :memory: works.
        kwargs["connect_args"] = {"check_same_thread": False}
        if ":memory:" in url or url in ("sqlite+aiosqlite:///", "sqlite+aiosqlite://"):
            kwargs["poolclass"] = StaticPool
    else:
        kwargs["pool_size"] = settings.DB_POOL_SIZE
        kwargs["max_overflow"] = settings.DB_MAX_OVERFLOW

    engine = create_async_engine(url, **kwargs)
    _state.update(engine=engine, url=url, sessionmaker=None)
    logger.info("DATABASE_ENGINE_READY url=%s", _safe_url(url))
    return engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    if _state["sessionmaker"] is None:
        _state["sessionmaker"] = async_sessionmaker(
            bind=get_engine(), class_=AsyncSession, expire_on_commit=False
        )
    return _state["sessionmaker"]


def reset_engine() -> None:
    """Tear down cached engine/sessionmaker (tests, or config reload)."""
    _state.update(engine=None, sessionmaker=None, url=None)


def _safe_url(url: str) -> str:
    """Strip credentials from a DSN — connection strings contain passwords."""
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" in rest:
        rest = "***@" + rest.rsplit("@", 1)[1]
    return f"{scheme}://{rest}"


class _AsyncSessionFactory:
    """
    Callable proxy so `async with AsyncSessionLocal() as db:` keeps working
    while the real maker is resolved lazily (see get_sessionmaker).
    """

    def __call__(self, **kwargs: Any) -> AsyncSession:
        return get_sessionmaker()(**kwargs)

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - passthrough
        return getattr(get_sessionmaker(), item)


AsyncSessionLocal = _AsyncSessionFactory()


def __getattr__(name: str) -> Any:
    # PEP 562: `from shared.database import engine` resolves lazily.
    if name == "engine":
        return get_engine()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db() -> None:
    """Create all tables on startup if they don't exist."""
    import shared.models  # noqa: F401  (register every model on Base.metadata)

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def db_ready() -> bool:
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
        return True
    except Exception as exc:  # pragma: no cover - depends on infra state
        logger.error("DATABASE_UNAVAILABLE error=%s", exc)
        return False


# ── Dialect helpers ───────────────────────────────────────────────────
#
# Postgres columns are TIMESTAMPTZ, so queries bind tz-aware datetimes.
# SQLite has no tz type: binding a tz-aware datetime writes a string with a
# "+00:00" suffix while CURRENT_TIMESTAMP rows have none, which silently
# breaks range filters. `bind_ts` normalises per dialect so range queries are
# correct on both, without scattering `if is_sqlite` through every endpoint.

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def bind_ts(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return naive_utc(dt) if get_settings().is_sqlite else dt


def ts_column() -> DateTime:
    """TIMESTAMPTZ-on-PG / DATETIME-on-SQLite column type with UTC default."""
    if get_settings().is_sqlite:
        return DateTime()
    return DateTime(timezone=True)


def now_server_default():
    """DB-side default that works on both dialects."""
    if get_settings().is_sqlite:
        return text("CURRENT_TIMESTAMP")
    return func.now()

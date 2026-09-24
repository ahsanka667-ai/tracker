"""
shared/config.py — Centralized settings via pydantic-settings

Fails fast on startup with clear error messages if required
config is missing or looks wrong — instead of cryptic failures
3 layers deep at runtime.

v5: adds the security/attribution/queue/retention knobs the tracking,
attribution and Meta CAPI pipeline needs. Everything Telegram-related stays
optional for the dashboard process exactly as before.
"""
import sys
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── PostgreSQL ────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/tg_tracker"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_ECHO: bool = False

    # ── Redis ─────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_TTL_HOURS: int = 72          # correlation-payload TTL (click cache)

    # Queue names (spec §33). Kept flat + explicit: there is exactly one
    # ad platform in this product, so no generic routing layer exists.
    QUEUE_META_CAPI: str = "tgq:meta_capi"
    QUEUE_TELEGRAM_EVENTS: str = "tgq:telegram_events"
    QUEUE_AUTOMATION: str = "tgq:automation"
    QUEUE_ANALYTICS: str = "tgq:analytics"
    QUEUE_DELAYED_SUFFIX: str = ":delayed"   # zset of jobs not yet due
    QUEUE_MAXLEN: int = 100_000              # bounded queue (spec §42)
    CAPI_MAX_ATTEMPTS: int = 6
    CAPI_BACKOFF_BASE_SECONDS: int = 15      # 15,30,60,120,240…
    CAPI_BACKOFF_MAX_SECONDS: int = 3600
    CAPI_BATCH_SIZE: int = 50                # events per Graph API request
    WORKER_HEARTBEAT_TTL: int = 90           # seconds a heartbeat stays "fresh"

    # ── Secrets ───────────────────────────────────────────────────────
    # SECRET_KEY signs correlation tokens / short links / WS tickets. It is
    # REQUIRED (no default) — with a default secret anyone could forge a
    # click-correlation token.
    SECRET_KEY: str = ""
    # Optional 32-byte hex/base64 key for encrypting per-account secrets at
    # rest (Meta CAPI access tokens, bot tokens). Empty = store as-is.
    ENCRYPTION_KEY: str = ""

    # ── Master Bot (TWA auth + admin commands) ────────────────────────
    MASTER_BOT_TOKEN: str = ""

    # ── Telegram API credentials (Telethon userbot sessions) ──────────
    TELEGRAM_API_ID: int = 0
    TELEGRAM_API_HASH: str = ""

    ADMIN_TELEGRAM_ID: int = 0
    ADMIN_USERNAME: str = ""
    ADMIN_PASSWORD: str = ""

    SESSIONS_DIR: str = "./sessions"
    BASE_URL: str = "https://your-domain.com"

    # ── Logging ───────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "./logs"
    LOG_JSON: bool = False          # structured (machine-readable) logs

    # ── Click capture / abuse protection ─────────────────────────────
    CLICK_RATE_LIMIT_MAX: int = 20
    CLICK_RATE_LIMIT_WINDOW_SECONDS: int = 60
    # How many hops of X-Forwarded-For to trust (0 = never trust XFF, use
    # the socket peer). Set 1 behind nginx, 2 behind nginx+CDN.
    TRUST_PROXY_HOPS: int = 1
    # Filter link-preview crawlers out of click counts.
    FILTER_CRAWLERS: bool = True

    # ── Attribution defaults (overridable per user in Settings) ──────
    ATTRIBUTION_MODEL: str = "last_touch"      # last_touch | first_touch | last_non_direct
    ATTRIBUTION_WINDOW_HOURS: int = 168        # 7-day lookback
    ATTRIBUTION_INCLUDE_ORGANIC: bool = False  # never attribute paid events to organic

    # ── Correlation tokens ────────────────────────────────────────────
    CORRELATION_TOKEN_TTL_SECONDS: int = 6 * 3600
    # A redeemed token may be replayed this many seconds for idempotency
    # (Telegram redelivers /start updates); a *different* identity is rejected.
    TOKEN_IDEMPOTENCY_WINDOW_SECONDS: int = 86_400

    # ── Privacy / retention (spec §3) ────────────────────────────────
    DATA_RETENTION_DAYS: int = 365             # 0 = keep forever
    CLICK_RETENTION_DAYS: int = 0              # defaults to DATA_RETENTION_DAYS
    ANONYMIZE_IPS: bool = False                # store /24 (v4) or /64 (v6) prefix
    GEO_LOOKUP_ENABLED: bool = False           # opt-in: no geolocation by default
    GEOIP_CITY_DB: str = ""                    # optional MaxMind MMDB path
    RETENTION_SWEEP_INTERVAL_SECONDS: int = 6 * 3600

    # ── Meta ───────────────────────────────────────────────────────────
    META_GRAPH_VERSION: str = "v21.0"
    META_TIMEOUT_SECONDS: int = 12
    META_MAX_RETRIES: int = 3                  # in-request retries (fast path)
    META_BACKOFF_BASE: float = 1.5
    # When True the hosted landing page loads Meta's fbevents.js. Off by
    # default so nothing is sent to Meta until a pixel is actually configured.
    ENABLE_BROWSER_PIXEL: bool = True

    # ── Webhooks / API ────────────────────────────────────────────────
    TELEGRAM_WEBHOOK_SECRET: str = ""          # X-Telegram-Bot-Api-Secret-Token
    WEBHOOK_RATE_LIMIT_MAX: int = 120
    WEBHOOK_RATE_LIMIT_WINDOW_SECONDS: int = 60
    ALLOW_PRIVATE_URLS_FOR_OUTBOUND_WEBHOOKS: bool = False  # SSRF guard (§31)

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    @field_validator("ATTRIBUTION_MODEL")
    @classmethod
    def _valid_attribution_model(cls, v: str) -> str:
        allowed = {"last_touch", "first_touch", "last_non_direct"}
        if v not in allowed:
            raise ValueError(f"ATTRIBUTION_MODEL must be one of {sorted(allowed)}")
        return v

    @property
    def is_sqlite(self) -> bool:
        return self.DATABASE_URL.startswith("sqlite")

    @property
    def is_https_base(self) -> bool:
        return self.BASE_URL.startswith("https://")

    @property
    def effective_click_retention_days(self) -> int:
        return self.CLICK_RETENTION_DAYS or self.DATA_RETENTION_DAYS


def _validate(settings: Settings, component: str = "dashboard") -> list[str]:
    """
    Returns a list of human-readable problems with the current config.

    `component` controls how strict Telegram-related checks are:
      - "dashboard" (service_a / the API + web dashboard): Telegram
        credentials are OPTIONAL. You can run the dashboard with only
        ADMIN_USERNAME/ADMIN_PASSWORD and never touch Telegram at all.
      - "worker" (service_b) / "bot" (master_bot): these processes exist
        ONLY to talk to Telegram, so MASTER_BOT_TOKEN / TELEGRAM_API_ID /
        TELEGRAM_API_HASH are FATAL if missing.
      - "capi_worker" / "jobs_worker": need Redis + Postgres + SECRET_KEY
        only — no Telegram credentials (they must keep draining the queue
        even if every Telegram account is down).
    """
    problems = []
    telegram_required = component in ("worker", "bot")

    if not settings.SECRET_KEY or len(settings.SECRET_KEY) < 32:
        problems.append(
            "SECRET_KEY is missing or shorter than 32 characters. It signs the "
            "correlation tokens that link a Telegram identity back to an ad click, "
            "so a weak/absent value means forged tokens. Generate one with: "
            "python -c \"import secrets;print(secrets.token_urlsafe(48))\""
        )

    if settings.ENCRYPTION_KEY:
        key = settings.ENCRYPTION_KEY.strip()
        if len(key) < 32:
            problems.append("ENCRYPTION_KEY must be at least 32 characters (or empty to disable at-rest encryption).")

    bot_token_ok = bool(settings.MASTER_BOT_TOKEN) and ":" in settings.MASTER_BOT_TOKEN
    if not bot_token_ok:
        msg = ("MASTER_BOT_TOKEN is missing or malformed. "
               "Get it from @BotFather → /newbot → copy the token (looks like 123456789:AAF...)")
        problems.append(msg if telegram_required else f"WARNING (non-fatal): {msg} "
                        "Telegram login/bot features will be unavailable until this is set.")

    if settings.TELEGRAM_API_ID == 0:
        msg = "TELEGRAM_API_ID is not set (currently 0). Get it from https://my.telegram.org/apps"
        problems.append(msg if telegram_required else f"WARNING (non-fatal): {msg} "
                        "Personal-account/channel tracking (Service B) needs this to run.")

    if not settings.TELEGRAM_API_HASH:
        msg = "TELEGRAM_API_HASH is not set. Get it from https://my.telegram.org/apps"
        problems.append(msg if telegram_required else f"WARNING (non-fatal): {msg}")

    if telegram_required and settings.ADMIN_TELEGRAM_ID == 0 and not settings.ADMIN_USERNAME:
        problems.append(
            "ADMIN_TELEGRAM_ID is not set (currently 0), and no ADMIN_USERNAME is set either. "
            "Message @userinfobot to get your numeric Telegram ID, or set ADMIN_USERNAME/ADMIN_PASSWORD instead."
        )

    if component == "dashboard":
        has_local_login = bool(settings.ADMIN_USERNAME and settings.ADMIN_PASSWORD)
        has_telegram_login = bool(settings.ADMIN_TELEGRAM_ID and bot_token_ok)
        if not has_local_login and not has_telegram_login:
            problems.append(
                "No way to log in is configured. Set ADMIN_USERNAME and ADMIN_PASSWORD in .env "
                "for username/password login (no Telegram needed), or set MASTER_BOT_TOKEN + "
                "ADMIN_TELEGRAM_ID for Telegram login."
            )
        if settings.ADMIN_USERNAME and not settings.ADMIN_PASSWORD:
            problems.append("ADMIN_USERNAME is set but ADMIN_PASSWORD is empty — set both, or neither.")
        if settings.ADMIN_PASSWORD and not settings.ADMIN_USERNAME:
            problems.append("ADMIN_PASSWORD is set but ADMIN_USERNAME is empty — set both, or neither.")

    if settings.BASE_URL in ("https://your-domain.com", "", "http://localhost:8000"):
        problems.append(
            "WARNING (non-fatal): BASE_URL is still a placeholder/localhost. "
            "Tracking links and the bot's Web App button won't work correctly "
            "until BASE_URL is set to your real domain or ngrok URL."
        )

    if settings.QUEUE_MAXLEN <= 0:
        problems.append("QUEUE_MAXLEN must be > 0 (unbounded queues are an outage waiting to happen).")

    if settings.REDIS_TTL_HOURS < 1:
        problems.append("REDIS_TTL_HOURS must be >= 1 — it is how long a click stays redeemable by Telegram.")

    return problems


@lru_cache()
def get_settings() -> Settings:
    settings = Settings()
    return settings


def reload_settings() -> Settings:
    """Drop the cache and re-read the environment (used by tests)."""
    get_settings.cache_clear()
    return get_settings()


def validate_or_exit(component: str = "dashboard"):
    """
    Call this once at process startup (in each service's main()).
    Prints all config problems at once and exits with a non-zero code
    on FATAL errors, so you fix everything in one pass instead of
    playing whack-a-mole with one cryptic crash at a time.

    component: "dashboard" | "worker" | "bot" | "capi_worker" | "jobs_worker"
    """
    settings = get_settings()
    problems = _validate(settings, component)
    if not problems:
        return

    fatal = [p for p in problems if not p.startswith("WARNING")]
    warnings = [p for p in problems if p.startswith("WARNING")]

    if warnings:
        print("\n⚠️  Configuration warnings:", file=sys.stderr)
        for w in warnings:
            print(f"   - {w}", file=sys.stderr)

    if fatal:
        print("\n❌ Configuration errors — fix your .env file:", file=sys.stderr)
        for f in fatal:
            print(f"   - {f}", file=sys.stderr)
        print("", file=sys.stderr)
        sys.exit(1)

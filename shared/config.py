"""
shared/config.py — Centralized settings via pydantic-settings

Fails fast on startup with clear error messages if required
config is missing or looks wrong — instead of cryptic failures
3 layers deep at runtime.
"""
import sys
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # PostgreSQL
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/tg_tracker"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_TTL_HOURS: int = 72

    # Master Bot (for TWA auth + bot commands)
    MASTER_BOT_TOKEN: str = ""

    # Telegram API (for Telethon userbot sessions)
    TELEGRAM_API_ID: int = 0
    TELEGRAM_API_HASH: str = ""

    # Your own Telegram ID — auto-created as the first admin on startup.
    # Optional if you use ADMIN_USERNAME/ADMIN_PASSWORD instead.
    ADMIN_TELEGRAM_ID: int = 0

    # Username/password login — lets you sign into the dashboard with no
    # Telegram bot, widget, or domain setup at all. Auto-creates/updates
    # one admin account with these credentials on every startup.
    ADMIN_USERNAME: str = ""
    ADMIN_PASSWORD: str = ""

    # Session storage path
    SESSIONS_DIR: str = "./sessions"

    # App base URL
    BASE_URL: str = "https://your-domain.com"

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "./logs"

    # Rate limiting — max click captures per IP per window (anti click-fraud / abuse)
    CLICK_RATE_LIMIT_MAX: int = 20
    CLICK_RATE_LIMIT_WINDOW_SECONDS: int = 60

    class Config:
        env_file = ".env"


def _validate(settings: Settings, component: str = "dashboard") -> list[str]:
    """
    Returns a list of human-readable problems with the current config.

    `component` controls how strict Telegram-related checks are:
      - "dashboard" (service_a / the API + web dashboard): Telegram
        credentials are OPTIONAL. You can run the dashboard with only
        ADMIN_USERNAME/ADMIN_PASSWORD and never touch Telegram at all —
        Telegram login just won't be offered. This is the "no Telegram
        connections needed" mode.
      - "worker" (service_b) / "bot" (master_bot): these processes exist
        ONLY to talk to Telegram, so MASTER_BOT_TOKEN / TELEGRAM_API_ID /
        TELEGRAM_API_HASH are FATAL if missing — there is no meaningful
        way to run them without Telegram credentials.
    """
    problems = []
    telegram_required = component in ("worker", "bot")

    bot_token_ok = bool(settings.MASTER_BOT_TOKEN) and ":" in settings.MASTER_BOT_TOKEN
    if not bot_token_ok:
        msg = ("MASTER_BOT_TOKEN is missing or malformed. "
               "Get it from @BotFather → /newbot → copy the token (looks like 123456789:AAF...)")
        problems.append(msg if telegram_required else f"WARNING (non-fatal): {msg} "
                         "Telegram login/bot features will be unavailable until this is set.")

    if settings.TELEGRAM_API_ID == 0:
        msg = ("TELEGRAM_API_ID is not set (currently 0). "
               "Get it from https://my.telegram.org/apps")
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
        # Not a hard error — local dev legitimately uses localhost — but warn.
        problems.append(
            "WARNING (non-fatal): BASE_URL is still a placeholder/localhost. "
            "Tracking links and the bot's Web App button won't work correctly "
            "until BASE_URL is set to your real domain or ngrok URL."
        )

    return problems


@lru_cache()
def get_settings() -> Settings:
    settings = Settings()
    return settings


def validate_or_exit(component: str = "dashboard"):
    """
    Call this once at process startup (in each service's main()).
    Prints all config problems at once and exits with a non-zero code
    on FATAL errors, so you fix everything in one pass instead of
    playing whack-a-mole with one cryptic crash at a time.

    component: "dashboard" (service_a), "worker" (service_b), or "bot" (master_bot).
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

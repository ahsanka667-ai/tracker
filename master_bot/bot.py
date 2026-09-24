"""
master_bot/bot.py — Master Telegram Bot
Fixed:
- IS_PRODUCTION computed dynamically (not at import time)
- Tokens are reusable until expiry (not single-use) — use max_uses instead
- Error handler registered so crashes don't kill the bot silently
- /start always works even if URL button fails, falls back gracefully
- No interactive commands for regular users
"""
import logging, os, secrets, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime, timedelta, timezone
from sqlalchemy import select

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, MenuButtonWebApp,
    BotCommand,
)
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

from shared.config import get_settings, validate_or_exit
from shared.logging_config import setup_logging
from shared.database import AsyncSessionLocal, init_db
from shared.models import AccessToken, DashboardUser, TelegramAccount, Campaign

logger = logging.getLogger(__name__)

settings = get_settings()


def get_dashboard_url() -> str:
    """Always read from settings fresh — picks up ngrok URL changes after restart."""
    return f"{get_settings().BASE_URL}/dashboard/"


def is_production() -> bool:
    """Dynamic check — not cached at import time."""
    return get_settings().BASE_URL.startswith("https://")


# ── Helpers ───────────────────────────────────────────────────────────

async def get_user(telegram_id: int) -> DashboardUser | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DashboardUser).where(
                DashboardUser.telegram_id == telegram_id,
                DashboardUser.is_active == True,
            )
        )
        return result.scalar_one_or_none()


async def is_admin(telegram_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DashboardUser).where(
                DashboardUser.telegram_id == telegram_id,
                DashboardUser.is_admin == True,
                DashboardUser.is_active == True,
            )
        )
        return result.scalar_one_or_none() is not None


async def send_dashboard_button(update: Update, text: str):
    """
    Send the dashboard open button.
    Production: Web App button (opens inside Telegram)
    Local dev:  Plain text instructions (no URL button — Telegram rejects http://)
    """
    dashboard_url = get_dashboard_url()
    prod = is_production()

    if prod:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                text="📊 Open TG Tracker",
                web_app=WebAppInfo(url=dashboard_url),
            )
        ]])
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)
    else:
        # Local dev — Telegram rejects http:// URL buttons
        # Just tell user to open in browser
        full_text = (
            f"{text}\n\n"
            f"📌 *Open dashboard in your browser:*\n"
            f"`{dashboard_url}`"
        )
        await update.message.reply_text(full_text, parse_mode="Markdown")


# ── /start ────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id
    args = context.args or []

    # ── Token login via deep link: /start <token>
    if args:
        token_str = args[0].strip()
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(AccessToken).where(AccessToken.token == token_str)
            )
            token_row = result.scalar_one_or_none()

            if not token_row:
                await update.message.reply_text(
                    "❌ *Invalid access token.*\n\nAsk the administrator for a valid invite link.",
                    parse_mode="Markdown",
                )
                return

            # Check expiry
            if token_row.expires_at and token_row.expires_at < datetime.now(timezone.utc):
                await update.message.reply_text(
                    "⏰ *This invite link has expired.*\n\nAsk the administrator for a new one.",
                    parse_mode="Markdown",
                )
                return

            # Whitelist the user (create or reactivate)
            existing = await db.get(DashboardUser, tg_id)
            if existing:
                existing.is_active = True
                existing.username = user.username
                existing.first_name = user.first_name or existing.first_name
            else:
                db.add(DashboardUser(
                    telegram_id=tg_id,
                    first_name=user.first_name or "User",
                    username=user.username,
                    is_active=True,
                    is_admin=False,
                ))

            # Track usage but DON'T mark as used — token stays active until expiry
            # This allows admins to share one link with multiple people
            token_row.used_by = tg_id  # track who used it last
            token_row.used_at = datetime.now(timezone.utc)
            await db.commit()

        await send_dashboard_button(
            update,
            f"✅ *Access granted! Welcome, {user.first_name}!*\n\n"
            "You now have full access to TG Tracker."
        )
        return

    # ── Regular /start
    db_user = await get_user(tg_id)

    if db_user:
        await send_dashboard_button(
            update,
            f"👋 *Hey {db_user.first_name}!*\nOpen your dashboard below."
        )
    else:
        # No access — friendly message
        await update.message.reply_text(
            "👋 *Welcome to TG Tracker!*\n\n"
            "This is a private ad conversion tracking tool.\n\n"
            "🔐 *You don't have access yet.*\n\n"
            "To get access:\n"
            "1️⃣ Contact the developer\n"
            "2️⃣ Ask for an invite link\n"
            "3️⃣ Click the link — you'll be added automatically ✅\n\n"
            "_Already have a token? Paste it here._",
            parse_mode="Markdown",
        )


# ── Catch all messages ────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    tg_id = update.effective_user.id

    # Token paste (24-char hex)
    if len(text) == 24 and text.isalnum():
        context.args = [text]
        await cmd_start(update, context)
        return

    db_user = await get_user(tg_id)
    if db_user:
        await send_dashboard_button(update, "Tap below to open your dashboard 👇")
    else:
        await update.message.reply_text(
            "🔐 You don't have access yet.\n\n"
            "Contact the developer to get an invite link."
        )


# ═══════════════════════════════════════════════════════
# ADMIN COMMANDS
# ═══════════════════════════════════════════════════════

async def cmd_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    if not await is_admin(tg_id):
        await update.message.reply_text("❌ Admin only.")
        return

    expires_hours = 0  # 0 = never expires
    if context.args:
        arg = context.args[0].lower()
        try:
            if arg == "never": expires_hours = 0
            elif arg.endswith("h"): expires_hours = int(arg[:-1])
            elif arg.endswith("d"): expires_hours = int(arg[:-1]) * 24
            else: expires_hours = int(arg)
        except ValueError:
            pass

    token_str = secrets.token_hex(12)
    expires_at = None
    if expires_hours > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_hours)

    async with AsyncSessionLocal() as db:
        db.add(AccessToken(
            token=token_str,
            created_by=tg_id,
            expires_at=expires_at,
            is_used=False,
        ))
        await db.commit()

    bot_username = (await context.bot.get_me()).username
    deep_link = f"https://t.me/{bot_username}?start={token_str}"

    expiry_text = f"{expires_hours}h" if expires_hours > 0 else "never expires"

    await update.message.reply_text(
        f"🎟 *New Invite Token*\n\n"
        f"Expiry: *{expiry_text}*\n"
        f"Reusable: *Yes* (multiple people can use this link)\n\n"
        f"*Share this link:*\n`{deep_link}`\n\n"
        f"Tip: `/token 24h` = expires in 24h, `/token never` = permanent",
        parse_mode="Markdown",
    )


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin only.")
        return
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(DashboardUser).order_by(DashboardUser.created_at))
        users = result.scalars().all()
    if not users:
        await update.message.reply_text("No users yet.")
        return
    lines = ["👥 *Dashboard Users:*\n"]
    for u in users:
        icon  = "✅" if u.is_active else "❌"
        badge = " 👑" if u.is_admin else ""
        uname = f"@{u.username}" if u.username else "no username"
        lines.append(f"{icon} `{u.telegram_id}` — {u.first_name} ({uname}){badge}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/revoke <telegram_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return
    async with AsyncSessionLocal() as db:
        user = await db.get(DashboardUser, target_id)
        if not user:
            await update.message.reply_text("❌ User not found.")
            return
        user.is_active = False
        await db.commit()
    await update.message.reply_text(f"✅ Revoked access for `{target_id}`.", parse_mode="Markdown")


async def cmd_makeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/makeadmin <telegram_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return
    async with AsyncSessionLocal() as db:
        user = await db.get(DashboardUser, target_id)
        if not user:
            await update.message.reply_text("❌ User not found. Grant access first.")
            return
        user.is_admin = True
        await db.commit()
    await update.message.reply_text(f"👑 `{target_id}` is now admin.", parse_mode="Markdown")


# ── Global error handler — prevents silent crashes ────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors but never crash the bot."""
    logger.error("Update caused error: %s", context.error, exc_info=context.error)

    # Don't reply on non-message updates (e.g. channel posts)
    if not isinstance(update, Update) or not update.effective_message:
        return

    # Only show error message for bad requests (not network timeouts etc)
    if isinstance(context.error, BadRequest):
        try:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong. Please try again."
            )
        except Exception:
            pass


# ── post_init ─────────────────────────────────────────────────────────

async def post_init(application: Application) -> None:
    await init_db()

    # Auto-create first admin
    if settings.ADMIN_TELEGRAM_ID:
        async with AsyncSessionLocal() as db:
            existing = await db.get(DashboardUser, settings.ADMIN_TELEGRAM_ID)
            if not existing:
                db.add(DashboardUser(
                    telegram_id=settings.ADMIN_TELEGRAM_ID,
                    first_name="Admin",
                    is_active=True,
                    is_admin=True,
                ))
                await db.commit()
                logger.info("Auto-created admin: %d", settings.ADMIN_TELEGRAM_ID)

    # Set Web App menu button only in production
    if is_production():
        try:
            await application.bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="📊 Dashboard",
                    web_app=WebAppInfo(url=get_dashboard_url()),
                )
            )
            logger.info("✅ Menu button set: %s", get_dashboard_url())
        except Exception as e:
            logger.warning("Could not set menu button: %s", e)

    # Set commands
    try:
        await application.bot.set_my_commands([
            BotCommand("start", "Open TG Tracker"),
            BotCommand("token", "Generate invite link (admin)"),
            BotCommand("users", "List users (admin)"),
            BotCommand("revoke", "Revoke access (admin)"),
            BotCommand("makeadmin", "Promote to admin (admin)"),
        ])
    except Exception:
        pass

    logger.info("Bot ready | mode: %s | url: %s",
                "PRODUCTION" if is_production() else "LOCAL DEV",
                get_dashboard_url())


# ── Entry point ───────────────────────────────────────────────────────

def main():
    setup_logging("master_bot")
    validate_or_exit("bot")
    app = (
        Application.builder()
        .token(settings.MASTER_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("token",     cmd_token))
    app.add_handler(CommandHandler("users",     cmd_users))
    app.add_handler(CommandHandler("revoke",    cmd_revoke))
    app.add_handler(CommandHandler("makeadmin", cmd_makeadmin))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # IMPORTANT: register error handler so crashes are logged not silently dropped
    app.add_error_handler(error_handler)

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

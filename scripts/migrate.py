"""
scripts/migrate.py — Safe incremental database migration

Adds new columns and tables introduced in v4 without touching existing data.
Safe to run multiple times — each step checks if the change already exists
before applying it.

Usage:
    source venv/bin/activate
    python scripts/migrate.py
"""
import asyncio, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from shared.database import engine, init_db
from sqlalchemy import text

MIGRATIONS = [
    # v4: channel account support
    {
        "id": "v4_01_telegram_accounts_monitor_account_id",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='telegram_accounts' AND column_name='monitor_account_id'",
        "sql": "ALTER TABLE telegram_accounts ADD COLUMN monitor_account_id INTEGER REFERENCES telegram_accounts(id) ON DELETE SET NULL",
    },
    # v4: conversion_triggers table
    {
        "id": "v4_02_conversion_triggers",
        "check": "SELECT table_name FROM information_schema.tables WHERE table_name='conversion_triggers'",
        "sql": """
            CREATE TABLE conversion_triggers (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                trigger_order INTEGER NOT NULL DEFAULT 0,
                trigger_type VARCHAR(32) NOT NULL,
                event_name VARCHAR(64) NOT NULL,
                keywords TEXT,
                value FLOAT,
                currency VARCHAR(8),
                content_name VARCHAR(256),
                content_ids VARCHAR(512),
                custom_data_json TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE
            )
        """,
    },
    # v4: user_sessions table
    {
        "id": "v4_03_user_sessions",
        "check": "SELECT table_name FROM information_schema.tables WHERE table_name='user_sessions'",
        "sql": """
            CREATE TABLE user_sessions (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                tg_user_id BIGINT NOT NULL,
                tg_username VARCHAR(64),
                tg_first_name VARCHAR(128),
                tg_phone VARCHAR(32),
                session_key VARCHAR(64) NOT NULL,
                fbclid VARCHAR(256),
                client_ip VARCHAR(64),
                user_agent TEXT,
                fired_triggers VARCHAR(256) NOT NULL DEFAULT '',
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (tg_user_id, campaign_id)
            )
        """,
    },
    {
        "id": "v4_03b_user_sessions_idx",
        "check": "SELECT indexname FROM pg_indexes WHERE indexname='idx_us_session_key'",
        "sql": "CREATE INDEX idx_us_session_key ON user_sessions(session_key)",
    },
    # v4: webhook_tokens table
    {
        "id": "v4_04_webhook_tokens",
        "check": "SELECT table_name FROM information_schema.tables WHERE table_name='webhook_tokens'",
        "sql": """
            CREATE TABLE webhook_tokens (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                token VARCHAR(64) NOT NULL UNIQUE,
                label VARCHAR(128),
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_hit_at TIMESTAMPTZ,
                hit_count INTEGER NOT NULL DEFAULT 0
            )
        """,
    },
    # v4: new columns on conversion_logs
    {
        "id": "v4_05_conv_logs_trigger_id",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='trigger_id'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN trigger_id INTEGER REFERENCES conversion_triggers(id) ON DELETE SET NULL",
    },
    {
        "id": "v4_05b_conv_logs_trigger_type",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='trigger_type'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN trigger_type VARCHAR(32)",
    },
    {
        "id": "v4_05c_conv_logs_event_value",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='event_value'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN event_value FLOAT",
    },
    {
        "id": "v4_05d_conv_logs_event_currency",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='event_currency'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN event_currency VARCHAR(8)",
    },
    {
        "id": "v4_05e_conv_logs_content_name",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='content_name'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN content_name VARCHAR(256)",
    },
    {
        "id": "v4_05f_conv_logs_meta_event_id",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='meta_event_id'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN meta_event_id VARCHAR(64)",
    },
    {
        "id": "v4_05g_conv_logs_fbtrace_id",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_logs' AND column_name='fbtrace_id'",
        "sql": "ALTER TABLE conversion_logs ADD COLUMN fbtrace_id VARCHAR(64)",
    },
    # v4: new CHANNEL value for account_type enum
    {
        "id": "v4_06_account_type_channel",
        "check": "SELECT enumlabel FROM pg_enum pe JOIN pg_type pt ON pe.enumtypid=pt.oid WHERE pt.typname='accounttype' AND pe.enumlabel='CHANNEL'",
        "sql": "ALTER TYPE accounttype ADD VALUE IF NOT EXISTS 'CHANNEL'",
    },
    # v4: new TriggerType enum
    {
        "id": "v4_07_triggertype_enum",
        "check": "SELECT typname FROM pg_type WHERE typname='triggertype'",
        "sql": """
            CREATE TYPE triggertype AS ENUM (
                'click','bot_start','first_message','any_message',
                'keyword','channel_join','manual','webhook'
            )
        """,
    },
    # v4: new EventType values
    {
        "id": "v4_08_eventtype_addtocart",
        "check": "SELECT enumlabel FROM pg_enum pe JOIN pg_type pt ON pe.enumtypid=pt.oid WHERE pt.typname='eventtype' AND pe.enumlabel='AddToCart'",
        "sql": "ALTER TYPE eventtype ADD VALUE IF NOT EXISTS 'AddToCart'",
    },
    {
        "id": "v4_08b_eventtype_addpaymentinfo",
        "check": "SELECT enumlabel FROM pg_enum pe JOIN pg_type pt ON pe.enumtypid=pt.oid WHERE pt.typname='eventtype' AND pe.enumlabel='AddPaymentInfo'",
        "sql": "ALTER TYPE eventtype ADD VALUE IF NOT EXISTS 'AddPaymentInfo'",
    },
    {
        "id": "v4_08c_eventtype_starttrial",
        "check": "SELECT enumlabel FROM pg_enum pe JOIN pg_type pt ON pe.enumtypid=pt.oid WHERE pt.typname='eventtype' AND pe.enumlabel='StartTrial'",
        "sql": "ALTER TYPE eventtype ADD VALUE IF NOT EXISTS 'StartTrial'",
    },
    {
        "id": "v4_08d_eventtype_schedule",
        "check": "SELECT enumlabel FROM pg_enum pe JOIN pg_type pt ON pe.enumtypid=pt.oid WHERE pt.typname='eventtype' AND pe.enumlabel='Schedule'",
        "sql": "ALTER TYPE eventtype ADD VALUE IF NOT EXISTS 'Schedule'",
    },

    # ── v5: Funnel redesign ──────────────────────────────────────────
    # Funnel steps now link to a specific ConversionTrigger row (by id)
    # instead of a loose trigger_type string + optional campaign_id.
    # This makes a step mean "count of fires for THIS exact configured
    # trigger" rather than "count of fires for this trigger type across
    # campaigns" — required for campaigns with multiple triggers of the
    # same type (e.g. two separate keyword triggers).
    #
    # funnel_events is dropped entirely: step counts are now computed
    # live from conversion_logs (grouped by trigger_id), since every
    # trigger fire already writes a row there. No separate completion
    # log to keep in sync, no possibility of drift between the two.
    #
    # Old funnel/funnel_step data predates trigger-linking and can't be
    # mapped forward automatically (there's no trigger_id to backfill
    # from a free-text trigger_type), so old funnels are dropped here.
    # Campaigns, accounts, conversions, and messages are untouched.
    {
        "id": "v5_01_drop_funnel_events",
        "check": "SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='funnel_events')",
        "sql": "DROP TABLE IF EXISTS funnel_events CASCADE",
    },
    {
        "id": "v5_02_drop_old_funnel_steps",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='funnel_steps' AND column_name='trigger_id'",
        "sql": "DROP TABLE IF EXISTS funnel_steps CASCADE",
    },
    {
        "id": "v5_03_drop_old_funnels",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='funnels' AND column_name='campaign_id'",
        "sql": "DROP TABLE IF EXISTS funnels CASCADE",
    },
    {
        "id": "v5_04_create_funnels",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='funnels' AND column_name='campaign_id'",
        "sql": """
            CREATE TABLE funnels (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES dashboard_users(telegram_id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                name VARCHAR(256) NOT NULL,
                description TEXT,
                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
    },
    {
        "id": "v5_04b_funnels_idx",
        "check": "SELECT indexname FROM pg_indexes WHERE indexname='idx_funnel_campaign'",
        "sql": "CREATE INDEX idx_funnel_campaign ON funnels(campaign_id)",
    },
    {
        "id": "v5_05_create_funnel_steps",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='funnel_steps' AND column_name='trigger_id'",
        "sql": """
            CREATE TABLE funnel_steps (
                id SERIAL PRIMARY KEY,
                funnel_id INTEGER NOT NULL REFERENCES funnels(id) ON DELETE CASCADE,
                trigger_id INTEGER NOT NULL REFERENCES conversion_triggers(id) ON DELETE CASCADE,
                step_order INTEGER NOT NULL,
                label VARCHAR(128)
            )
        """,
    },
    {
        "id": "v5_05b_funnel_steps_idx1",
        "check": "SELECT indexname FROM pg_indexes WHERE indexname='idx_fs_funnel'",
        "sql": "CREATE INDEX idx_fs_funnel ON funnel_steps(funnel_id)",
    },
    {
        "id": "v5_05c_funnel_steps_idx2",
        "check": "SELECT indexname FROM pg_indexes WHERE indexname='idx_fs_trigger'",
        "sql": "CREATE INDEX idx_fs_trigger ON funnel_steps(trigger_id)",
    },
    # v6: username/password dashboard login (no Telegram needed)
    {
        "id": "v6_01_dashboard_users_login_username",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='login_username'",
        "sql": "ALTER TABLE dashboard_users ADD COLUMN login_username VARCHAR(64) UNIQUE",
    },
    {
        "id": "v6_02_dashboard_users_password_hash",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='password_hash'",
        "sql": "ALTER TABLE dashboard_users ADD COLUMN password_hash VARCHAR(256)",
    },
    # v6: keyword trigger match mode (any/all) — supports AND-mode keyword groups
    {
        "id": "v6_03_conversion_triggers_match_mode",
        "check": "SELECT column_name FROM information_schema.columns WHERE table_name='conversion_triggers' AND column_name='match_mode'",
        "sql": "ALTER TABLE conversion_triggers ADD COLUMN match_mode VARCHAR(8) NOT NULL DEFAULT 'any'",
    },
]


async def run_migrations():
    print("=== TG Tracker Database Migration ===\n")

    # First ensure base tables exist
    await init_db()
    print("Base tables checked ✅\n")

    async with engine.begin() as conn:
        for m in MIGRATIONS:
            mid = m["id"]
            # Check if already applied
            result = await conn.execute(text(m["check"]))
            rows = result.fetchall()
            if rows:
                print(f"  ✓  {mid} (already applied)")
                continue
            try:
                await conn.execute(text(m["sql"]))
                print(f"  ✅ {mid}")
            except Exception as e:
                if "already exists" in str(e).lower() or "already been added" in str(e).lower():
                    print(f"  ✓  {mid} (already exists)")
                else:
                    print(f"  ❌ {mid}: {e}")
                    raise

    print("\n✅ All migrations complete. You can now restart the app.\n")


if __name__ == "__main__":
    asyncio.run(run_migrations())

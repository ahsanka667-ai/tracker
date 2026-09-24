# Inspection Report + Implementation Plan (v5 upgrade)

Produced by inspecting every file in this repository (no external docs assumed).

## 1. What already exists (verified by reading the code)

| Area | File(s) | State found |
|---|---|---|
| FastAPI app, dashboard static hosting, WS hub, auth, campaigns, triggers, funnels, inbox, webhooks, manual conversions | `service_a/main.py` (1926 lines, 60+ routes) | **Mostly working** |
| Click capture `/t/{slug}` | `service_a/main.py::capture_click` | **Partially working** — Redis-only payload, 72h TTL, no DB record |
| Telethon worker (BOT + PERSONAL + CHANNEL), join requests, keyword triggers, first/any message, channel monitoring, sign-in queue, heartbeat, admin alerts | `service_b/worker.py` (1075 lines) | **Working but architecture-level problems** (see below) |
| Meta CAPI dispatcher with retry/backoff, user_data hashing, custom_data, event_id | `service_b/meta_capi.py` | **Working, wrong fbc semantics** |
| Master bot (invite tokens, TWA launcher, admin commands) | `master_bot/bot.py` | Working, but never records a tracking event and does nothing with `/start <clickkey>` |
| WebSocket notifications (Redis pub/sub → WS), live toasts/badge | `service_a/websocket_manager.py`, dashboard | **Working — preserve** |
| Auth: PBKDF2 local login, Telegram initData HMAC, Login-widget HMAC, opaque Redis session tokens | `shared/security.py`, `service_a/main.py` | **Working and correct — preserve** |
| Config validation at startup, rotating file logs, rate limiting, crawler filtering, `GETDEL` atomic click consume | `shared/config.py`, `shared/logging_config.py`, worker | **Working — preserve** |
| Dashboard SPA (vanilla, dark SaaS, 2275 lines) | `dashboard/index.html` | Working; nav is 7 tabs vs ~40 screens requested |
| Idempotent schema-upgrade script | `scripts/migrate.py` (v4_01…v6_03) | Working — extend |
| Alembic scaffold | `alembic.ini`, `migrations/env.py` | Present but **zero revision files** and env.py imports only 3 models |
| Docker compose (postgres/redis/service_a/service_b/master_bot) | `docker-compose.yml`, `Dockerfile` | Working; no CAPI/queue worker service |
| Tests | — | **None at all** |

## 2. Concrete bugs / gaps found (each one is addressed in this upgrade)

1. **`fbc` regenerated at conversion time** — `meta_capi.build_user_data()` does
   `ud["fbc"] = f"fb.1.{int(time.time()*1000)}.{fbclid}"` on *every* fire. Meta
   expects the fbc captured at browser time; regenerating the timestamp degrades
   match quality and breaks dedup semantics. **Fix:** `fbc` is built once, at click
   time, stored on the click row, and reused verbatim.
2. **`fbp` never captured** — no `_fbp` cookie read anywhere; `build_user_data(fbp=…)`
   exists but every caller passes nothing → `fbp` is always NULL. **Fix:** real
   first-party `_fbp` read + browser bridge write-back to the click; never invented.
3. **Clicks are not persisted** — only `redis SETEX click:{key}` (default 72h TTL).
   After TTL everything (fbclid, campaign, IP, UA) is gone; no click history, no
   multi-click support, no "never lose original attribution". **Fix:** `clicks` table
   (full record), Redis kept only as the fast correlation cache.
4. **Meta CAPI called synchronously from Telegram handlers** — `fire_trigger()`,
   `handle_bot_start`, `handle_join_request`, `_fire_default_or_triggers`,
   `/api/conversions/manual`, `/webhook/{token}` all `await fire_event(...)` inline.
   A Meta 12s timeout × 3 retries stalls the Telethon handler. **Fix:** event engine
   writes a `meta_events` row + pushes to Redis queue; a separate CAPI worker sends
   with retry/backoff/`DEAD_LETTER`.
5. **`mark_trigger_fired(session, "bot_start")`** in `handle_bot_start` passes a
   *string* where a trigger **id** is expected → writes the literal `bot_start` into
   the dedup bitmask, so the fallback path dedups nothing (and could collide with an
   integer id parse). **Fix:** dedup via `meta_events.dedup_key` + unique constraints.
6. **No Pixel/CAPI dedup pairing** — a `event_id` is generated per CAPI call and the
   browser Pixel isn't installed at all, so nothing can dedup. **Fix:** one
   `event_id` generated server-side, injected into the landing-page Pixel
   (`fbq('track', name, params, {eventID})`) and reused by CAPI.
7. **Channel-join attribution loss** — `handle_channel_join` builds a session from
   `{"session_key": "organic"}` (see the README's own "known limitation"). Bot→channel
   journeys lose `fbclid`/campaign. **Fix:** attribution engine resolves identity →
   eligible click for every event including joins.
8. **Duplicate Telegram identity tables/none at all** — attribution is keyed on
   `user_sessions.tg_user_id` per campaign only; there is no unified identity record,
   no cross-campaign profile, no CRM profile. **Fix:** `telegram_identities` (unique
   per (user, telegram_user_id)) + one `resolve_or_create_identity()` used everywhere.
9. **Monitor-account validation contradicts docs** — `POST /api/accounts/channel`
   rejects non-PERSONAL monitors ("Monitor account must be a personal account") while
   the README and the frontend both say bots are the recommended monitor and are
   supported by the backend. **Fix:** allow BOT or PERSONAL.
10. **Channel wiring needs a worker restart** — `targets` resolved once in
    `boot_account`. **Fix:** Redis pub/sub `tg:reload_channels` signal → re-resolve.
11. **`/ws/{user_id}` is unauthenticated** — anyone who opens a socket with your
    numeric id receives your click/conversion notifications. **Fix:** require the
    session token (or a short-lived signed WS ticket) and derive the user from it.
12. **No DB indexes / unbounded queries** for the hot paths (`fbclid`,
    `telegram_user_id + created_at`, `event_type`, `meta status`); `/api/clicks/pending`
    does a full Redis `SCAN` per request. **Fix:** index set from spec §42, keyset
    pagination, capped scans.
13. **Alembic unusable** — no revisions, `migrations/env.py` imports 3 of 11 models so
    autogenerate would drop everything. **Fix:** import `shared.models` wholesale +
    real revision files, `scripts/migrate.py` kept for the "safe to re-run" path.
14. **No tests, no CI, `requirements.txt` has no test deps**; `sqlite3.OperationalError`
    referenced in `boot_account` without importing `sqlite3` → that except clause
    raises `NameError` inside an error handler on locked sessions. **Fix:** all of it.
15. **Silent exception swallowing** — `except Exception: pass` in the Redis subscriber
    and several handlers. **Fix:** structured error logging with event codes.
16. **`redis[asyncio]==5.0.4`** — non-existent extra (pip warning). **Fix:** `redis>=5`.
17. Secrets handling: `TelegramAccount.meta_capi_token`/`identifier` (bot token) are
    plaintext at rest, `PATCH /api/accounts/{id}` accepts `""` and `None`
    interchangeably, `GET /api/accounts` hides the token (good) but `PATCH` does not
    validate. **Fix:** at-rest AES-GCM encryption (optional key), masked responses,
    validation, and a log-redaction filter so tokens/session strings never hit logs.

## 3. Architecture after the upgrade

```
service_a (FastAPI)     : tracking domains/links, /t|/c click engine, hosted landing
                          page + Meta Pixel, /api/* (traffic, telegram, crm, analytics,
                          meta, funnels, flows, settings, webhooks, API keys), WS hub,
                          health endpoints, OpenAPI docs
service_b/worker.py     : Telethon (BOT/PERSONAL/CHANNEL) + bot HTTP webhook ingest
                          → shared event engine (never calls Meta directly)
service_b/capi_worker.py: Redis queue consumer → Meta Graph API, retry/backoff/DLQ,
                          writes status back to meta_events + conversion_logs
service_b/jobs_worker.py: automation/flows (+waits), funnel progress, analytics
                          rollups, retention sweep, health heartbeats
shared/                 : config, database, models, security, tokens, events engine,
                          attribution engine, meta payload builder, flow engine, queue,
                          ua/geo/privacy, structured logging
master_bot/bot.py       : access tokens + tracking deep-link handoff to the same engine
dashboard/              : existing SPA extended to the requested IA (real API data only)
```

Primary data flow is the one in the spec:
`Meta ad → tracking link → click record (+fbc/fbp/sub1-9/device/geo) →
Telegram destination (bot/channel/DM bridge/Mini App) → signed correlation token →
identity + event → attribution engine → funnel + automation → Meta Pixel/CAPI
(same event_id, stored fbc/fbp, queued) → analytics + CRM + user journey`.

## 4. Deliberate non-goals / honesty list

- No ad platform other than Meta (spec §48). No generic "connector" abstraction.
- Telegram gives no tracking parameter to `t.me/<channel>` links: attribution into
  channel joins is recovered through the **identity → click** attribution engine (and
  the channel CTA link path), never by pretending a channel URL carries `fbclid`.
- A personal-account DM can only be correlated through a bridge/Mini App token, and
  only messages the connected account can actually see are read. No "magic" tracking.
- Join-request approval only happens for chats where the connected identity can
  moderate; otherwise the event is recorded and `auto_approve` is reported as skipped.
- `fbp` is only ever the real `_fbp` cookie value; if a visitor never runs the Pixel,
  `fbp` stays NULL and the CAPI event ships without it.

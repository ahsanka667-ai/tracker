# TG Tracker v3 — Private Telegram Ad Conversion Tracker + Inbox + Funnels

Track Meta (Facebook/Instagram) ad conversions across Telegram bots, personal
accounts, and private channels — with real-time dashboard notifications,
a unified message inbox, and multi-step funnel/drop-off analytics.

---

## What's New in v3

- **Real-time notifications** — WebSocket-powered live alerts (bell icon + toasts)
  for new conversions, clicks, and incoming messages
- **Message Inbox** — all incoming DMs from your personal accounts/bots in one
  place, with read/unread tracking and per-conversation view
- **Funnels** — define multi-step funnels (Ad Click → Bot Start → DM → Purchase)
  and see drop-off percentages at every stage
- **Reusable invite tokens** — one link can onboard multiple users until it expires
- **Admin account visibility** — admins see all accounts across all users for
  campaign assignment

---

## Quick Start (Local / WSL2) — no Telegram required to start

**Upgrading an existing install?** New columns (username/password login,
keyword match modes) don't appear automatically on a database that
already exists — `init_db()` only creates missing tables, not missing
columns on ones you already have. Run this once after pulling an update:
```bash
source venv/bin/activate
python scripts/migrate.py
```
It's safe to run repeatedly — every step checks whether it's already
applied before touching anything. Skip this only on a genuinely fresh
database (first-ever run) — `init_db()` builds the full current schema
there already.

```bash
cd ~
cp /mnt/c/Users/YOUR_USERNAME/Downloads/tg_tracker.zip .
unzip -o tg_tracker.zip
cd tg_tracker

cp .env.example .env
nano .env   # set ADMIN_USERNAME + ADMIN_PASSWORD (that's the whole minimum!)

pip install -r requirements.txt
bash start.sh
```

Open `http://localhost:8000/dashboard/` and log in with the username/password
you set. **You do not need MASTER_BOT_TOKEN, TELEGRAM_API_ID/HASH, or
ADMIN_TELEGRAM_ID to reach this point** — `start.sh` detects they're
missing and just skips the two Telegram-only processes (Service B, the
worker; and the master bot), so you can explore the dashboard, campaigns,
and UI with zero Telegram setup.

**Telegram credentials only become necessary once you actually want to
track something** — because the product's whole job is turning Telegram
activity into Meta conversion events, so at that point you do need real
Telegram API access. See "Adding Telegram tracking" further down for
exactly when/how.

If you also want the ngrok URL for browser testing:
```bash
bash start_ngrok.sh
```
Copy the printed `https://xxxx.ngrok-free.app` URL into `.env` as `BASE_URL`,
restart `start.sh`, and open `https://xxxx.ngrok-free.app/dashboard/`.

---

## New Database Tables (auto-created on first run)

| Table | Purpose |
|---|---|
| `messages` | Every inbound/outbound message — powers the Inbox tab |
| `funnels` | Named funnel definitions |
| `funnel_steps` | Ordered steps within a funnel (trigger type + value) |
| `funnel_events` | Records every time a user completes a step |

If you're upgrading from v2, just restart the app — `init_db()` creates the
new tables automatically without touching existing data.

---

## How Real-Time Notifications Work

```
Service B (Telethon) ──publish──▶ Redis pub/sub "tg_notifications"
                                          │
Service A ──subscribe──▶ WebSocket ──▶ Dashboard (bell icon + toast)
```

Every dashboard tab opens a WebSocket to `/ws/{telegram_id}` on load. When
Service B detects a conversion, new message, or click, it publishes an event
to Redis. Service A forwards it to the right user's open dashboard tabs
instantly — no polling.

---

## Funnels — How They Work (v5)

A funnel measures the journey of **one campaign's traffic** through a
sequence of steps you define — e.g. "Bot /start → First Reply →
Purchase Keyword." Every funnel is tied to exactly one campaign.

**Each step links to one specific, already-configured trigger** — not
a generic trigger type. This matters because a campaign can have
several triggers of the same type: one keyword trigger firing
`InitiateCheckout` on `interested,price,how much`, and a second
keyword trigger firing `Purchase` on `paid,bought,done`. A funnel step
means "this exact trigger, with these exact keywords" — never a vague
"any keyword match," and never accidentally mixing two different
keyword triggers' counts together.

**Step counts are computed live** from the same `conversion_logs` table
the Conversions tab reads — there's no separate funnel tracking table
to fall out of sync. A funnel step's count is always identical to what
you'd see filtering Conversions by that trigger. Drop-off % is
`1 - (step_N_count / step_1_count) * 100`.

### Three ways to build a funnel

**1. Auto-generate (recommended to start)**
Configure 2+ triggers on a campaign (Campaigns → ⚙️ Configure → Add
Trigger), then click **⚡ Build Funnel** in that same modal, or
**⚡ Auto-generate** on the Funnels tab. This builds a funnel from your
triggers in their configured order — zero manual step-picking. Edit a
trigger's keywords later and the funnel automatically reflects it,
since the step always points at the live trigger, not a snapshot.

**2. Manual**
Funnels tab → New Funnel → pick a campaign → click triggers in the
order they should happen → Create. Use this when you want a funnel
that's a *subset* of your triggers, or in a different order than
they're configured.

**3. Apply a campaign template**
Templates (Lead Generation, Chat Qualified, Purchase Tracking, Full
Funnel) configure triggers *and* auto-build a matching funnel in one
step when you create a campaign.

### Editing
Funnels tab → ✏️ Edit Steps on any funnel → add/remove/reorder which
triggers are included. Editing an auto-generated funnel converts it to
manual (shown without the "Auto" badge) — future auto-generate runs on
that campaign won't touch your edit; they create a fresh funnel instead.

---

## Message Inbox

- Every incoming Telegram message (DMs to personal accounts, `/start` commands
  to bots) is saved to the `messages` table
- The Inbox tab groups messages by conversation (account + chat)
- Unread badge appears on the Inbox tab and updates in real-time via WebSocket
- Click a conversation to view full history and mark as read

---

## How DM / Channel / Group Tracking Works (and how to verify it)

There are four kinds of "account" you can add on the Accounts tab, and
each one unlocks a different set of trigger types:

| Account type | What it is | Trigger types it can fire |
|---|---|---|
| **BOT** | A bot token from @BotFather | `bot_start`, `first_message`, `any_message`, `keyword` |
| **PERSONAL** | Your own Telegram account (phone sign-in) | `first_message`, `any_message`, `keyword` (in DMs to that account) |
| **CHANNEL** | A channel or group you want to monitor | `channel_join`, `keyword` (in messages posted in that chat) |
| — | External systems (payment gateway, CRM, etc.) | `webhook` (POST to `/webhook/{token}`) |

### DMs (BOT and PERSONAL accounts)
This is the most-exercised path and requires nothing extra: add the
account, create a campaign pointed at it, add a trigger (or use a
template), and message it. `/start <key>` from a tracked link fires
`bot_start`; any reply after that can fire `first_message` or `keyword`
triggers. **`/start` with no key attached to it does NOT fire a
conversion by itself** — a click must have happened first so there's a
key to consume, or the message needs to match a keyword trigger.

**To verify:** Accounts tab → your bot/personal account → **Test Pixel**
button fires a synthetic CAPI event immediately, independent of any real
Telegram interaction — use this first to confirm your Pixel ID/CAPI
token are correct before testing the real flow. Then click your own
tracking link (Campaigns tab → copy link) and message the bot/account —
you should see a real-time toast notification and a new row on the
Conversions tab within a couple seconds.

### Channels and Groups (CHANNEL accounts)
This is the part that needed fixing — worth reading if you're setting
one up for the first time. A CHANNEL account doesn't get its own
Telegram connection (Telegram only allows one active session per phone
number); instead you **link it to an account that's already connected**
— either a bot or a personal account — and that account listens on the
channel's behalf.

**Bot is the recommended monitor, not a personal account.** A bot
added as channel/group admin is the standard, Telegram-sanctioned way
to do this — it doesn't carry the risk a personal account does of
being flagged for automated behavior, and you can reuse the same bot
you're already using for DM tracking (one bot, multiple channels, all
as admin). Use a personal account only if you specifically need
something a bot can't do in that chat.

**Setup:**
1. Add (or already have) a **BOT** account — @BotFather token, same as
   for DM tracking. You can reuse an existing one.
2. Add that bot to the channel/group **as admin** (Telegram only
   surfaces join events to admins, whether the listener is a bot or a
   personal account).
3. Add a **CHANNEL** account: identifier = the channel/group's
   `@username` or invite link, and set **"Monitor via Account"** to the
   bot from step 1.
4. Point a campaign's account at this CHANNEL account, add a
   `channel_join` and/or `keyword` trigger.
5. **Restart Service B (the worker)** — channel/group linkage is resolved
   once at worker startup, so a newly-added or newly-changed CHANNEL
   account won't be picked up until the worker restarts. (This is a
   known limitation — see "What's Next" below.)

**What actually fires `channel_join`:** a plain "user joined the
channel/group" event — no join-request/approval workflow required. If
the chat has "approve new members" turned on, join *requests* are also
caught and auto-approved.

**Important — this tracks joins, it does NOT by itself attribute them
to an ad click.** Telegram has no mechanism for a channel link to carry
a tracking key the way `t.me/yourbot?start=<key>` does for bots — so a
channel-join conversion currently fires as "organic" (no fbclid), even
if the person genuinely clicked your tracked link first. If you need
the join attributed to a specific ad/click, route the link through
your bot first (which captures full attribution) and have the bot
prompt a "Join Channel" button — see "What's Next" for the planned fix
that carries that attribution into the join event instead of marking
it organic.

**To verify:** check the worker's logs after it boots — you should see
`Channel account N ('...') resolved -> chat_id=..., now monitored`. If
you instead see `could not resolve` or nothing at all, the monitor
account isn't a member/admin of that chat yet, or the identifier is
wrong (an invite link that's already been "used up" resolves
differently than a fresh one — prefer `@username` for public channels).
Then have a test
account join the channel — you should see a `channel_join` conversion
appear within a few seconds, same as the DM flow.

### Known limitation
Channel/group message and join tracking is resolved once at worker
boot — adding, editing, or re-linking a CHANNEL account requires
restarting Service B to take effect. This isn't hot-reloaded.

---

## Project Structure
```
tg_tracker/
├── service_a/
│   ├── main.py                 FastAPI: click tracking, full REST API, WebSocket hub
│   ├── websocket_manager.py    Connection manager for real-time notifications
│   └── __init__.py
├── service_b/
│   ├── worker.py                Telethon: tracking, message saving, funnel events, notify()
│   ├── meta_capi.py             Meta Conversions API dispatcher
│   └── __init__.py
├── master_bot/
│   └── bot.py                   Telegram bot: access tokens, Web App launcher
├── shared/
│   ├── models.py                 9 tables: users, tokens, accounts, campaigns,
│   │                              conversion_logs, messages, funnels, funnel_steps, funnel_events
│   ├── database.py
│   ├── security.py               Password hashing for username/password login
│   └── config.py
├── dashboard/
│   └── index.html                Full dashboard: Overview, Campaigns, Accounts,
│                                  Inbox, Funnels, Conversions, Users, Tokens
├── docker-compose.yml
├── requirements.txt
├── start.sh
├── start_ngrok.sh
└── .env.example
```

---

## Full API Reference

| Method | Path | Description |
|---|---|---|
| GET | `/t/{slug}` | Click capture + redirect |
| WS  | `/ws/{user_id}` | Real-time notification stream |
| POST | `/api/auth/verify` | TWA initData HMAC verification |
| POST | `/api/auth/login-widget` | Telegram Login Widget verification |
| POST | `/api/auth/login` | Username/password login — no Telegram needed |
| POST | `/api/auth/logout` | Revoke current session |
| POST | `/api/me/password` | Set/change your own username+password login |
| GET | `/api/me` | Current user profile |
| GET/POST | `/api/users` | List / grant dashboard access (by Telegram ID) |
| POST | `/api/users/local` | Create a username/password user (no Telegram needed) |
| DELETE | `/api/users/{id}` | Revoke access |
| GET/POST | `/api/tokens` | List / create invite tokens |
| DELETE | `/api/tokens/{id}` | Delete token |
| GET | `/api/accounts` | List accounts (`?all_users=true` for admins) |
| POST | `/api/accounts/bot` | Add bot account |
| POST | `/api/accounts/personal/step1` `/step2` | 2-step personal sign-in |
| PATCH/DELETE | `/api/accounts/{id}` | Edit / remove account |
| POST | `/api/accounts/{id}/test-pixel` | Fire test CAPI event |
| GET/POST | `/api/campaigns` | List / create campaigns |
| PATCH/DELETE | `/api/campaigns/{id}` | Edit / delete campaign |
| GET | `/api/conversions` | Conversion log (filterable) |
| GET | `/api/conversions/summary` | Aggregate stats |
| GET | `/api/messages` | Message inbox |
| GET | `/api/messages/unread-count` | Unread badge count |
| PATCH | `/api/messages/{id}/read` | Mark one message read |
| POST | `/api/messages/read-all` | Mark all read |
| GET/POST | `/api/funnels` | List / create funnels |
| DELETE | `/api/funnels/{id}` | Delete funnel |
| GET | `/api/funnels/{id}/stats` | Step-by-step drop-off stats |

---

## Deployment Checklist (VPS)

1. Point `track.yourdomain.com` A record at your VPS IP
2. `docker compose up -d postgres redis`
3. `pip install -r requirements.txt`
4. Set `BASE_URL=https://track.yourdomain.com` in `.env`
5. Run all 3 processes as systemd services (see previous setup guide)
6. nginx reverse proxy — **must support WebSocket upgrade** for `/ws/`:

```nginx
location /ws/ {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}
```

7. `certbot --nginx -d track.yourdomain.com`
8. @BotFather → `/newapp` → Web App URL: `https://track.yourdomain.com/dashboard/`

---

## Security: Authentication & Login (v4.2)

**Three login paths into the dashboard, in order of what most people
will actually use:**

### 1. Username / password (no Telegram involved)
Set `ADMIN_USERNAME` and `ADMIN_PASSWORD` in `.env`. On every startup,
Service A creates (or refreshes) that admin login — no bot, no widget,
no domain registration, nothing Telegram-related required. Passwords
are stored as salted PBKDF2-SHA256 hashes (260k iterations), never
plaintext. An admin can create additional username/password logins for
teammates from the dashboard's Users tab ("Username/password (no
Telegram)" option) — those people never need a Telegram account either.

### 2. Inside Telegram (Web App) — optional
Opening the dashboard via the bot's menu button verifies Telegram's
signed `initData` payload server-side using HMAC-SHA256 with your bot
token as the key. Only relevant if you've set up `MASTER_BOT_TOKEN`.

### 3. Browser, via Telegram (Login Widget) — optional
Telegram's official **Login Widget** — the person taps "Log in with
Telegram," confirms inside their own Telegram app, and Telegram redirects
back with a signed payload we verify the same way (different HMAC key
per Telegram's login widget spec). The dashboard only renders this
widget at all if `GET /api/bot-info` reports a configured bot — if you
haven't set up `MASTER_BOT_TOKEN`, this option simply doesn't appear and
the login screen shows only the username/password form.

**There is intentionally no other way in beyond these three, and none
of them trust an unsigned client-supplied identity.** An earlier version
of this dashboard let anyone type a numeric Telegram ID into a text box
to "log in" — meaning anyone who knew or guessed your ID had full
access, including firing manual conversions and seeing your CAPI
tokens. That path was removed entirely and never reintroduced by the
username/password option, which requires a real password match.

### Session tokens
After any of the three login methods succeeds, the server issues an opaque,
random 256-bit session token (stored in Redis, 7-day sliding expiry).
Every subsequent API call sends `Authorization: Bearer <token>` — the
server resolves the real `telegram_id` from Redis, never from anything
the client claims. If access is revoked while a session is active, the
very next request kills that session automatically. Username/password
users get a synthetic negative `telegram_id` (real Telegram ids are
always positive) so they share the exact same session/access-control
code path as Telegram users — nothing is special-cased or weaker for them.

### Required: register your domain with BotFather (only if using Telegram login)
The Telegram Login Widget only works on domains you've explicitly
registered for your bot:
```
@BotFather → /setdomain → select your bot → enter your domain
e.g. track.yourdomain.com (no https://, no trailing slash)
```
Without this, the widget will refuse to render. Local ngrok testing
needs the same step with your current ngrok hostname — re-run it each
time the ngrok URL changes. **Skip this entirely if you're only using
username/password login.**

### Logging out
Click the logout icon next to your name in the top bar. This revokes
the session token server-side immediately, not just locally.

---

## Production Hardening (v3.1)

This release focuses on reliability and operability — no new user-facing
features, just making the existing system trustworthy enough for real ad spend.

### 1. Config validation at startup
Every service now calls `validate_or_exit()` before doing anything else.
If `MASTER_BOT_TOKEN`, `TELEGRAM_API_ID/HASH`, or `ADMIN_TELEGRAM_ID` are
missing or malformed, the service prints **all** problems at once and exits —
instead of crashing 30 seconds later with a cryptic error three layers deep.

### 2. Meta CAPI retry with exponential backoff
`fire_conversion_event()` now retries up to 3 times (1.5s → 3s → 6s) on:
- Connection errors / timeouts
- 5xx responses from Meta (their servers having issues)

It does **not** retry 4xx responses (bad token, bad pixel ID, malformed
payload) — those will never succeed no matter how many times you ask.
A momentary network blip no longer permanently loses a conversion.

### 3. Atomic click consumption (no more duplicate conversions)
`consume_click_payload()` uses Redis `GETDEL` — read and delete in one
atomic operation. If Telegram redelivers the same `/start` update twice
(common with polling), only the first copy can process it.

### 4. Crawler filtering + click rate limiting
- Link-preview bots (Telegram, WhatsApp, Facebook, etc.) fetching your
  tracking URL for preview cards no longer count as clicks.
- Per-IP rate limiting (`CLICK_RATE_LIMIT_MAX` per `CLICK_RATE_LIMIT_WINDOW_SECONDS`,
  default 20/min) silently drops abusive traffic without revealing the
  rate limit to the client.

### 5. Account-down alerts
The worker now DMs `ADMIN_TELEGRAM_ID` directly (via the bot API, independent
of the dashboard) when:
- A personal account's session is revoked or the account is banned/deactivated
- An account fails to reconnect for ~3 consecutive heartbeats (3 minutes)
- A previously-down account comes back online

### 6. Rotating file logs
All 3 services now log to both stdout AND `logs/{service}.log`
(5MB × 5 backups = 25MB max per service). Survives `systemctl restart`
and doesn't depend on `journalctl`/`docker logs` retention.

### 7. Restricted CORS
Replaced `allow_origins=["*"]` with an explicit allowlist: your `BASE_URL`,
`web.telegram.org` (Telegram's Web App wrapper), and `t.me`. Local dev
(`http://` BASE_URL) also allows `localhost`/`127.0.0.1`.

### 8. Graceful shutdown (SIGTERM handling)
`systemctl stop` / `docker stop` send SIGTERM, which does **not** raise
`KeyboardInterrupt` in Python by default — the process used to die mid-write,
risking corrupted Telethon session files. Now both SIGTERM and SIGINT trigger
a clean disconnect of all Telegram clients before exit.

### 9. Database + session backups
`scripts/backup_db.sh` — dumps PostgreSQL (gzip'd) and tars your Telethon
`sessions/` directory (these can't be regenerated without re-authenticating
every personal account from scratch). Keeps 14 days of history. Set up as
a daily cron job:
```bash
crontab -e
# add:
0 3 * * * cd /path/to/tg_tracker && bash scripts/backup_db.sh >> logs/backup.log 2>&1
```

### 10. New debug endpoint: pending clicks
`GET /api/clicks/pending` — lists clicks sitting in Redis that haven't
converted yet, with remaining TTL. Lets you distinguish "user hasn't opened
Telegram yet" from "something's actually broken".

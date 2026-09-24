# TG Tracker — Audit Findings & What's Next

This file covers three things you asked for: what was actually broken
in DM/channel/group tracking, what the login change did, and how this
compares to tgtracker.io if you want to close the gap further.

---

## 1. What I found when checking DM / channel / group tracking

### DMs — working correctly
Bot `/start <key>`, first-message, any-message, and keyword triggers
for both BOT and PERSONAL accounts were already implemented correctly
and are unchanged. Dedup-by-trigger-id, session attribution, and the
Meta CAPI firing path all check out.

### Channels/Groups — was NOT actually working (now fixed)
The `CHANNEL` account type existed in the database model, the README
claimed it worked ("monitors member joins"), and the dashboard let you
add one — but the worker's `boot_account()` function did nothing with
it beyond logging a message and returning. **No event handler was ever
registered for a CHANNEL account.** If you'd added one and waited for a
join, nothing would have fired, with no error anywhere to tell you why.

Separately, the one join-tracking mechanism that *did* exist
(`UpdateBotChatInviteRequester`) only covers chats with "approve new
members" turned on — a plain public channel join was never caught by
anything, even on a personal/bot account that had message handlers
registered.

**What I changed:** CHANNEL accounts now link to a monitor account
(bot or personal) via "Monitor via Account," and once that monitor
account's Telegram client boots, it registers two additional handlers
scoped to the linked chat: one for plain joins (`events.ChatAction`,
no approval workflow needed) and one for keyword matching on messages
posted in that group/channel. See the README's new "How DM / Channel /
Group Tracking Works" section for setup steps and how to verify it.

**Update — bot-as-monitor is now the recommended setup, not personal
account.** The dashboard's "Monitor via Account" dropdown only offered
PERSONAL accounts at first — the backend already supported a bot too,
so this was a frontend gap, now fixed. A bot admin in the channel is
the standard, lower-risk way to do this (no risk of a personal account
getting flagged for automation), and it can be the same bot already
used for DM tracking.

**Residual limitation:** this linkage is resolved once when the worker
boots, not hot-reloaded. Add/change a CHANNEL account → restart Service
B. Fixing that hot-reload gap is the top item in the roadmap below.

---

## 2. What the login change did

- `.env`: `ADMIN_USERNAME` + `ADMIN_PASSWORD` → the dashboard now boots
  and lets you log in without any Telegram bot, widget, or domain setup.
- Telegram credentials (`MASTER_BOT_TOKEN`, `TELEGRAM_API_ID/HASH`,
  `ADMIN_TELEGRAM_ID`) are only fatal for Service B (the worker) and the
  master bot now — because those two processes genuinely can't do
  anything without Telegram access. The dashboard (Service A) treats
  them as optional.
- `start.sh` detects missing Telegram credentials and skips launching
  the worker/master bot cleanly instead of crash-looping with config
  errors in your terminal.
- Admins can create more username/password logins for teammates from
  the dashboard (Users tab), independent of Telegram entirely.
- Passwords are PBKDF2-SHA256 hashed (stdlib only, no new dependency
  added to `requirements.txt`).

---

## 3. Roadmap — in rough priority order

0. **Attribution carryover from bot → channel join.** Right now a
   channel join always fires as "organic" (no fbclid) because Telegram
   gives channel links no way to carry a tracking key the way
   `t.me/yourbot?start=<key>` does for bots. The fix: when someone
   clicks a tracked bot link, gets attributed via `bot_start`, and the
   bot prompts them to join a linked channel — carry that same fbclid
   into the resulting `channel_join` conversion instead of treating it
   as a fresh organic session. Also needs: a configurable welcome
   message + "Join Channel" inline button sent after `/start` fires
   (doesn't exist at all right now — the bot currently sends nothing
   back to the user). This is the top priority if channel-join ad
   attribution matters for your campaigns, not just join *counts*.
1. **Hot-reload channel/group linkage.** Right now a new CHANNEL
   account needs a worker restart. A `/api/internal/reload-channels`
   endpoint (or a Redis pub/sub signal the worker listens for) that
   re-resolves `targets` without a full restart would close this.
2. **VPS deployment.** Still the biggest real gap — everything so far
   has been local/ngrok. Given the Dhaka ISP blocking inbound 80/443,
   this needs an actual VPS. The Deployment Checklist section in
   README.md is ready to follow once you pick a provider.
3. **Local-login self-service password reset.** Right now only an
   already-logged-in admin can create local users, and a locked-out
   local user has no self-serve recovery path (no email system exists
   to email a reset link to). Worth a simple "admin resets it for you"
   flow at minimum, which already technically works via `/api/users/local`
   admin re-creation, but isn't a clean UX yet.
4. **Multi-account channel resolution robustness.** If the linked
   personal account's session drops and reconnects, re-verify that
   channel handlers re-register on reconnect (the current heartbeat
   loop explicitly skips CHANNEL rows, which is correct, but doesn't
   re-check whether the *monitor* account's reconnect re-wires its
   channels — worth a direct test once you have a real channel set up).
5. **Test coverage for the channel/group handlers.** These are new and
   have not been exercised against live Telegram traffic — recommend
   a dedicated test channel before pointing this at real ad spend.

---

## 4. On "I want tgtracker.io's system but simpler"

tgtracker.io (and similar products like tgtrack.io / tgtracker.in) is a
multi-tenant SaaS aimed at agencies and media buyers. The core loop —
tracked link → Telegram action (bot start / DM / channel join) →
server-side conversion event — is the same thing TG Tracker already
does. Where they go further, roughly in order of how much work each
would be to add here:

**Already equivalent / close:**
- Tracked links with fbclid capture → Telegram action → CAPI event
- Channel join tracking with auto-approve on join requests
- Real-time event visibility (their "Event Explorer" ≈ your
  Conversions tab + WebSocket toasts)
- Funnel/step drop-off visibility

**Meaningfully bigger asks than what's here, decreasing by effort:**
- **CSV export of conversions/clicks** — genuinely simple: one new
  endpoint that streams the existing `conversion_logs` query as CSV.
  Worth doing if you want reporting/audits.
- **Country/IP breakdown filters on the Conversions tab** — the data
  (`client_ip`) is already captured and logged; this is a dashboard
  filtering/display feature, not new tracking infrastructure.
- **Persistent cross-campaign visitor profile IDs** — tgtracker.io
  links a visitor's clicks across campaigns/time via a first-party
  cookie/profile system. Your `UserSession` table is per-campaign by
  design (deliberately, so drop-off math per campaign stays clean) —
  adding a separate cross-campaign identity table is a real schema
  addition, moderate effort.
- **TikTok / Google Ads support** — TG Tracker is Meta-only by design.
  Each additional ad platform means a new CAPI-equivalent integration
  (TikTok Events API, Google Ads Enhanced Conversions) — this is the
  single biggest lift on the list, comparable in size to the original
  Meta CAPI integration.
- **OAuth-based multi-account ad platform linking** — tgtracker.io lets
  a user connect their own Meta/TikTok/Google accounts via OAuth inside
  the product. You're currently pasting a Pixel ID + CAPI token per
  account manually, which is simpler to build and, for a single-operator
  setup, arguably simpler to *use* too — probably not worth the OAuth
  complexity unless you're onboarding other people's ad accounts.

If the goal is genuinely "simpler than tgtracker.io," the current
scope (self-hosted, Meta-only, manual pixel/token entry, single
operator or small team) already reflects that trade-off well — the
two additions I'd actually recommend if you want more polish without
much more complexity are the CSV export and the IP/country filter,
both of which are a few hours of work each, not a rebuild.

---

## 5. Keyword trigger upgrades (this round)

- **Multiple keyword→event groups per campaign already worked** — this
  was verified end-to-end (save path, storage, dedup keying, fire path),
  not newly built.
- **Whole-word matching** — "pay" no longer matches inside "repay".
  Handles multi-word phrases too (e.g. "not interested" is matched as a
  phrase, not two independent words).
- **AND-mode** — each keyword trigger can require ALL its words to
  appear (not just any one), via a new `match_mode` field (`any`/`all`).
- **Free-text event names** — the CAPI Event field is now a text input
  with autocomplete suggestions for Meta's standard events, but accepts
  any custom name.
- **Migration required for existing databases** — `scripts/migrate.py`
  now has three new steps (`v6_01`–`v6_03`) for the login columns and
  `match_mode`. Run it once after pulling this update — see the note at
  the top of the Quick Start section in README.md.

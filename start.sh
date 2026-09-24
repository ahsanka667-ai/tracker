#!/bin/bash
set -e

echo ""
echo "╔══════════════════════════════════════╗"
echo "║       TG Tracker — Local Start       ║"
echo "╚══════════════════════════════════════╝"
echo ""

if [ ! -f ".env" ]; then
  echo "❌ .env not found! Run: cp .env.example .env"
  exit 1
fi

if [ ! -d "venv" ]; then
  echo "⚙️  Creating virtual environment..."
  python3.11 -m venv venv 2>/dev/null || python3 -m venv venv
fi

source venv/bin/activate

if ! python -c "import fastapi" 2>/dev/null; then
  echo "📦 Installing dependencies..."
  pip install -r requirements.txt -q
fi

if command -v docker &> /dev/null; then
  echo "🐳 Starting PostgreSQL and Redis..."
  docker compose up -d postgres redis 2>/dev/null || docker-compose up -d postgres redis
  sleep 5
fi

echo "🗄️  Initializing database..."
python -c "import asyncio; from shared.database import init_db; asyncio.run(init_db())" && echo "   ✅ Database ready" || echo "   ⚠️  Already exists"

mkdir -p sessions

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Starting all services..."
echo ""

# Read BASE_URL from .env
BASE_URL=$(grep "^BASE_URL=" .env | cut -d'=' -f2-)
echo "  Base URL:   $BASE_URL"
echo "  API Docs:   $BASE_URL/internal/docs"
echo "  Dashboard:  $BASE_URL/dashboard/"
echo ""
echo "  Press Ctrl+C to stop."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

cleanup() {
  echo ""
  echo "🛑 Stopping..."
  kill $(jobs -p) 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# Service A — FastAPI (serves dashboard + API). Always starts — this is
# the only service required for the dashboard + username/password login.
uvicorn service_a.main:app --host 0.0.0.0 --port 8000 --reload &
sleep 3

# Service B (Telethon worker) and the Master Bot both talk to Telegram
# and are useless without MASTER_BOT_TOKEN / TELEGRAM_API_ID / TELEGRAM_API_HASH.
# If you haven't set those up yet (e.g. you're just using ADMIN_USERNAME/
# ADMIN_PASSWORD to log in and look around), skip them instead of dumping
# a wall of config-error text — the dashboard runs fine without them.
TOKEN_SET=$(grep -E "^MASTER_BOT_TOKEN=" .env | cut -d'=' -f2-)
API_ID_SET=$(grep -E "^TELEGRAM_API_ID=" .env | cut -d'=' -f2-)

if [ -n "$TOKEN_SET" ] && [ "$TOKEN_SET" != "0" ] && [ -n "$API_ID_SET" ] && [ "$API_ID_SET" != "0" ]; then
  # Service B — Telethon worker (actual Telegram tracking)
  python service_b/worker.py &
  sleep 2
  # Master Bot
  python master_bot/bot.py &
else
  echo "  ⏭️  Skipping worker + master bot — no Telegram credentials in .env yet."
  echo "     (Dashboard still works: log in with ADMIN_USERNAME/ADMIN_PASSWORD.)"
  echo "     Add MASTER_BOT_TOKEN + TELEGRAM_API_ID + TELEGRAM_API_HASH to .env"
  echo "     and re-run this script when you're ready to add Telegram tracking."
  echo ""
fi

wait

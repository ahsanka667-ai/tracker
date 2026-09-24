#!/bin/bash
# start_ngrok.sh — start ngrok tunnel and auto-update .env + restart bot

echo ""
echo "╔══════════════════════════════════════╗"
echo "║        TG Tracker — ngrok Setup      ║"
echo "╚══════════════════════════════════════╝"
echo ""

# Check ngrok installed
if ! command -v ngrok &> /dev/null; then
  echo "📦 Installing ngrok..."
  curl -sSL https://ngrok-agent.s3.amazonaws.com/ngrok.asc | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null
  echo "deb https://ngrok-agent.s3.amazonaws.com buster main" | sudo tee /etc/apt/sources.list.d/ngrok.list >/dev/null
  sudo apt update -q && sudo apt install ngrok -y -q
  echo ""
  echo "⚠️  You need a free ngrok account to get a stable URL."
  echo "   1. Sign up at: https://ngrok.com (free)"
  echo "   2. Copy your authtoken from: https://dashboard.ngrok.com/get-started/your-authtoken"
  echo "   3. Run: ngrok config add-authtoken YOUR_TOKEN"
  echo "   4. Then run this script again."
  exit 1
fi

# Check authtoken configured
if ! ngrok config check &>/dev/null; then
  echo "⚠️  ngrok authtoken not set."
  echo "   Sign up at https://ngrok.com (free)"
  echo "   Then run: ngrok config add-authtoken YOUR_TOKEN"
  exit 1
fi

echo "🚇 Starting ngrok tunnel on port 8000..."

# Start ngrok in background
ngrok http 8000 --log=stdout &> /tmp/ngrok.log &
NGROK_PID=$!

echo "   Waiting for ngrok to connect..."
sleep 4

# Get the public URL from ngrok API
NGROK_URL=$(curl -s http://localhost:4040/api/tunnels | python3 -c "
import sys,json
data=json.load(sys.stdin)
tunnels=data.get('tunnels',[])
for t in tunnels:
    if t.get('proto')=='https':
        print(t['public_url'])
        break
" 2>/dev/null)

if [ -z "$NGROK_URL" ]; then
  echo "❌ Could not get ngrok URL. Check /tmp/ngrok.log"
  kill $NGROK_PID 2>/dev/null
  exit 1
fi

echo ""
echo "✅ ngrok tunnel active!"
echo "   Public URL: $NGROK_URL"
echo ""

# Update BASE_URL in .env
if [ -f ".env" ]; then
  # Replace BASE_URL line
  sed -i "s|^BASE_URL=.*|BASE_URL=$NGROK_URL|g" .env
  echo "✅ Updated .env: BASE_URL=$NGROK_URL"
  echo ""
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Your public URLs:"
echo ""
echo "  Dashboard:   $NGROK_URL/dashboard/"
echo "  API Docs:    $NGROK_URL/internal/docs"
echo "  Health:      $NGROK_URL/health"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Next steps:"
echo "  1. Set this as your Telegram Web App URL in @BotFather:"
echo "     /setmenubutton → select bot → $NGROK_URL/dashboard/"
echo ""
echo "  2. Restart the app to pick up new BASE_URL:"
echo "     bash start.sh"
echo ""
echo "  Press Ctrl+C to stop ngrok."
echo ""

# Keep running
wait $NGROK_PID

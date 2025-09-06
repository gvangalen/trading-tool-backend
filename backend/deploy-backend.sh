#!/bin/bash
set -e

# 📂 Directories
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$BACKEND_DIR/backend/.env"
LOG_DIR="/var/log/pm2"

# ✅ Pad fix
export PATH="$HOME/.local/bin:$PATH"
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

# 🧹 Opschonen
echo "🧹 Verwijder oude __pycache__ mappen..."
find "$BACKEND_DIR" -type d -name '__pycache__' -exec rm -rf {} +

mkdir -p "$LOG_DIR"

# 📥 Pull laatste code
cd "$BACKEND_DIR"
git fetch origin main
git reset --hard origin/main

# 🧪 Check Python deps
pip install --user -r backend/requirements.txt

# 🔁 PM2 restart
pm2 delete backend || true
pm2 delete celery || true
pm2 delete celery-beat || true

# 🌱 Laad .env handmatig (voor pm2 export)
if [ -f "$ENV_FILE" ]; then
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
else
  echo "❌ .env bestand niet gevonden: $ENV_FILE"
  exit 1
fi

# 🚀 Start backend (✅ juiste cmd)
pm2 start "python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 5002 --reload" \
  --name backend \
  --cwd "$BACKEND_DIR" \
  --interpreter python3 \
  --output "$LOG_DIR/backend.log" \
  --error "$LOG_DIR/backend.err.log"

# 🚀 Start celery worker (🔁 FIXED versie)
pm2 start celery \
  --name celery \
  --interpreter python3 \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery.log" \
  --error "$LOG_DIR/celery.err.log" \
  -- \
  -A backend.celery_task.celery_app worker --loglevel=info

# ⏰ Start celery beat (🔁 FIXED versie)
pm2 start celery \
  --name celery-beat \
  --interpreter python3 \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery-beat.log" \
  --error "$LOG_DIR/celery-beat.err.log" \
  -- \
  -A backend.celery_task.celery_app beat --loglevel=info

# 💾 Bewaar PM2 config
pm2 save
pm2 startup | grep sudo && echo "⚠️ Voer bovenstaande 'sudo' commando éénmalig uit voor autostart bij reboot"

# ✅ Check status
echo ""
echo "✅ Alles draait nu:"
pm2 status

echo ""
echo "🌐 Backend:       http://localhost:5002"
echo "📄 Logs backend:  $LOG_DIR/backend.log"
echo "📄 Logs celery:   $LOG_DIR/celery.log"
echo "📄 Logs beat:     $LOG_DIR/celery-beat.log"
echo ""
echo "🧠 AI_MODE check:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

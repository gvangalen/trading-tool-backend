#!/bin/bash
set -e  # 🛑 Stop script bij fouten

# 🧠 Settings
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$BACKEND_DIR/backend/.env"
LOG_DIR="/var/log/pm2"

# ✅ Zet juiste paden voor Celery + Node
export PATH="$HOME/.local/bin:$PATH"
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

# 🧼 Verwijder oude __pycache__ mappen
echo "🧹 Verwijder oude __pycache__ mappen..."
find "$BACKEND_DIR" -type d -name '__pycache__' -exec rm -rf {} +

# 🛠 Maak logmap aan
mkdir -p "$LOG_DIR"

echo "📁 Ga naar projectmap..."
cd "$BACKEND_DIR" || {
  echo "❌ Map niet gevonden: $BACKEND_DIR"
  exit 1
}

echo "📥 Haal laatste code op van main branch..."
git fetch origin main
git reset --hard origin/main || {
  echo "❌ Git reset mislukt."
  exit 1
}

echo "🐍 Installeer Python dependencies..."
pip install --user -r backend/requirements.txt || {
  echo "❌ Installeren van requirements.txt mislukt."
  exit 1
}

echo "💀 Stop oude PM2-processen..."
pm2 delete backend || echo "⚠️ 'backend' niet actief"
pm2 delete celery || echo "⚠️ 'celery' niet actief"
pm2 delete celery-beat || echo "⚠️ 'celery-beat' niet actief"

echo "🌱 Laad .env bestand..."
if [ -f "$ENV_FILE" ]; then
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
else
  echo "❌ .env bestand niet gevonden: $ENV_FILE"
  exit 1
fi

echo "🚀 Start backend (FastAPI via uvicorn)..."
pm2 start uvicorn \
  --name backend \
  --interpreter python3 \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/backend.log" \
  --error "$LOG_DIR/backend.err.log" \
  -- backend.main:app --host 0.0.0.0 --port 5002 --reload

echo "🚀 Start Celery Worker..."
pm2 start celery \
  --name celery \
  --interpreter python3 \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery.log" \
  --error "$LOG_DIR/celery.err.log" \
  -- -A backend.celery_task.celery_app worker --loglevel=info

echo "⏰ Start Celery Beat..."
pm2 start celery \
  --name celery-beat \
  --interpreter python3 \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery-beat.log" \
  --error "$LOG_DIR/celery-beat.err.log" \
  -- -A backend.celery_task.celery_app beat --loglevel=info

echo "💾 PM2 configuratie opslaan (voor reboot)..."
pm2 save
pm2 startup | grep sudo && echo "⚠️ Voer bovenstaande 'sudo' commando éénmalig uit voor autostart bij reboot"

echo ""
echo "✅ Alles draait nu onder PM2:"
pm2 status

echo ""
echo "🌐 Backend URL:      http://localhost:5002"
echo "📄 Logs backend:     $LOG_DIR/backend.log"
echo "📄 Logs celery:      $LOG_DIR/celery.log"
echo "📄 Logs celery-beat: $LOG_DIR/celery-beat.log"
echo ""
echo "🧠 AI_MODE check:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

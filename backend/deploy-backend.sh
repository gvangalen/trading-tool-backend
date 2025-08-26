#!/bin/bash
set -e  # 🛑 Stop script bij fouten

# 🧠 Settings
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$BACKEND_DIR/backend/.env"
LOG_DIR="/var/log/pm2"

# ✅ Activeer NVM en zet Node/PM2 in PATH
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

# 🛠 Maak logmap aan als die nog niet bestaat
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

echo "🚀 Start backend (FastAPI/Uvicorn)..."
pm2 start bash --name backend --cwd backend --output "$LOG_DIR/backend.log" --error "$LOG_DIR/backend.err.log" -- \
  -c "uvicorn main:app --host 0.0.0.0 --port 5002"

echo "🚀 Start Celery Worker..."
pm2 start bash --name celery --cwd backend --output "$LOG_DIR/celery.log" --error "$LOG_DIR/celery.err.log" -- \
  -c "celery -A backend.celery_task.celery_app worker --loglevel=info"

echo "⏰ Start Celery Beat..."
pm2 start bash --name celery-beat --cwd backend --output "$LOG_DIR/celery-beat.log" --error "$LOG_DIR/celery-beat.err.log" -- \
  -c "celery -A backend.celery_task.celery_app beat --loglevel=info"

echo "💾 PM2 configuratie opslaan (voor reboot)..."
pm2 save
pm2 startup | grep sudo && echo "⚠️ Voer bovenstaande 'sudo' commando éénmalig uit voor autostart bij reboot"

echo ""
echo "✅ Productieprocessen draaien:"
pm2 status

echo ""
echo "🌐 Backend:       http://localhost:5002"
echo "📄 Logs backend:  $LOG_DIR/backend.log"
echo "📄 Logs celery:   $LOG_DIR/celery.log"
echo "📄 Logs beat:     $LOG_DIR/celery-beat.log"
echo ""
echo "🧠 AI_MODE check:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

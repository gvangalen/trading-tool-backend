#!/bin/bash
set -e  # 🛑 Stop script bij fouten

# ✅ Activeer NVM en zet Node/PM2 in PATH
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

echo "📁 Ga naar root projectmap..."
cd ~/trading-tool-backend || {
  echo "❌ Map ~/trading-tool-backend niet gevonden."
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
  echo "❌ Installeren dependencies mislukt."
  exit 1
}

echo "💀 Stop oude backend..."
if pm2 list | grep -q backend; then
  pm2 delete backend || echo "⚠️ Kon oude backend niet stoppen (misschien al gestopt)."
else
  echo "ℹ️ Geen bestaand backend-proces actief."
fi

echo "🌱 Laad .env bestand..."
if [ -f backend/.env ]; then
  set -o allexport
  source backend/.env
  set +o allexport
else
  echo "❌ .env bestand niet gevonden in ~/trading-tool-backend/backend"
  exit 1
fi

echo "🚀 Start nieuwe backend met ingeladen variabelen..."
pm2 start "uvicorn main:app --host 0.0.0.0 --port 5002" \
  --interpreter python3 \
  --name backend \
  --cwd ~/trading-tool-backend/backend \
  --env PYTHONPATH=./ \
  --env ENV=production \
  --env DB_HOST="$DB_HOST" \
  --env DB_PORT="$DB_PORT" \
  --env DB_NAME="$DB_NAME" \
  --env DB_USER="$DB_USER" \
  --env DB_PASS="$DB_PASS" \
  --env CELERY_BROKER_URL="$CELERY_BROKER_URL" \
  --env CELERY_RESULT_BACKEND="$CELERY_RESULT_BACKEND" \
  --env OPENAI_API_KEY="$OPENAI_API_KEY" \
  --env AI_MODE="$AI_MODE" \
  --env API_BASE_URL="$API_BASE_URL" \
  --env LOG_LEVEL="$LOG_LEVEL" || {
    echo "❌ Start backend mislukt."
    exit 1
  }

echo "💾 Sla PM2-config op voor herstart..."
pm2 save

echo "🌍 Controleer geladen AI_MODE in PM2:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

echo "✅ Backend draait op http://localhost:5002"

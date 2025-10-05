#!/bin/bash
set -e

# 📂 Directories
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$BACKEND_DIR/backend/.env"
LOG_DIR="/var/log/pm2"

# ✅ PATH fix
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

# 🧪 Dependencies
pip install --user -r backend/requirements.txt

# 🧠 Load .env
if [ -f "$ENV_FILE" ]; then
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
else
  echo "❌ .env bestand niet gevonden: $ENV_FILE"
  exit 1
fi

# ✅ Check keys
if [ -z "$OPENAI_API_KEY" ] || [ -z "$DB_HOST" ]; then
  echo "❌ OPENAI_API_KEY of DB_HOST ontbreekt in .env"
  exit 1
fi

# 🧯 Stop oude processen
pm2 delete backend || true
pm2 delete celery || true
pm2 delete celery-app || true

sleep 2

# 🚀 Start backend met Uvicorn via PM2 (met alle env vars)
echo "🚀 Start backend met Uvicorn via PM2..."
pm2 start uvicorn \
  --name backend \
  --cwd "$BACKEND_DIR" \
  --interpreter python3 \
  --env "OPENAI_API_KEY=$OPENAI_API_KEY" \
  --env "DB_HOST=$DB_HOST" \
  --env "DB_PORT=$DB_PORT" \
  --env "DB_NAME=$DB_NAME" \
  --env "DB_USER=$DB_USER" \
  --env "DB_PASS=$DB_PASS" \
  --env "ALPHA_VANTAGE_API_KEY=$ALPHA_VANTAGE_API_KEY" \
  --env "API_BASE_URL=$API_BASE_URL" \
  --env "CELERY_BROKER_URL=$CELERY_BROKER_URL" \
  --env "CELERY_RESULT_BACKEND=$CELERY_RESULT_BACKEND" \
  --env "AI_MODE=$AI_MODE" \
  --env "COINGECKO_URL=$COINGECKO_URL" \
  --env "VOLUME_URL=$VOLUME_URL" \
  --env "ASSETS_JSON=$ASSETS_JSON" \
  --output "$LOG_DIR/backend.log" \
  --error "$LOG_DIR/backend.err.log" \
  -- \
  backend.main:app --host 0.0.0.0 --port 5002

# 🚀 Start Celery Worker
echo "🚀 Start Celery Worker..."
pm2 start "$(which celery)" \
  --name celery \
  --interpreter none \
  --cwd "$BACKEND_DIR" \
  --env "OPENAI_API_KEY=$OPENAI_API_KEY" \
  --env "DB_HOST=$DB_HOST" \
  --env "DB_PORT=$DB_PORT" \
  --env "DB_NAME=$DB_NAME" \
  --env "DB_USER=$DB_USER" \
  --env "DB_PASS=$DB_PASS" \
  --env "ALPHA_VANTAGE_API_KEY=$ALPHA_VANTAGE_API_KEY" \
  --env "API_BASE_URL=$API_BASE_URL" \
  --env "CELERY_BROKER_URL=$CELERY_BROKER_URL" \
  --env "CELERY_RESULT_BACKEND=$CELERY_RESULT_BACKEND" \
  --env "AI_MODE=$AI_MODE" \
  --env "COINGECKO_URL=$COINGECKO_URL" \
  --env "VOLUME_URL=$VOLUME_URL" \
  --env "ASSETS_JSON=$ASSETS_JSON" \
  --output "$LOG_DIR/celery.log" \
  --error "$LOG_DIR/celery.err.log" \
  -- \
  -A backend.celery_task.celery_app worker --loglevel=info

# ⏰ Start Celery Beat
echo "⏰ Start Celery Beat (celery-app)..."
pm2 start "$(which celery)" \
  --name celery-app \
  --interpreter none \
  --cwd "$BACKEND_DIR" \
  --env "OPENAI_API_KEY=$OPENAI_API_KEY" \
  --env "DB_HOST=$DB_HOST" \
  --env "DB_PORT=$DB_PORT" \
  --env "DB_NAME=$DB_NAME" \
  --env "DB_USER=$DB_USER" \
  --env "DB_PASS=$DB_PASS" \
  --env "ALPHA_VANTAGE_API_KEY=$ALPHA_VANTAGE_API_KEY" \
  --env "API_BASE_URL=$API_BASE_URL" \
  --env "CELERY_BROKER_URL=$CELERY_BROKER_URL" \
  --env "CELERY_RESULT_BACKEND=$CELERY_RESULT_BACKEND" \
  --env "AI_MODE=$AI_MODE" \
  --env "COINGECKO_URL=$COINGECKO_URL" \
  --env "VOLUME_URL=$VOLUME_URL" \
  --env "ASSETS_JSON=$ASSETS_JSON" \
  --output "$LOG_DIR/celery-app.log" \
  --error "$LOG_DIR/celery-app.err.log" \
  -- \
  -A backend.celery_task.celery_app beat --loglevel=info

# 💾 PM2 config opslaan
pm2 save
pm2 startup | grep sudo && echo "⚠️ Voer bovenstaande 'sudo' commando éénmalig uit voor autostart bij reboot"

# ✅ Statusoverzicht
echo ""
echo "✅ Alles draait nu:"
pm2 status

echo ""
echo "🌐 Backend:       http://localhost:5002"
echo "📄 Logs backend:  $LOG_DIR/backend.log"
echo "📄 Logs celery:   $LOG_DIR/celery.log"
echo "📄 Logs beat:     $LOG_DIR/celery-app.log"
echo ""

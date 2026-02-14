#!/bin/bash
set -e

echo "🚀 Starting backend deploy..."

# =====================================================
# PATH FIX (Node / Python / NVM)
# =====================================================
export PATH="$HOME/.local/bin:$PATH"
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

# =====================================================
# DIRECTORIES
# =====================================================
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$BACKEND_DIR/.env"
LOG_DIR="/var/log/pm2"

mkdir -p "$LOG_DIR"

# =====================================================
# VERIFY ENV
# =====================================================
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ .env niet gevonden: $ENV_FILE"
  exit 1
fi

echo "✅ .env gevonden"

# =====================================================
# CLEAN CACHE
# =====================================================
echo "🧹 Cleaning __pycache__..."
find "$BACKEND_DIR" -type d -name '__pycache__' -exec rm -rf {} +

# =====================================================
# PULL LATEST CODE
# =====================================================
echo "📥 Updating code..."
cd "$BACKEND_DIR"
git fetch origin main
git reset --hard origin/main

# =====================================================
# INSTALL DEPENDENCIES
# =====================================================
echo "📦 Installing Python dependencies..."
pip install --user -r backend/requirements.txt

# =====================================================
# STOP PM2 COMPLETELY (CRITICAL)
# =====================================================
echo "🛑 Stopping old processes & clearing PM2 cache..."
pm2 delete all || true
pm2 kill || true
rm -f ~/.pm2/dump.pm2 || true

sleep 2

# =====================================================
# LOAD ENV INTO CURRENT SHELL
# =====================================================
echo "🔐 Loading environment variables..."
set -o allexport
source "$ENV_FILE"
set +o allexport

# sanity checks
if [ -z "$OPENAI_API_KEY" ]; then
  echo "❌ OPENAI_API_KEY ontbreekt"
  exit 1
fi

if [ -z "$FRONTEND_URL" ]; then
  echo "❌ FRONTEND_URL ontbreekt"
  exit 1
fi

echo "✅ Environment loaded"
echo "➡ FRONTEND_URL=$FRONTEND_URL"

# =====================================================
# START FASTAPI BACKEND
# =====================================================
echo "🚀 Starting FastAPI backend..."

pm2 start uvicorn \
  --name backend \
  --cwd "$BACKEND_DIR" \
  --interpreter python3 \
  --output "$LOG_DIR/backend.log" \
  --error "$LOG_DIR/backend.err.log" \
  -- \
  backend.main:app --host 0.0.0.0 --port 5002

# =====================================================
# START CELERY WORKER
# =====================================================
echo "🚀 Starting Celery worker..."

pm2 start "$(which celery)" \
  --name celery \
  --interpreter none \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery.log" \
  --error "$LOG_DIR/celery.err.log" \
  -- \
  -A backend.celery_task.celery_app worker --loglevel=info

# =====================================================
# START CELERY BEAT
# =====================================================
echo "⏰ Starting Celery Beat..."

pm2 start "$(which celery)" \
  --name celery-beat \
  --interpreter none \
  --cwd "$BACKEND_DIR" \
  --output "$LOG_DIR/celery-beat.log" \
  --error "$LOG_DIR/celery-beat.err.log" \
  -- \
  -A backend.celery_task.celery_app beat --loglevel=info

# =====================================================
# SAVE PM2 STATE
# =====================================================
pm2 save

echo ""
echo "✅ DEPLOY SUCCESSFUL"
echo "-----------------------------------"
pm2 status
echo ""
echo "🌐 Backend: http://localhost:5002"
echo "📄 Logs backend: $LOG_DIR/backend.log"
echo "📄 Logs celery:  $LOG_DIR/celery.log"
echo "📄 Logs beat:    $LOG_DIR/celery-beat.log"
echo ""

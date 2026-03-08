#!/bin/bash
set -e

echo "🚀 Starting backend deploy..."

# =====================================================
# PATH FIX
# =====================================================
export PATH="$HOME/.local/bin:$PATH"
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

# =====================================================
# DIRECTORIES
# =====================================================
BACKEND_DIR="$HOME/trading-tool-backend"
ENV_FILE="$HOME/.secrets/trading.env"
LOG_DIR="/var/log/pm2"

mkdir -p "$LOG_DIR"

# =====================================================
# VERIFY ENV
# =====================================================
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ ENV FILE NOT FOUND: $ENV_FILE"
  exit 1
fi

echo "✅ Using ENV file:"
echo "➡ $ENV_FILE"

# =====================================================
# CLEAN CACHE
# =====================================================
echo "🧹 Cleaning __pycache__..."
find "$BACKEND_DIR" -type d -name '__pycache__' -exec rm -rf {} +

# =====================================================
# UPDATE CODE
# =====================================================
echo "📥 Updating code..."
cd "$BACKEND_DIR"
git fetch origin main
git reset --hard origin/main

# =====================================================
# INSTALL DEPENDENCIES
# =====================================================
echo "📦 Installing Python dependencies..."
pip install -r backend/requirements.txt

# =====================================================
# LOAD ENV
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
# RESTART BACKEND SERVICES ONLY
# =====================================================
echo "♻️ Restarting backend services..."

pm2 delete backend || true
pm2 delete celery || true
pm2 delete celery-beat || true

sleep 2

# =====================================================
# START FASTAPI
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

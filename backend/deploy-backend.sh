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

echo "💀 Stop oude PM2-processen..."
pm2 delete backend || echo "⚠️ Backend niet actief"
pm2 delete celery || echo "⚠️ Celery worker niet actief"
pm2 delete celery-beat || echo "⚠️ Celery beat niet actief"

echo "🌱 Laad .env bestand..."
if [ -f backend/.env ]; then
  set -o allexport
  source backend/.env
  set +o allexport
else
  echo "❌ .env bestand niet gevonden in ~/trading-tool-backend/backend"
  exit 1
fi

echo "🚀 Start nieuwe backend..."
pm2 start "uvicorn main:app --host 0.0.0.0 --port 5002" \
  --interpreter python3 \
  --name backend \
  --cwd backend \
  --env-file backend/.env || {
    echo "❌ Start backend mislukt."
    exit 1
  }

echo "🚀 Start Celery worker via PM2 (script)..."
pm2 start "start_celery_worker.sh" \
  --interpreter bash \
  --name celery \
  --cwd backend || {
    echo "❌ Start celery worker mislukt."
    exit 1
  }

echo "⏰ Start Celery Beat via PM2 (script)..."
pm2 start "start_celery_beat.sh" \
  --interpreter bash \
  --name celery-beat \
  --cwd backend || {
    echo "❌ Start celery beat mislukt."
    exit 1
  }

echo "💾 Sla PM2-config op voor herstart..."
pm2 save

echo "🌍 Controleer geladen AI_MODE in PM2:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

echo "✅ Backend draait op http://localhost:5002"
echo "✅ Celery worker actief als 'celery'"
echo "✅ Celery beat actief als 'celery-beat'"

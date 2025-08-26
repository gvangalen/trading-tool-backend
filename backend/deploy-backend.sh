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
  echo "❌ Installeren van requirements.txt mislukt."
  exit 1
}

echo "💀 Stop oude PM2-processen..."
pm2 delete backend || echo "⚠️ Process 'backend' niet actief"
pm2 delete celery || echo "⚠️ Process 'celery' niet actief"
pm2 delete celery-beat || echo "⚠️ Process 'celery-beat' niet actief"

echo "🌱 Laad .env bestand..."
if [ -f backend/.env ]; then
  set -o allexport
  source backend/.env
  set +o allexport
else
  echo "❌ .env bestand niet gevonden in ~/trading-tool-backend/backend"
  exit 1
fi

echo "🚀 Start nieuwe backend (FastAPI via Uvicorn)..."
pm2 start "uvicorn main:app --host 0.0.0.0 --port 5002" \
  --interpreter python3 \
  --name backend \
  --cwd backend \
  --env-file backend/.env || {
    echo "❌ Start backend mislukt."
    exit 1
  }

echo "🚀 Start Celery Worker via PM2 (script)..."
pm2 start "start_celery_worker.sh" \
  --interpreter bash \
  --name celery \
  --cwd backend || {
    echo "❌ Start Celery worker mislukt."
    exit 1
  }

echo "⏰ Start Celery Beat via PM2 (script)..."
pm2 start "start_celery_beat.sh" \
  --interpreter bash \
  --name celery-beat \
  --cwd backend || {
    echo "❌ Start Celery Beat mislukt."
    exit 1
  }

echo "💾 Sla PM2-processen op (voor reboot/herstart)..."
pm2 save

echo ""
echo "✅ Alles draait! Statusoverzicht:"
echo "🌐 Backend: http://localhost:5002"
echo "⚙️  Celery worker: pm2 logs celery"
echo "⏰ Celery beat:   pm2 logs celery-beat"
echo ""
echo "🧠 Controleer of AI_MODE is geladen:"
pm2 show backend | grep AI_MODE || echo "⚠️ AI_MODE niet gevonden in PM2 env."

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

echo "🌱 Laad .env met DB-gegevens..."
set -o allexport
source .env
set +o allexport

echo "🚀 Start nieuwe backend..."
pm2 start "uvicorn start_backend:app --host 0.0.0.0 --port 5002" \
  --interpreter python3 \
  --name backend \
  --cwd ~/trading-tool-backend/backend \
  --env PYTHONPATH=./ \
  --env ENV=production || {
    echo "❌ Start backend mislukt."
    exit 1
  }

echo "💾 Sla PM2-config op voor herstart..."
pm2 save

echo "✅ Backend draait op http://localhost:5002"

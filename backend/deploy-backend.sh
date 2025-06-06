#!/bin/bash
set -e  # 🛑 Stop bij fouten

# ✅ Activeer NVM en PM2 pad
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/v18.20.8/bin:$PATH"

echo "📁 Ga naar backend map..."
cd ~/trading-tool-backend/backend || {
  echo "❌ Map ~/trading-tool-backend/backend niet gevonden."
  exit 1
}

echo "📥 Haal laatste code op..."
git fetch origin main
git reset --hard origin/main || {
  echo "❌ Git reset mislukt."
  exit 1
}

echo "🐍 Installeer Python dependencies..."
pip install --user -r requirements.txt || {
  echo "❌ Installeren dependencies mislukt."
  exit 1
}

echo "💀 Stop oude backend (indien actief)..."
pm2 delete backend || echo "ℹ️ Geen bestaand backend-proces actief"

echo "🚀 Start backend opnieuw via Uvicorn (ASGI)..."
pm2 start "uvicorn backend.main:app --host 0.0.0.0 --port 5002" --interpreter python3 --name backend || {
  echo "❌ Start backend mislukt."
  exit 1
}

echo "💾 Sla PM2-config op (voor herstart na reboot)..."
pm2 save

echo "✅ Backend succesvol gedeployed op http://localhost:5002"

#!/bin/bash
set -e  # 🛑 Stop script bij fouten

# ✅ Activeer NVM en zorg dat pm2 in PATH zit
export NVM_DIR="$HOME/.nvm"
source "$NVM_DIR/nvm.sh"
export PATH="$NVM_DIR/versions/node/$(nvm current)/bin:$PATH"

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

echo "🐍 Installeer Python dependencies (user)..."
pip install --user -r requirements.txt || {
  echo "❌ Installeren dependencies mislukt."
  exit 1
}

echo "💀 Stop oude backend (indien actief)..."
if pm2 list | grep -q backend; then
  pm2 delete backend || echo "⚠️ Kon oude backend niet verwijderen (misschien al gestopt)."
else
  echo "ℹ️ Geen bestaand backend-proces actief."
fi

echo "🚀 Start backend opnieuw via Uvicorn (ASGI)..."
pm2 start "uvicorn start_backend:app --host 0.0.0.0 --port 5002" \
  --interpreter python3 \
  --name backend || {
    echo "❌ Start backend mislukt."
    exit 1
  }

echo "💾 Sla PM2-config op (voor reboot persistentie)..."
pm2 save

echo "✅ Backend succesvol gedeployed op http://localhost:5002"

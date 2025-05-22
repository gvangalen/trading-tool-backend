#!/bin/bash

set -e  # 🛑 Stop script bij fouten

echo "📁 Ga naar backend map..."
cd ~/trading-tool-backend || {
  echo "❌ Map ~/trading-tool-backend niet gevonden."
  exit 1
}

echo "📥 Haal laatste code op..."
git fetch origin main
git reset --hard origin/main || {
  echo "❌ Git reset mislukt."
  exit 1
}

echo "🐍 Installeer dependencies..."
pip install -r requirements.txt || {
  echo "❌ Installeren dependencies mislukt."
  exit 1
}

echo "🔁 Herstart backend met PM2..."
pm2 delete backend || echo "ℹ️ Backend draaide nog niet"

pm2 start "uvicorn main:app --host 0.0.0.0 --port 8000 --reload" --name backend || {
  echo "❌ Start backend mislukt."
  exit 1
}

echo "✅ Backend succesvol gedeployed!"

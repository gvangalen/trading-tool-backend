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

echo "💀 Stop oude backend (indien actief)..."
pm2 delete backend || echo "ℹ️ Geen bestaand backend-proces"

echo "🚀 Start backend op via Uvicorn (ASGI)..."
pm2 start "uvicorn backend.main:app --host 0.0.0.0 --port 5002 --reload" --name backend || {
  echo "❌ Start backend mislukt."
  exit 1
}

echo "💾 PM2-config bewaren..."
pm2 save

echo "✅ Backend succesvol gedeployed op poort 5002!"

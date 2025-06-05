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

echo "🐍 Installeer Python dependencies..."
pip install -r requirements.txt || {
  echo "❌ Installeren dependencies mislukt."
  exit 1
}

echo "💀 Stop oude backend (indien actief)..."
pm2 delete backend || echo "ℹ️ Geen bestaand backend-proces"

echo "🚀 Start backend met Uvicorn via PM2..."
pm2 start uvicorn \
  --name backend \
  --interpreter python3 \
  -- "backend.main:app" \
  --host 0.0.0.0 \
  --port 5002 \
  --workers 1 || {
  echo "❌ Start backend mislukt."
  exit 1
}

echo "💾 PM2-config bewaren..."
pm2 save

echo "✅ Backend succesvol gedeployed op http://localhost:5002/api/health"

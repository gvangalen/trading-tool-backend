#!/bin/bash

echo "📦 Ga naar backend map..."
cd ~/trading-tool-backend/backend || {
  echo "❌ Backend map niet gevonden."; exit 1;
}

echo "📥 Haal laatste code op..."
git fetch origin
git reset --hard origin/main

echo "🐍 Installeer dependencies..."
pip install -r requirements.txt

echo "🔁 Herstart backend met PM2..."
pm2 delete backend || echo "ℹ️ Backend draaide nog niet"
pm2 start "uvicorn main:app --host 0.0.0.0 --port 8000" --name backend

echo "✅ Backend succesvol gedeployed!"

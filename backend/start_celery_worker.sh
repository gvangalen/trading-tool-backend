#!/bin/bash

# 📍 Ga naar de projectmap
cd ~/trading-tool-backend || {
  echo "❌ Map niet gevonden: ~/trading-tool-backend"
  exit 1
}

# 📦 Laad de omgevingsvariabelen uit .env
if [ -f backend/.env ]; then
  echo "📦 .env gevonden, laden..."
  set -o allexport
  source backend/.env
  set +o allexport
else
  echo "❌ .env bestand niet gevonden op: backend/.env"
  exit 1
fi

# 🚀 Start de Celery Worker
echo "🚀 Start Celery worker..."
exec python3 -m celery -A backend.celery_task.celery_app worker --loglevel=info

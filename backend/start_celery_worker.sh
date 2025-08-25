#!/bin/bash

# ⬇️ Ga naar de juiste map
cd ~/trading-tool-backend || {
  echo "❌ Map niet gevonden: ~/trading-tool-backend"
  exit 1
}

# ⬇️ Laad de omgeving
if [ -f backend/.env ]; then
  set -o allexport
  source backend/.env
  set +o allexport
else
  echo "❌ .env bestand niet gevonden."
  exit 1
fi

# 🚀 Start Celery worker
echo "🚀 Start Celery worker..."
python3 -m celery -A backend.celery_task.celery_app worker --loglevel=info

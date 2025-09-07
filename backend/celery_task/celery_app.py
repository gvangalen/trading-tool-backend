import os
import sys
from celery import Celery
from celery.schedules import crontab

# ✅ Voeg rootpad van het project toe voor correcte imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ✅ Celery app initialiseren
celery = Celery(
    "celery_app",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
)

# ✅ Configuratie
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Periodieke taken
celery.conf.beat_schedule = {
    "fetch-macro-data": {
        "task": "backend.celery_task.macro_task.fetch_macro_data",
        "schedule": crontab(minute=0, hour="*/6"),  # Elke 6 uur
    },
    "fetch-technical-data": {
        "task": "backend.celery_task.technical_task.fetch_technical_data",
        "schedule": crontab(minute=10, hour="*/6"),
    },
    "fetch-market-data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute=20, hour="*/6"),
    },
    "validate-setups": {
        "task": "backend.celery_task.setup_task.validate_setups_task",
        "schedule": crontab(minute=30, hour="*/12"),
    },
    "generate-strategies": {
        "task": "backend.celery_task.strategy_task.generate_all",
        "schedule": crontab(minute=45, hour="*/12"),
    },
}

# ✅ Automatisch alle taken uit submodules inladen
celery.autodiscover_tasks([
    "backend.celery_task.market_task",
    "backend.celery_task.macro_task",
    "backend.celery_task.technical_task",
    "backend.celery_task.setup_task",
    "backend.celery_task.strategy_task",
    "backend.celery_task.daily_report_task",
    "backend.celery_task.weekly_report_task",
    "backend.celery_task.monthly_report_task",
    "backend.celery_task.quarterly_report_task",
    "backend.celery_task.btc_price_history_task",
    "backend.ai_tasks.trading_advice_task",
    "backend.ai_tasks.validation_task"
])

# ✅ Export voor celery CLI
app = celery

from celery import Celery
from celery.schedules import crontab
import os
import logging

# ✅ Logging instellen voor debug en foutopsporing
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Controleer omgevingsvariabelen en gebruik een fallback
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0")

# ✅ Celery-app configureren
celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
)

# ✅ Algemene Celery-configuratie
celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# ✅ Celery Beat Schedule instellen (periodieke taken)
celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "celery_worker.fetch_market_data",
        "schedule": crontab(minute="*/5"),  # Elke 5 minuten
    },
    "fetch_macro_data": {
        "task": "celery_worker.fetch_macro_data",
        "schedule": crontab(minute="*/10"),  # Elke 10 minuten
    },
    "generate_daily_report": {
        "task": "generate_daily_report",
        "schedule": crontab(hour=8, minute=0),  # Elke dag om 08:00 UTC
    },
    "validate_setups_task": {
        "task": "celery_worker.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),  # Elke 6 uur
    },
    "generate_trading_advice_task": {
        "task": "celery_worker.generate_trading_advice_task",
        "schedule": crontab(minute=5, hour="*/6"),  # Elke 6 uur, 5 min na setup validatie
    },
    "generate_ai_strategies_task": {
        "task": "celery_worker.generate_strategieën_automatisch",
        "schedule": crontab(hour=8, minute=10),  # Elke dag om 08:10 UTC
    },
}

# ✅ Taken expliciet importeren (Voorkomt importfouten!)
try:
    import celery_worker
    import ai_daily_report_generator  # Zorgt dat task 'generate_daily_report' zichtbaar is
    logging.info("✅ Celery taken correct ingeladen!")
except ImportError as e:
    logging.error(f"❌ Fout bij importeren Celery taken: {e}")

# ✅ Opstartlog tonen
logging.info(f"✅ Celery Beat is gestart met broker: {CELERY_BROKER}")

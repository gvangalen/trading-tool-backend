from celery import Celery
from celery.schedules import crontab
import os
import logging
import traceback

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Omgevingsvariabelen met fallback
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0")

# ✅ Celery-app aanmaken
celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
)

# ✅ Algemene configuratie
celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# ✅ Periodieke taken instellen via Celery Beat
celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "celery_worker.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    "fetch_macro_data": {
        "task": "celery_worker.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "generate_daily_report": {
        "task": "ai_daily_report_generator.generate_daily_report",
        "schedule": crontab(hour=8, minute=0),
    },
    "validate_setups_task": {
        "task": "celery_worker.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice_task": {
        "task": "celery_worker.generate_trading_advice_task",
        "schedule": crontab(minute=5, hour="*/6"),
    },
    "generate_ai_strategies_task": {
        "task": "celery_worker.generate_strategieën_automatisch",
        "schedule": crontab(hour=8, minute=10),
    },
}

# ✅ Taken importeren om te registreren bij Celery
try:
    import celery_worker
    import ai_daily_report_generator
    logging.info("✅ Celery taken correct geïmporteerd.")
except ImportError as e:
    logging.error("❌ ImportError bij laden van Celery taken:")
    logging.error(traceback.format_exc())

# ✅ Startlog
logging.info(f"✅ Celery Beat is gestart met broker: {CELERY_BROKER}")

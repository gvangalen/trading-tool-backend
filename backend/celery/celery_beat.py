from celery import Celery
from celery.schedules import crontab
import os
import logging
import traceback

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Omgevingsvariabelen met fallback
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://market_dashboard-redis:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://market_dashboard-redis:6379/0")

# ✅ Celery-app aanmaken
celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=[
        "market_tasks",
        "macro_tasks",
        "technical_tasks",
        "ai_tasks",
        "strategy_tasks",
        "report_tasks"
    ]
)

# ✅ Algemene configuratie
celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# ✅ Periodieke taken via Celery Beat
celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "market.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    "fetch_macro_data": {
        "task": "macro.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "fetch_technical_data": {
        "task": "technical.fetch_technical_data",
        "schedule": crontab(minute="*/10"),
    },
    "validate_setups_task": {
        "task": "ai_tasks.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice": {
        "task": "ai_tasks.generate_trading_advice",
        "schedule": crontab(minute=5, hour="*/6"),
    },
    "generate_ai_strategieën": {
        "task": "strategy_tasks.generate_strategieën_automatisch",
        "schedule": crontab(hour=8, minute=10),
    },
    "generate_daily_report_pdf": {
        "task": "report_tasks.generate_daily_report_pdf",
        "schedule": crontab(hour=8, minute=15),
    },
}

# ✅ Taken importeren (optioneel als je `include=[...]` al gebruikt)
try:
    import market_tasks
    import macro_tasks
    import technical_tasks
    import ai_tasks
    import strategy_tasks
    import report_tasks
    logger.info("✅ Celery taken correct geïmporteerd.")
except ImportError:
    logger.error("❌ Fout bij importeren van Celery taken:")
    logger.error(traceback.format_exc())

# ✅ Startlog
logger.info(f"🚀 Celery Beat actief met broker: {CELERY_BROKER}")

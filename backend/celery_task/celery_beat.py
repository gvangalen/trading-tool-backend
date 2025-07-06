from celery import Celery
from celery.schedules import crontab
import os
import logging
import traceback

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=[
        "celery_task.market_task",
        "celery_task.macro_task",
        "celery_task.technical_task",
        "celery_task.setup_task",
        "celery_task.strategie_task",
        "celery_task.daily_report_task",
        "ai_tasks.trading_advice_task",
        "ai_tasks.validation_task",
    ]
)

celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    "fetch_macro_data": {
        "task": "celery_task.macro_task.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "fetch_technical_data": {
        "task": "celery_task.technical_task.fetch_technical_data",
        "schedule": crontab(minute="*/10"),
    },
    "validate_setups_task": {
        "task": "ai_tasks.validation_task.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice": {
        "task": "ai_tasks.trading_advice_task.generate_trading_advice",
        "schedule": crontab(minute=5, hour="*/6"),
    },
    "generate_ai_strategieën": {
        "task": "celery_task.strategie_task.generate_strategieën_automatisch",
        "schedule": crontab(hour=8, minute=10),
    },
    "generate_daily_report_pdf": {
        "task": "celery_task.daily_report_task.generate_daily_report_pdf",
        "schedule": crontab(hour=8, minute=15),
    },
}

# ✅ Taken importeren (voor logging en debugging)
try:
    import celery_task.market_task
    import celery_task.macro_task
    import celery_task.technical_task
    import celery_task.setup_task
    import celery_task.strategie_task
    import celery_task.daily_report_task
    import ai_tasks.trading_advice_task
    import ai_tasks.validation_task
    logger.info("✅ Celery taken correct geïmporteerd.")
except ImportError:
    logger.error("❌ Fout bij importeren van Celery taken:")
    logger.error(traceback.format_exc())

logger.info(f"🚀 Celery Beat actief met broker: {CELERY_BROKER}")

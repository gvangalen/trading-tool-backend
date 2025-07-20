import os
import sys
import logging
import traceback
from celery import Celery
from celery.schedules import crontab

# ✅ Zorg dat backend.* import werkt (ook vanuit root)
sys.path.insert(0, os.path.abspath("."))

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Omgevingsvariabelen of fallback naar localhost Redis
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# ✅ Celery App initialiseren
celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=[
        "backend.celery_task.market_task",
        "backend.celery_task.macro_task",
        "backend.celery_task.technical_task",
        "backend.celery_task.setup_task",
        "backend.celery_task.strategy_task",  # ✅ GEFIXT
        "backend.celery_task.daily_report_task",
        "backend.ai_tasks.trading_advice_task",
        "backend.ai_tasks.validation_task",
    ]
)

# ✅ Configuratie
celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# ✅ Beat scheduler met geplande taken
celery.conf.beat_schedule = {
    "fetch_market_data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    "fetch_macro_data": {
        "task": "backend.celery_task.macro_task.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "fetch_technical_data": {
        "task": "backend.celery_task.technical_task.fetch_technical_data",
        "schedule": crontab(minute="*/10"),
    },
    "validate_setups_task": {
        "task": "backend.ai_tasks.validation_task.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice": {
        "task": "backend.ai_tasks.trading_advice_task.generate_trading_advice",
        "schedule": crontab(minute=5, hour="*/6"),
    },
    "generate_ai_strategieën": {
        "task": "backend.celery_task.strategy_task.generate_strategieën_automatisch",  # ✅ GEFIXT
        "schedule": crontab(hour=8, minute=10),
    },
    "generate_daily_report_pdf": {
        "task": "backend.celery_task.daily_report_task.generate_daily_report_pdf",
        "schedule": crontab(hour=8, minute=15),
    },
}

# ✅ Taken expliciet importeren (voor debug/logging)
try:
    import backend.celery_task.market_task
    import backend.celery_task.macro_task
    import backend.celery_task.technical_task
    import backend.celery_task.setup_task
    import backend.celery_task.strategy_task  # ✅ GEFIXT
    import backend.celery_task.daily_report_task
    import backend.ai_tasks.trading_advice_task
    import backend.ai_tasks.validation_task
    logger.info("✅ Alle Celery taken succesvol geïmporteerd.")
except ImportError:
    logger.error("❌ Fout bij importeren van Celery taken:")
    logger.error(traceback.format_exc())

# ✅ Laatste statusmelding
logger.info(f"🚀 Celery en Beat draaien met broker: {CELERY_BROKER}")

# ✅ Nodig voor 'celery -A backend.celery_app worker'
app = celery

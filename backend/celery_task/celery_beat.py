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

# ✅ Redis-configuratie ophalen
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# ✅ Celery-app initialiseren
celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=[
        "backend.celery_task.market_task",
        "backend.celery_task.macro_task",
        "backend.celery_task.technical_task",
        "backend.celery_task.setup_task",
        "backend.celery_task.strategy_task",
        "backend.celery_task.daily_report_task",
        "backend.celery_task.weekly_report_task",
        "backend.celery_task.monthly_report_task",
        "backend.celery_task.quarterly_report_task",
        "backend.celery_task.btc_price_history_task",  # ✅ BTC-prijs
        "backend.ai_tasks.trading_advice_task",
        "backend.ai_tasks.validation_task",
    ]
)

# ✅ Algemene configuratie
celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# ✅ Beat scheduler: taken inplannen
celery.conf.beat_schedule = {
    # 📈 Live BTC/crypto prijsdata
    "fetch_market_data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/5"),
    },
    # 📊 Macro & Technische indicators
    "fetch_macro_data": {
        "task": "backend.celery_task.macro_task.fetch_macro_data",
        "schedule": crontab(minute="*/10"),
    },
    "fetch_technical_data": {
        "task": "backend.celery_task.technical_task.fetch_technical_data",
        "schedule": crontab(minute="*/10"),
    },
    # ✅ BTC prijs (dagelijks historisch)
    "fetch_btc_daily_price": {
        "task": "backend.celery_task.btc_price_history_task.fetch_btc_history_daily",
        "schedule": crontab(hour=1, minute=10),
    },
    # 📚 Historiek en returns
    "save_market_data_7d": {
        "task": "backend.celery_task.market_task.save_market_data_7d",
        "schedule": crontab(hour=1, minute=30),
    },
    "save_forward_returns": {
        "task": "backend.celery_task.market_task.save_forward_returns",
        "schedule": crontab(hour=2, minute=0),
    },
    # 🤖 AI-validatie en advies
    "validate_setups_task": {
        "task": "backend.ai_tasks.validation_task.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice": {
        "task": "backend.ai_tasks.trading_advice_task.generate_trading_advice",
        "schedule": crontab(minute=5, hour="*/6"),
    },
    # 🧠 Strategieën & rapporten
    "generate_ai_strategieën": {
        "task": "backend.celery_task.strategy_task.generate_strategieën_automatisch",
        "schedule": crontab(hour=8, minute=10),
    },
    "generate_daily_report_pdf": {
        "task": "backend.celery_task.daily_report_task.generate_daily_report_pdf",
        "schedule": crontab(hour=8, minute=15),
    },
    "generate_weekly_report": {
        "task": "backend.celery_task.weekly_report_task.generate_weekly_report",
        "schedule": crontab(hour=8, minute=20, day_of_week="monday"),
    },
    "generate_monthly_report": {
        "task": "backend.celery_task.monthly_report_task.generate_monthly_report",
        "schedule": crontab(hour=8, minute=30, day_of_month="1"),
    },
    "generate_quarterly_report": {
        "task": "backend.celery_task.quarterly_report_task.generate_quarterly_report",
        "schedule": crontab(hour=8, minute=45, day_of_month="1", month_of_year="1,4,7,10"),
    },
}

# ✅ Expliciete imports voor log/debug
try:
    import backend.celery_task.market_task
    import backend.celery_task.macro_task
    import backend.celery_task.technical_task
    import backend.celery_task.setup_task
    import backend.celery_task.strategy_task
    import backend.celery_task.daily_report_task
    import backend.celery_task.weekly_report_task
    import backend.celery_task.monthly_report_task
    import backend.celery_task.quarterly_report_task
    import backend.celery_task.btc_price_history_task
    import backend.ai_tasks.trading_advice_task
    import backend.ai_tasks.validation_task
    logger.info("✅ Alle Celery taken succesvol geïmporteerd.")
except ImportError:
    logger.error("❌ Fout bij importeren van Celery taken:")
    logger.error(traceback.format_exc())

# ✅ Laatste melding
logger.info(f"🚀 Celery en Beat draaien met broker: {CELERY_BROKER}")

# ✅ Nodig voor PM2 of CLI
app = celery

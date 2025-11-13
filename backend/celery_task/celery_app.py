import os
import sys
import logging
import traceback
from dotenv import load_dotenv
from celery import Celery
from celery.schedules import crontab

# =========================================================
# ⚙️ .env en sys.path setup
# =========================================================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# =========================================================
# 🪵 Logging
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

logger.info(f"🔍 ASSETS_JSON uit .env: {os.getenv('ASSETS_JSON')}")

# =========================================================
# 🧠 Celery Config
# =========================================================
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=[
        # 🔹 Standaard backend taken
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
        "backend.celery_task.store_daily_scores_task",

        # 🔹 AI-taken
        "backend.ai_tasks.trading_advice_task",
        "backend.ai_tasks.validation_task",

        # 🔹 Nieuwe AI agents
        "backend.ai_agents.macro_ai_agent",
        "backend.ai_agents.market_ai_agent",
        "backend.ai_agents.technical_ai_agent",
        "backend.ai_agents.score_ai_agent",
        "backend.ai_agents.strategy_ai_agent",    # ✅ TOEGEVOEGD
        "backend.ai_agents.report_ai_agent",      # ❗ Ook nodig voor daily report PDFs
    ],
)

celery.conf.enable_utc = True
celery.conf.timezone = "UTC"

# =========================================================
# 🕒 Beat Schedule
# =========================================================
celery.conf.beat_schedule = {

    # 📊 MARKT
    "fetch_market_data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/15"),
    },
    "save_market_data_daily": {
        "task": "backend.celery_task.market_task.save_market_data_daily",
        "schedule": crontab(hour=0, minute=5),
    },
    "sync_price_history_and_returns": {
        "task": "backend.celery_task.market_task.sync_price_history_and_returns",
        "schedule": crontab(hour=1, minute=0),
    },

    # 🌍 MACRO
    "fetch_macro_data": {
        "task": "backend.celery_task.macro_task.fetch_macro_data",
        "schedule": crontab(hour=0, minute=12),
    },

    # 📈 TECHNICAL
    "fetch_technical_data_day": {
        "task": "backend.celery_task.technical_task.fetch_technical_data_day",
        "schedule": crontab(hour=0, minute=10),
    },
    "fetch_technical_data_week": {
        "task": "backend.celery_task.technical_task.fetch_technical_data_week",
        "schedule": crontab(hour=0, minute=15),
    },
    "fetch_technical_data_month": {
        "task": "backend.celery_task.technical_task.fetch_technical_data_month",
        "schedule": crontab(hour=0, minute=20),
    },
    "fetch_technical_data_quarter": {
        "task": "backend.celery_task.technical_task.fetch_technical_data_quarter",
        "schedule": crontab(hour=0, minute=25),
    },

    # ₿ BTC HISTORY
    "fetch_btc_daily_price": {
        "task": "backend.celery_task.btc_price_history_task.fetch_btc_history_daily",
        "schedule": crontab(hour=1, minute=10),
    },

    # ⚙️ AI VALIDATIE
    "validate_setups_task": {
        "task": "backend.ai_tasks.validation_task.validate_setups_task",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "generate_trading_advice": {
        "task": "backend.ai_tasks.trading_advice_task.generate_trading_advice",
        "schedule": crontab(minute=5, hour="*/6"),
    },

    # 🤖 AI STRATEGIE AGENT — TOEGEVOEGD
    "generate_strategy_ai": {
        "task": "backend.ai_agents.strategy_ai_agent.generate_strategy_ai",
        "schedule": crontab(hour=3, minute=30),
    },

    # 🧾 DAGELIJKSE RAPPORTAGE
    "store_daily_scores": {
        "task": "backend.celery_task.store_daily_scores_task.store_daily_scores_task",
        "schedule": crontab(hour=0, minute=45),
    },
    "generate_daily_report_pdf": {
        "task": "backend.celery_task.daily_report_task.generate_daily_report_pdf",
        "schedule": crontab(hour=1, minute=0),
    },
    "generate_daily_report_ai": {
        "task": "backend.celery_task.daily_report_task.generate_daily_report",
        "schedule": crontab(hour=2, minute=30),
    },
    "send_daily_report_email": {
        "task": "backend.celery_task.daily_report_task.send_daily_report_email",
        "schedule": crontab(hour=7, minute=0),
    },

    # 🗓️ WEEK/MAAND/KWARTAAL RAPPORTEN
    "generate_weekly_report": {
        "task": "backend.celery_task.weekly_report_task.generate_weekly_report",
        "schedule": crontab(hour=1, minute=20, day_of_week="monday"),
    },
    "generate_monthly_report": {
        "task": "backend.celery_task.monthly_report_task.generate_monthly_report",
        "schedule": crontab(hour=1, minute=30, day_of_month="1"),
    },
    "generate_quarterly_report": {
        "task": "backend.celery_task.quarterly_report_task.generate_quarterly_report",
        "schedule": crontab(hour=1, minute=45, day_of_month="1", month_of_year="1,4,7,10"),
    },

    # 🤖 MASTER SCORE / INSIGHTS
    "generate_macro_insight": {
        "task": "backend.ai_agents.macro_ai_agent.generate_macro_insight",
        "schedule": crontab(hour=3, minute=0),
    },
    "generate_market_insight": {
        "task": "backend.ai_agents.market_ai_agent.generate_market_insight",
        "schedule": crontab(hour=3, minute=10),
    },
    "generate_technical_insight": {
        "task": "backend.ai_agents.technical_ai_agent.generate_technical_insight",
        "schedule": crontab(hour=3, minute=20),
    },
    "generate_master_score": {
        "task": "backend.ai_agents.score_ai_agent.generate_master_score",
        "schedule": crontab(hour=3, minute=40),
    },
}

# =========================================================
# ✅ Imports verifiëren
# =========================================================
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
    import backend.celery_task.store_daily_scores_task
    import backend.ai_tasks.trading_advice_task
    import backend.ai_tasks.validation_task
    import backend.ai_agents.macro_ai_agent
    import backend.ai_agents.market_ai_agent
    import backend.ai_agents.technical_ai_agent
    import backend.ai_agents.score_ai_agent
    import backend.ai_agents.strategy_ai_agent    # ✅ Added
    import backend.ai_agents.report_ai_agent      # Optional but recommended

    logger.info("✅ Alle Celery taken en AI agents succesvol geïmporteerd.")

except ImportError:
    logger.error("❌ Fout bij importeren van taken:")
    logger.error(traceback.format_exc())

# =========================================================
# 🚀 Startup log
# =========================================================
logger.info(f"🚀 Celery en Beat draaien met broker: {CELERY_BROKER}")

app = celery

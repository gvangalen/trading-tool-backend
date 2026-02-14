import os
import sys
import logging
from dotenv import load_dotenv
from celery import Celery
from celery.schedules import crontab

# =========================================================
# ⚙️ .env + sys.path
# =========================================================
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# =========================================================
# 🪵 Logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

logger.info(f"🔍 CELERY_BROKER_URL = {os.getenv('CELERY_BROKER_URL')}")

# =========================================================
# 🧠 Celery instance
# =========================================================
CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

celery_app = Celery(
    "market_dashboard",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
)

# =========================================================
# 🕒 TIMEZONE — NEDERLAND
# =========================================================
celery_app.conf.enable_utc = False
celery_app.conf.timezone = "Europe/Amsterdam"

# =========================================================
# 🕒 CELERY BEAT — DAGELIJKSE PIPELINE
# =========================================================
celery_app.conf.beat_schedule = {

    # =====================================================
    # 1️⃣ GLOBAL MARKET DATA
    # =====================================================
    "fetch_market_data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/15"),
    },

    "fetch_market_data_7d": {
        "task": "backend.celery_task.market_task.fetch_market_data_7d",
        "schedule": crontab(hour=1, minute=20),
    },

    "save_market_data_daily": {
        "task": "backend.celery_task.market_task.save_market_data_daily",
        "schedule": crontab(hour=1, minute=5),
    },

    # =====================================================
    # 2️⃣ INDICATOR INGEST
    # =====================================================
    "dispatch_macro_indicators": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=1, minute=12),
        "kwargs": {
            "task_name": "backend.celery_task.macro_task.fetch_macro_data"
        },
    },

    "dispatch_technical_indicators": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=1, minute=15),
        "kwargs": {
            "task_name": "backend.celery_task.technical_task.fetch_technical_data_day"
        },
    },

    "dispatch_market_indicators": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=1, minute=18),
        "kwargs": {
            "task_name": "backend.celery_task.market_task.fetch_market_indicators"
        },
    },

    # =====================================================
    # 3️⃣ RULE-BASED SCORES
    # =====================================================
    "run_rule_based_daily_scores": {
        "task": "backend.celery_task.store_daily_scores_task.run_rule_based_daily_scores",
        "schedule": crontab(hour=3, minute=0),
    },

    # =====================================================
    # 4️⃣ AI CATEGORY AGENTS
    # =====================================================
    "dispatch_macro_ai": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=10),
        "kwargs": {
            "task_name": "backend.celery_task.macro_task.run_macro_agent_daily"
        },
    },

    "dispatch_market_ai": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=20),
        "kwargs": {
            "task_name": "backend.celery_task.market_task.run_market_agent_daily"
        },
    },

    "dispatch_technical_ai": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=30),
        "kwargs": {
            "task_name": "backend.celery_task.technical_task.run_technical_agent_daily"
        },
    },

    # =====================================================
    # ⭐ 5️⃣ REGIME MEMORY (CRITICAL LAYER)
    # =====================================================
    "dispatch_regime_memory": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=50),
        "kwargs": {
            "task_name": "backend.celery_task.regime_task.run_regime_memory"
        },
    },

    # =====================================================
    # 6️⃣ SETUP + STRATEGY
    # =====================================================
    "dispatch_setup_agent": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=4, minute=0),
        "kwargs": {
            "task_name": "backend.celery_task.setup_task.run_setup_agent_daily"
        },
    },

    "dispatch_strategy_snapshot": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=4, minute=10),
        "kwargs": {
            "task_name": "backend.celery_task.strategy_task.run_daily_strategy_snapshot"
        },
    },

    # =====================================================
    # 7️⃣ MASTER SCORE
    # =====================================================
    "run_master_score_ai": {
        "task": "backend.celery_task.store_daily_scores_task.run_master_score_ai",
        "schedule": crontab(hour=4, minute=20),
    },

    # =====================================================
    # 🤖 TRADING BOT
    # =====================================================
    "dispatch_trading_bot": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=4, minute=30),
        "kwargs": {
            "task_name": "backend.celery_task.trading_bot_task.run_daily_trading_bot"
        },
    },

    # =====================================================
    # 🔥 FINAL — DAILY REPORT
    # =====================================================
    "dispatch_daily_report": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=5, minute=0),
        "kwargs": {
            "task_name": "backend.celery_task.daily_report_task.generate_daily_report"
        },
    },
}

logger.info("🚀 Celery Beat schedule geladen (EUROPE/AMSTERDAM)")

# =========================================================
# 📌 FORCE IMPORTS — TASK REGISTRATIE
# =========================================================
try:
    import backend.celery_task.dispatcher
    import backend.celery_task.market_task
    import backend.celery_task.macro_task
    import backend.celery_task.technical_task
    import backend.celery_task.store_daily_scores_task
    import backend.celery_task.setup_task
    import backend.celery_task.strategy_task
    import backend.celery_task.trading_bot_task
    import backend.celery_task.regime_memory_task   # ⭐ NIEUW

    import backend.celery_task.daily_report_task
    import backend.celery_task.weekly_report_task
    import backend.celery_task.monthly_report_task
    import backend.celery_task.quarterly_report_task

    logger.info("✅ Alle Celery TASKS succesvol geïmporteerd")
except Exception:
    logger.error("❌ Fout bij Celery task imports", exc_info=True)

# =========================================================
# 🚀 EXPOSE
# =========================================================
app = celery_app

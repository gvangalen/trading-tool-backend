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

celery_app.autodiscover_tasks([
    "backend.celery_task",
    "backend.ai_agents",
])

celery_app.conf.enable_utc = True
celery_app.conf.timezone = "UTC"

# =========================================================
# 🕒 CELERY BEAT — DEFINITIEF CORRECTE VOLGORDE
# =========================================================
celery_app.conf.beat_schedule = {

    # =====================================================
    # 1️⃣ GLOBALE MARKET DATA (GEEN user_id)
    # =====================================================
    "fetch_market_data": {
        "task": "backend.celery_task.market_task.fetch_market_data",
        "schedule": crontab(minute="*/15"),
    },

    "save_market_data_daily": {
        "task": "backend.celery_task.market_task.save_market_data_daily",
        "schedule": crontab(hour=0, minute=5),
    },

    # =====================================================
    # 2️⃣ USER-AWARE INGESTIE
    # =====================================================
    "dispatch_macro_data": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=0, minute=12),
        "kwargs": {
            "task_name": "backend.celery_task.macro_task.fetch_macro_data"
        },
    },

    "dispatch_technical_data": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=0, minute=15),
        "kwargs": {
            "task_name": "backend.celery_task.technical_task.fetch_technical_data_day"
        },
    },

    # =====================================================
    # 3️⃣ DAILY SCORES (BASIS VOOR ALLES)
    # =====================================================
    "dispatch_daily_scores": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=0),
        "kwargs": {
            "task_name": "backend.celery_task.store_daily_scores_task.store_daily_scores_task"
        },
    },

    # =====================================================
    # 4️⃣ AI CATEGORY AGENTS (LEZEN daily_scores)
    # =====================================================
    "dispatch_macro_agent": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=10),
        "kwargs": {
            "task_name": "backend.ai_agents.macro_ai_agent.run_macro_agent"
        },
    },

    "dispatch_market_agent": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=20),
        "kwargs": {
            "task_name": "backend.ai_agents.market_ai_agent.generate_market_insight"
        },
    },

    "dispatch_technical_agent": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=30),
        "kwargs": {
            "task_name": "backend.ai_agents.technical_ai_agent.generate_technical_insight"
        },
    },

    # =====================================================
    # 5️⃣ SETUP + STRATEGY (AFHANKELIJK VAN AI INSIGHTS)
    # =====================================================
    "dispatch_setup_agent": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=3, minute=40),
        "kwargs": {
            "task_name": "backend.celery_task.setup_task.run_setup_agent_daily"
        },
    },

    "dispatch_strategy_snapshot": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=4, minute=0),
        "kwargs": {
            "task_name": "backend.celery_task.strategy_task.run_daily_strategy_snapshot"
        },
    },

    # =====================================================
    # 6️⃣ DAGRAPPORT (LAATSTE STAP)
    # =====================================================
    "dispatch_daily_report": {
        "task": "backend.celery_task.dispatcher.dispatch_for_all_users",
        "schedule": crontab(hour=5, minute=0),
        "kwargs": {
            "task_name": "backend.celery_task.daily_report_task.generate_daily_report"
        },
    },
}

logger.info("🚀 Celery Beat schedule geladen (DEFINITIEF)")

# =========================================================
# 📌 FORCE IMPORTS (GARANDEERT TASK REGISTRATIE)
# =========================================================
try:
    import backend.celery_task.dispatcher
    import backend.celery_task.market_task
    import backend.celery_task.macro_task
    import backend.celery_task.technical_task
    import backend.celery_task.store_daily_scores_task
    import backend.celery_task.setup_task
    import backend.celery_task.strategy_task
    import backend.celery_task.daily_report_task

    import backend.ai_agents.macro_ai_agent
    import backend.ai_agents.market_ai_agent
    import backend.ai_agents.technical_ai_agent
    import backend.ai_agents.setup_ai_agent
    import backend.ai_agents.strategy_ai_agent
    import backend.ai_agents.report_ai_agent
    import backend.ai_agents.score_ai_agent

    logger.info("✅ Alle Celery tasks & AI agents succesvol geïmporteerd")

except Exception as e:
    logger.error("❌ Fout bij force-imports", exc_info=True)

# =========================================================
# EXPOSE APP
# =========================================================
app = celery_app

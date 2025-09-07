# backend/celery_app.py
import os
import sys
from celery import Celery

# ✅ Project rootpad toevoegen (voor backend.* imports)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ✅ Celery app initialiseren
celery = Celery(
    "celery_app",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
)

# ✅ Algemene configuratie
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Taken automatisch ontdekken in submodules
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

# ✅ Export app voor CLI
app = celery

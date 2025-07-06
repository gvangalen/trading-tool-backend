import os
from celery import Celery

# ✅ Celery app initialiseren
celery = Celery(
    "celery_app",
    broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
)

# ✅ Configuratie
celery.conf.timezone = "UTC"
celery.conf.enable_utc = True

# ✅ Automatisch alle taken uit submodules inladen
celery.autodiscover_tasks([
    "backend.celery_task.market_task",
    "backend.celery_task.macro_task",
    "backend.celery_task.technical_task",
    "backend.celery_task.setup_task",
    "backend.celery_task.strategie_task",
    "backend.celery_task.daily_report_task",
    "backend.ai_tasks.trading_advice_task",
    "backend.ai_tasks.validation_task"
])

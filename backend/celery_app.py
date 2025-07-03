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
    "celery_task.market_task",
    "celery_task.macro_task",
    "celery_task.technical_task",
    "celery_task.setup_task",
    "celery_task.strategie_task",
    "celery_task.daily_report_task",
    "ai_tasks.trading_advice_task",
    "ai_tasks.validation_task"
])

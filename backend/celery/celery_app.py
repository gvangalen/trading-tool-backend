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

celery.autodiscover_tasks([
    "celery.market_task",
    "celery.macro_task",
    "celery.technical_task",
    "celery.setup_task",
    "celery.strategie_task",   # ✅ strategie met -ie
    "celery.daily_report_task",  # ✅ juiste bestandsnaam
    "ai_tasks.trading_advice_task",
    "ai_tasks.validation_task"
])

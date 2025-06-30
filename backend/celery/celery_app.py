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
    "celery.market_task",
    "celery.macro_task",
    "celery.technical_task",
    "celery.setup_task",
    "celery.strategy_task",
    "celery.report_task"
])

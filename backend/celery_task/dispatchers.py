# backend/celery_task/dispatchers.py
import logging
from celery import current_app
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)

def dispatch_for_all_users(task_name: str, *, active_only=True):
    """
    Dispatcht een Celery task (op basis van task_name string)
    voor alle users.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in dispatcher")
        return

    try:
        cur = conn.cursor()

        if active_only:
            cur.execute("SELECT id FROM users WHERE is_active = true;")
        else:
            cur.execute("SELECT id FROM users;")

        user_ids = [r[0] for r in cur.fetchall()]
        logger.info(f"🚀 Dispatch {task_name} voor {len(user_ids)} users")

        for user_id in user_ids:
            current_app.send_task(
                task_name,
                kwargs={"user_id": user_id},
            )

    except Exception as e:
        logger.error(f"❌ Dispatcher fout: {e}", exc_info=True)
    finally:
        conn.close()

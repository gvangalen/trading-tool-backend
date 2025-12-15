import logging
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)

def dispatch_for_all_users(task_func, *, active_only=True):
    """
    Roept een Celery task aan voor alle users.
    task_func = bijv. generate_daily_report
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
        logger.info(f"🚀 Dispatch {task_func.__name__} voor {len(user_ids)} users")

        for user_id in user_ids:
            task_func.delay(user_id)

    except Exception as e:
        logger.error(f"❌ Dispatcher fout: {e}", exc_info=True)
    finally:
        conn.close()

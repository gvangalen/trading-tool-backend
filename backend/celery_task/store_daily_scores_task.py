# ✅ backend/celery_task/store_daily_scores_task.py

import logging
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol

logger = logging.getLogger(__name__)

@shared_task(name="backend.celery_task.store_daily_scores_task")
def store_daily_scores_task():
    logger.info("🧠 Dagelijkse scoreberekening gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij daily score opslag.")
        return

    today = datetime.utcnow().date()

    try:
        # 🔍 Alleen scores voor BTC opslaan (default symbol)
        symbol = "BTC"
        scores = get_scores_for_symbol(symbol)

        logger.info(f"📊 Berekening scores voor {symbol}: {scores}")

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_scores (
                    report_date, macro_score, technical_score, market_score, sentiment_score, setup_score
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date) DO UPDATE SET
                    macro_score = EXCLUDED.macro_score,
                    technical_score = EXCLUDED.technical_score,
                    market_score = EXCLUDED.market_score,
                    sentiment_score = EXCLUDED.sentiment_score,
                    setup_score = EXCLUDED.setup_score
            """, (
                today,
                scores.get("macro_score", 0),
                scores.get("technical_score", 0),
                scores.get("market_score", 0),
                scores.get("sentiment_score", 0),
                scores.get("setup_score", 0)
            ))

        conn.commit()
        logger.info(f"✅ Dagelijkse scores opgeslagen voor {today}: {scores}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan dagelijkse scores: {e}")
    finally:
        conn.close()

# ✅ backend/celery_task/store_daily_scores_task.py

import logging
import json
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
        # ✅ Nieuw: haal scores direct op inclusief metadata
        scores = get_scores_for_symbol(include_metadata=True)

        logger.info(f"📊 Berekende scores: {scores}")

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_scores (
                    report_date,
                    macro_score, macro_interpretation, macro_top_contributors,
                    technical_score, technical_interpretation, technical_top_contributors,
                    setup_score, setup_interpretation, setup_top_contributors,
                    market_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_date) DO UPDATE SET
                    macro_score = EXCLUDED.macro_score,
                    macro_interpretation = EXCLUDED.macro_interpretation,
                    macro_top_contributors = EXCLUDED.macro_top_contributors,
                    technical_score = EXCLUDED.technical_score,
                    technical_interpretation = EXCLUDED.technical_interpretation,
                    technical_top_contributors = EXCLUDED.technical_top_contributors,
                    setup_score = EXCLUDED.setup_score,
                    setup_interpretation = EXCLUDED.setup_interpretation,
                    setup_top_contributors = EXCLUDED.setup_top_contributors,
                    market_score = EXCLUDED.market_score
            """, (
                today,
                scores.get("macro_score", 0),
                scores.get("macro_interpretation", ""),
                json.dumps(scores.get("macro_top_contributors", [])),

                scores.get("technical_score", 0),
                scores.get("technical_interpretation", ""),
                json.dumps(scores.get("technical_top_contributors", [])),

                scores.get("setup_score", 0),
                scores.get("setup_interpretation", ""),
                json.dumps(scores.get("setup_top_contributors", [])),

                scores.get("market_score", 0),
            ))

        conn.commit()
        logger.info(f"✅ Dagelijkse scores opgeslagen voor {today}")

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan dagelijkse scores: {e}", exc_info=True)
    finally:
        conn.close()

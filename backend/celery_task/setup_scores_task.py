import logging
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol

logger = logging.getLogger(__name__)

@shared_task(name="backend.celery_task.setup_scores_task.store_setup_scores_task")
def store_setup_scores_task():
    logger.info("📝 Setup-scores opslag gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij setup-scores opslag.")
        return

    today = datetime.utcnow().date()  # 📅 Huidige UTC-datum

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM setups")
            setups = cur.fetchall()

            for setup_id, symbol in setups:
                scores = get_scores_for_symbol(symbol)

                cur.execute("""
                    INSERT INTO setup_scores (
                        setup_id, symbol,
                        macro_score, technical_score,
                        market_score, setup_score,
                        report_date
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    setup_id, symbol,
                    scores.get("macro_score", 0),
                    scores.get("technical_score", 0),
                    scores.get("market_score", 0),
                    scores.get("setup_score", 0),
                    today  # ✅ rapportdatum
                ))

                logger.info(f"✅ Setup {setup_id} ({symbol}) opgeslagen met scores: {scores}")

        conn.commit()
        logger.info("✅ Alle setup-scores succesvol opgeslagen.")

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan setup-scores: {e}")
    finally:
        conn.close()

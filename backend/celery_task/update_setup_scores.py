# ✅ backend/celery_task/update_setup_scores.py

import logging
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol

logger = logging.getLogger(__name__)

def update_setup_scores():
    logger.info("🔄 Setup-scores bijwerken gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij update_setup_scores")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, symbol FROM setups")
            setups = cur.fetchall()

            for setup_id, symbol in setups:
                scores = get_scores_for_symbol(symbol)

                cur.execute("""
                    UPDATE setups
                    SET macro_score = %s,
                        technical_score = %s,
                        market_score = %s,
                        sentiment_score = %s,
                        setup_score = %s
                    WHERE id = %s
                """, (
                    scores.get("macro_score", 0),
                    scores.get("technical_score", 0),
                    scores.get("market_score", 0),
                    scores.get("sentiment_score", 0),
                    scores.get("setup_score", 0),
                    setup_id
                ))

                logger.info(
                    f"✅ Setup {setup_id} ({symbol}) geüpdatet met scores: "
                    f"{scores}"
                )

        conn.commit()
        logger.info("✅ Alle setups succesvol bijgewerkt.")
    except Exception as e:
        logger.error(f"❌ Fout bij updaten setup-scores: {e}")
    finally:
        conn.close()

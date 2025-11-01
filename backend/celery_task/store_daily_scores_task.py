import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol, match_setups_to_score
from backend.utils.setup_utils import get_all_setups

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
        # ✅ Stap 1: haal scores op (inclusief metadata)
        scores = get_scores_for_symbol(include_metadata=True)
        logger.info(f"📊 Berekende scores: {scores}")

        # ✅ Stap 2: sla dagelijkse totaalscores op in daily_scores
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

        # ✅ Stap 3: haal alle setups op en bepaal de best matchende setup
        setups = get_all_setups()
        matched = match_setups_to_score(setups, scores.get("setup_score", 0))

        # ✅ Stap 4: sla best passende setup op in daily_setup_scores
        if matched:
            best = matched[0]
            logger.info(f"🎯 Beste setup gevonden: {best['name']} (score {scores.get('setup_score')})")

            with conn.cursor() as cur:
                # ⬇️ Eerst alle setups op deze datum inactief maken
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_active = false
                    WHERE date = %s
                """, (today,))

                # ⬆️ Daarna de best matchende setup opslaan of activeren
                cur.execute("""
                    INSERT INTO daily_setup_scores (setup_id, date, score, explanation, is_active)
                    VALUES (%s, %s, %s, %s, true)
                    ON CONFLICT (date, setup_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        explanation = EXCLUDED.explanation,
                        is_active = true
                """, (
                    best["id"],
                    today,
                    scores.get("setup_score", 0),
                    best.get("explanation", ""),
                ))

        else:
            logger.warning("⚠️ Geen passende setup gevonden voor huidige score.")

        # ✅ Commit alles
        conn.commit()
        logger.info(f"✅ Dagelijkse scores én setup succesvol opgeslagen voor {today}")

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan dagelijkse scores: {e}", exc_info=True)
    finally:
        conn.close()

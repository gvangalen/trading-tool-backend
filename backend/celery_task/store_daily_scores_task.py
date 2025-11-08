import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol  # ✅ oude match-functie verwijderd
from backend.utils.setup_utils import get_all_setups

logger = logging.getLogger(__name__)


# =========================================================
# ✅ Vervanging van oude match_setups_to_score()
# =========================================================
def match_setups_to_score(setups, setup_score):
    """
    Bepaalt welke setups het best overeenkomen met de huidige setup_score.
    De score wordt vergeleken met het min/max-bereik van elke setup.
    Geeft een gesorteerde lijst van best passende setups terug.
    """
    if not setups:
        return []

    matched = []
    for s in setups:
        try:
            min_score = float(s.get("min_macro_score") or s.get("min_score") or 0)
            max_score = float(s.get("max_macro_score") or s.get("max_score") or 100)
            if min_score <= setup_score <= max_score:
                matched.append(s)
        except Exception as e:
            logger.warning(f"⚠️ Setup match fout ({s.get('name')}): {e}")

    # Sorteer: dichtst bij de actuele score eerst
    matched.sort(key=lambda x: abs(setup_score - ((float(x.get("min_macro_score") or 0) + float(x.get("max_macro_score") or 100)) / 2)))
    return matched


# =========================================================
# ✅ Dagelijkse Celery-task
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task")
def store_daily_scores_task():
    """
    Dagelijkse Celery-task:
    - Berekent macro/technical/market/setup scores via DB
    - Slaat totaalscores op in daily_scores
    - Matcht best passende setup en activeert die in daily_setup_scores
    """
    logger.info("🧠 Dagelijkse scoreberekening gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij daily score opslag.")
        return

    today = datetime.utcnow().date()

    try:
        # ✅ Stap 1: haal scores op (inclusief metadata)
        scores = get_scores_for_symbol(include_metadata=True) or {}
        if not scores:
            logger.warning("⚠️ Geen scores berekend – gebruik fallbackwaarden.")
            scores = {
                "macro_score": 0, "technical_score": 0, "market_score": 0, "setup_score": 0,
                "macro_interpretation": "Geen data",
                "technical_interpretation": "Geen data",
                "macro_top_contributors": [],
                "technical_top_contributors": [],
                "setup_top_contributors": [],
            }

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
                float(scores.get("macro_score", 0)),
                scores.get("macro_interpretation", "Geen data"),
                json.dumps(scores.get("macro_top_contributors", [])),
                float(scores.get("technical_score", 0)),
                scores.get("technical_interpretation", "Geen data"),
                json.dumps(scores.get("technical_top_contributors", [])),
                float(scores.get("setup_score", 0)),
                scores.get("setup_interpretation", "Geen data"),
                json.dumps(scores.get("setup_top_contributors", [])),
                float(scores.get("market_score", 0)),
            ))

        # ✅ Stap 3: haal setups op en match met setup_score
        setups = get_all_setups() or []
        matched = match_setups_to_score(setups, scores.get("setup_score", 0))

        if matched:
            best = matched[0]
            logger.info(f"🎯 Beste setup: {best['name']} (score {scores.get('setup_score', 0)})")

            with conn.cursor() as cur:
                # ⬇️ Eerst alle setups van vandaag inactief maken
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_active = false
                    WHERE date = %s
                """, (today,))

                # ⬆️ Beste match activeren
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
                    float(scores.get("setup_score", 0)),
                    best.get("explanation", ""),
                ))
        else:
            logger.warning("⚠️ Geen passende setup gevonden voor huidige score of lege setup-lijst.")

        # ✅ Commit alles
        conn.commit()
        logger.info(f"✅ Dagelijkse scores én setup succesvol opgeslagen voor {today}")

    except Exception as e:
        logger.error(f"❌ Fout bij opslaan dagelijkse scores: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()
        logger.info("🔒 Databaseverbinding gesloten.")

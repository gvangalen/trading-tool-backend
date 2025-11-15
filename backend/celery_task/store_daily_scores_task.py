import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.setup_utils import get_all_setups

logger = logging.getLogger(__name__)


# =========================================================
# ✅ Setup matching helper (blijft correct)
# =========================================================
def match_setups_to_score(setups, setup_score):
    """
    Bepaalt welke setups het best overeenkomen met de huidige setup_score.
    Match gebeurt op min/max score die voor setups geldt.
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

    # Sorteer op dichtst bij de score
    matched.sort(
        key=lambda x: abs(setup_score - (
            (float(x.get("min_macro_score") or 0) +
             float(x.get("max_macro_score") or 100)) / 2
        ))
    )
    return matched


# =========================================================
# 🧠 DAGELIJKSE LUXE SCORE-TASK (MACRO / TECH / MARKET / SETUP)
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task")
def store_daily_scores_task():
    """
    De volledige luxe daily score pipeline:
    - Haal macro/technical/market/setup via nieuwe DB-rule engine
    - Opslaan in daily_scores (met uitleg + contributors)
    - Setup matching + active setup opslaan
    """
    logger.info("🧠 Dagelijkse scoreberekening gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding bij daily score opslag.")
        return

    today = datetime.utcnow().date()

    try:
        # =====================================================
        # 1️⃣ BEREKEN SCORES VIA DB RULE ENGINE (macro + tech + market + setup)
        # =====================================================
        scores = get_scores_for_symbol(include_metadata=True) or {}

        if not scores:
            logger.warning("⚠️ Geen scores uit DB – fallback waarden gebruikt.")
            scores = {
                "macro_score": 0,
                "technical_score": 0,
                "market_score": 0,
                "setup_score": 0,
                "macro_interpretation": "Geen data",
                "technical_interpretation": "Geen data",
                "market_interpretation": "Geen data",
                "macro_top_contributors": [],
                "technical_top_contributors": [],
                "market_top_contributors": [],
            }

        logger.info(f"📊 Berekende DB-scores: {json.dumps(scores, indent=2)}")

        # =====================================================
        # 2️⃣ OPSLAAN IN daily_scores (volledig luxe structuur)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_scores (
                    report_date,

                    macro_score,
                    macro_interpretation,
                    macro_top_contributors,

                    technical_score,
                    technical_interpretation,
                    technical_top_contributors,

                    market_score,
                    market_interpretation,
                    market_top_contributors,

                    setup_score,
                    setup_interpretation,
                    setup_top_contributors
                )
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (report_date) DO UPDATE SET
                    macro_score = EXCLUDED.macro_score,
                    macro_interpretation = EXCLUDED.macro_interpretation,
                    macro_top_contributors = EXCLUDED.macro_top_contributors,

                    technical_score = EXCLUDED.technical_score,
                    technical_interpretation = EXCLUDED.technical_interpretation,
                    technical_top_contributors = EXCLUDED.technical_top_contributors,

                    market_score = EXCLUDED.market_score,
                    market_interpretation = EXCLUDED.market_interpretation,
                    market_top_contributors = EXCLUDED.market_top_contributors,

                    setup_score = EXCLUDED.setup_score,
                    setup_interpretation = EXCLUDED.setup_interpretation,
                    setup_top_contributors = EXCLUDED.setup_top_contributors
            """, (
                today,

                float(scores["macro_score"]),
                scores["macro_interpretation"],
                json.dumps(scores["macro_top_contributors"]),

                float(scores["technical_score"]),
                scores["technical_interpretation"],
                json.dumps(scores["technical_top_contributors"]),

                float(scores["market_score"]),
                scores["market_interpretation"],
                json.dumps(scores["market_top_contributors"]),

                float(scores["setup_score"]),
                scores.get("setup_interpretation", "Geen data"),
                json.dumps(scores.get("setup_top_contributors", [])),
            ))

        # =====================================================
        # 3️⃣ MATCH ACTIEVE SETUP OP BASIS VAN SETUP SCORE
        # =====================================================
        setups = get_all_setups() or []
        matched = match_setups_to_score(setups, scores["setup_score"])

        # Eerst alles van vandaag deactiveren
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_active = false
                WHERE date = %s
            """, (today,))

        if matched:
            best = matched[0]
            logger.info(f"🎯 Beste setup: {best['name']} (score={scores['setup_score']})")

            with conn.cursor() as cur:
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
                    float(scores["setup_score"]),
                    best.get("explanation", ""),
                ))
        else:
            logger.warning("⚠️ Geen setup matched huidige setup_score.")

        # =====================================================
        # 4️⃣ Commit
        # =====================================================
        conn.commit()
        logger.info("✅ Dagelijkse scores + actieve setup opgeslagen.")

    except Exception as e:
        logger.error(f"❌ Fout bij dagelijkse scoring: {e}", exc_info=True)
        conn.rollback()

    finally:
        conn.close()
        logger.info("🔒 Databaseverbinding gesloten.")

import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.setup_utils import get_all_setups

logger = logging.getLogger(__name__)


# =========================================================
# 🔍 Luxe setup-matching (macro + technical + market)
# =========================================================
def match_setups_to_score(setups, scores):
    """
    Nieuwe matching:
    - Macro-score moet binnen min/max_macro_score vallen
    - Technical-score binnen min/max_technical_score
    - Market-score binnen min/max_market_score
    - Sorteren op dichtst-bij totale setup_score
    """

    macro_score = scores["macro_score"]
    technical_score = scores["technical_score"]
    market_score = scores["market_score"]
    setup_score = scores["setup_score"]

    matched = []

    for s in setups:
        try:
            # Macro
            min_macro = float(s.get("min_macro_score") or 0)
            max_macro = float(s.get("max_macro_score") or 100)
            if not (min_macro <= macro_score <= max_macro):
                continue

            # Technical
            min_tech = float(s.get("min_technical_score") or 0)
            max_tech = float(s.get("max_technical_score") or 100)
            if not (min_tech <= technical_score <= max_tech):
                continue

            # Market
            min_market = float(s.get("min_market_score") or 0)
            max_market = float(s.get("max_market_score") or 100)
            if not (min_market <= market_score <= max_market):
                continue

            matched.append(s)

        except Exception as e:
            logger.warning(f"⚠️ Setup match fout bij '{s.get('name')}': {e}")

    # Sorteren op dichtst bij totale setup_score
    matched.sort(
        key=lambda s: abs(
            setup_score - (
                (
                    float(s.get("min_macro_score") or 0) +
                    float(s.get("max_macro_score") or 100) +
                    float(s.get("min_technical_score") or 0) +
                    float(s.get("max_technical_score") or 100)
                ) / 4
            )
        )
    )

    return matched


# =========================================================
# 🧠 DAGELIJKSE SUPER-LUXE SCORE TASK
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task")
def store_daily_scores_task():

    logger.info("🧠 Dagelijkse scoreberekening gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    today = datetime.utcnow().date()

    try:
        # =====================================================
        # 1️⃣ SCORES OPHALEN (MACRO + TECH + MARKET)
        # =====================================================
        scores = get_scores_for_symbol(include_metadata=True)

        if not scores:
            logger.error("❌ Geen scores beschikbaar — stop task")
            return

        logger.info(f"📊 DB-scores:\n{json.dumps(scores, indent=2)}")

        # =====================================================
        # 2️⃣ OPSLAAN IN daily_scores
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
                "Op basis van rule-engine",  # 👈 Kan je later uitbreiden
                json.dumps(scores.get("setup_top_contributors", [])),
            ))

        # =====================================================
        # 3️⃣ SETUP MATCHING
        # =====================================================
        setups = get_all_setups()

        matched = match_setups_to_score(setups, scores)

        # Eerst alles deactiveren (correcte kolom: report_date)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_active = false
                WHERE report_date = %s
            """, (today,))

        if matched:
            best = matched[0]

            logger.info(f"🎯 Beste setup geselecteerd: {best['name']}")

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores (setup_id, report_date, score, explanation, is_active)
                    VALUES (%s, %s, %s, %s, true)
                    ON CONFLICT (report_date, setup_id) DO UPDATE SET
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
            logger.warning("⚠️ Geen enkele setup matcht de huidige score.")

        conn.commit()
        logger.info("✅ Dagelijkse scores + setup saved")

    except Exception as e:
        logger.error(f"❌ Fout bij daily score task: {e}", exc_info=True)
        conn.rollback()

    finally:
        conn.close()
        logger.info("🔒 Verbinding gesloten")

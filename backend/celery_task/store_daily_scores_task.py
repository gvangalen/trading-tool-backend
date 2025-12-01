import logging
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.setup_utils import get_all_setups

logger = logging.getLogger(__name__)


# =========================================================
# 🔍 BEST-MATCH LOGICA
# =========================================================
def best_match(setups, scores):
    """
    Nieuwe logica:
    👉 altijd een beste setup kiezen
    👉 via afstand tot range per categorie
    """

    macro_score = float(scores["macro_score"])
    technical_score = float(scores["technical_score"])
    market_score = float(scores["market_score"])

    def dist(val, low, high):
        """Afstand tot range (0 = binnen range)."""
        try:
            low = float(low)
            high = float(high)
        except:
            low, high = 0, 100

        if val < low:
            return low - val
        if val > high:
            return val - high
        return 0

    candidates = []

    for s in setups:
        try:
            min_macro = float(s.get("min_macro_score") or 0)
            max_macro = float(s.get("max_macro_score") or 100)

            min_tech = float(s.get("min_technical_score") or 0)
            max_tech = float(s.get("max_technical_score") or 100)

            min_market = float(s.get("min_market_score") or 0)
            max_market = float(s.get("max_market_score") or 100)

            d_macro = dist(macro_score, min_macro, max_macro)
            d_tech = dist(technical_score, min_tech, max_tech)
            d_market = dist(market_score, min_market, max_market)

            total = d_macro + d_tech + d_market

            candidates.append({
                "setup": s,
                "macro_dist": d_macro,
                "tech_dist": d_tech,
                "market_dist": d_market,
                "total_dist": total
            })

        except Exception as e:
            logger.warning(f"⚠️ Could not evaluate setup {s.get('name')}: {e}")

    # Als er helemaal geen setups zijn
    if not candidates:
        return None, []

    # Sorteer op laagste afstand
    candidates.sort(key=lambda x: x["total_dist"])

    best = candidates[0]
    return best, candidates



# =========================================================
# 🧠 DAGELIJKSE SCORE TASK
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task.store_daily_scores_task")
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
                "Op basis van rule-engine",
                json.dumps(scores.get("setup_top_contributors", [])),
            ))

        # =====================================================
        # 3️⃣ SETUP MATCHING via BEST MATCH
        # =====================================================
        setups = get_all_setups()
        best, candidates = best_match(setups, scores)

        # Alle setups van vandaag eerst inactief maken
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_active = false
                WHERE report_date = %s
            """, (today,))

        if best:
            s = best["setup"]
            logger.info(f"🎯 Beste setup: {s['name']} (distance={best['total_dist']})")

            # Breakdown opslaan
            breakdown = {
                "macro_dist": best["macro_dist"],
                "tech_dist": best["tech_dist"],
                "market_dist": best["market_dist"],
                "total_dist": best["total_dist"]
            }

            # Beste setup opslaan in daily_setup_scores
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores 
                        (setup_id, report_date, score, explanation, breakdown, is_active)
                    VALUES (%s, %s, %s, %s, %s, true)
                    ON CONFLICT (report_date, setup_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        explanation = EXCLUDED.explanation,
                        breakdown = EXCLUDED.breakdown,
                        is_active = true
                """, (
                    s["id"],
                    today,
                    float(scores["setup_score"]),
                    s.get("explanation", ""),
                    json.dumps(breakdown),
                ))

        else:
            logger.warning("⚠️ Geen enkele setup gevonden — dit zou nooit moeten gebeuren")

        conn.commit()
        logger.info("✅ Dagelijkse scores + beste setup opgeslagen.")

    except Exception as e:
        logger.error(f"❌ Fout bij daily score task: {e}", exc_info=True)
        conn.rollback()

    finally:
        conn.close()
        logger.info("🔒 Verbinding gesloten")

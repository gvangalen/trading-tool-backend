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
        except Exception:
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
# 🧠 DAGELIJKSE SCORE TASK (USER-SPECIFIEK)
# =========================================================
@shared_task(name="backend.celery_task.store_daily_scores_task.store_daily_scores_task")
def store_daily_scores_task(user_id: int):
    """
    Slaat daily_scores + beste setup op voor een specifieke user.
    """
    logger.info(f"🧠 Dagelijkse scoreberekening gestart voor user_id={user_id}...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    today = datetime.utcnow().date()

    try:
        # =====================================================
        # 1️⃣ SCORES OPHALEN (MACRO + TECH + MARKET + SETUP)
        # =====================================================
        try:
            scores = get_scores_for_symbol(user_id=user_id, include_metadata=True)
        except TypeError:
            # fallback als oude signatuur nog actief zou zijn
            scores = get_scores_for_symbol(include_metadata=True)

        if not scores:
            logger.error(f"❌ Geen scores beschikbaar voor user_id={user_id} — stop task")
            return

        logger.info(f"📊 DB-scores voor user_id={user_id}:\n{json.dumps(scores, indent=2)}")

        # =====================================================
        # 2️⃣ OPSLAAN IN daily_scores (USER-SPECIFIEK)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_scores (
                    report_date,
                    user_id,

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
                VALUES (%s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (report_date, user_id) DO UPDATE SET
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
                user_id,

                float(scores["macro_score"]),
                scores.get("macro_interpretation", ""),
                json.dumps(scores.get("macro_top_contributors", [])),

                float(scores["technical_score"]),
                scores.get("technical_interpretation", ""),
                json.dumps(scores.get("technical_top_contributors", [])),

                float(scores["market_score"]),
                scores.get("market_interpretation", ""),
                json.dumps(scores.get("market_top_contributors", [])),

                float(scores.get("setup_score", 0)),
                "Op basis van rule-engine",
                json.dumps(scores.get("setup_top_contributors", [])),
            ))

        # =====================================================
        # 3️⃣ SETUP MATCHING via BEST MATCH (USER-SPECIFIEK)
        # =====================================================
        # Voor nu nemen we setups voor BTC; eventueel uitbreiden naar andere symbols
        setups = get_all_setups(symbol="BTC", user_id=user_id)
        best, candidates = best_match(setups, scores)

        # Alle setups van vandaag voor deze user eerst inactief maken
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_active = false
                WHERE report_date = %s
                  AND user_id = %s
            """, (today, user_id))

        if best:
            s = best["setup"]
            logger.info(
                f"🎯 Beste setup voor user_id={user_id}: "
                f"{s['name']} (distance={best['total_dist']})"
            )

            breakdown = {
                "macro_dist": best["macro_dist"],
                "tech_dist": best["tech_dist"],
                "market_dist": best["market_dist"],
                "total_dist": best["total_dist"]
            }

            # Beste setup opslaan in daily_setup_scores (USER-SPECIFIEK)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores 
                        (user_id, setup_id, report_date, score, explanation, breakdown, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, true)
                    ON CONFLICT (user_id, report_date, setup_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        explanation = EXCLUDED.explanation,
                        breakdown = EXCLUDED.breakdown,
                        is_active = EXCLUDED.is_active
                """, (
                    user_id,
                    s["id"],
                    today,
                    float(scores.get("setup_score", 0)),
                    s.get("explanation", ""),
                    json.dumps(breakdown),
                ))

        else:
            logger.warning(f"⚠️ Geen enkele setup gevonden voor user_id={user_id}.")

        conn.commit()
        logger.info(f"✅ Dagelijkse scores + beste setup opgeslagen voor user_id={user_id}.")

    except Exception as e:
        logger.error(f"❌ Fout bij daily score task voor user_id={user_id}: {e}", exc_info=True)
        conn.rollback()

    finally:
        conn.close()
        logger.info(f"🔒 Verbinding gesloten voor user_id={user_id}")

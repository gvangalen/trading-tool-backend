import logging
import json
from decimal import Decimal

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🔢 Helpers
# ======================================================

def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def score_overlap(value, min_v, max_v):
    """
    Bepaalt overlap-score (0–100).
    NULL min/max = geen beperking = 100
    """
    value = to_float(value)
    min_v = to_float(min_v)
    max_v = to_float(max_v)

    if value is None:
        return 0

    # ✅ GEEN FILTER = VOLLEDIGE MATCH
    if min_v is None and max_v is None:
        return 100

    if min_v is not None and value < min_v:
        return 0

    if max_v is not None and value > max_v:
        return 0

    # Eénzijdige range → geldig = 100
    if min_v is None or max_v is None:
        return 100

    # Volledige range → afstand tot midden
    mid = (min_v + max_v) / 2
    max_dist = (max_v - min_v) / 2

    if max_dist <= 0:
        return 100

    dist = abs(value - mid)
    return round(100 - (dist / max_dist * 100))


# ======================================================
# 🤖 SETUP AI AGENT — FINAL & CORRECT
# ======================================================

def run_setup_agent(*, user_id: int, asset: str = "BTC"):
    """
    Bepaalt beste setup van de dag voor één user + asset.
    """

    if user_id is None:
        raise ValueError("❌ Setup Agent vereist user_id")

    logger.info(f"🤖 [Setup-Agent] Start (user_id={user_id}, asset={asset})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # ======================================================
        # 1️⃣ Daily scores ophalen
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                  AND user_id = %s
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen daily_scores gevonden")
            return

        macro_score, technical_score, market_score = map(to_float, row)

        # ======================================================
        # 2️⃣ Setups ophalen
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id, name, symbol,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score
                FROM setups
                WHERE user_id = %s
                  AND symbol = %s
                ORDER BY created_at DESC;
            """, (user_id, asset))
            setups = cur.fetchall()

        if not setups:
            logger.info("ℹ️ Geen setups gevonden")
            return

        # ======================================================
        # 3️⃣ Reset previous best
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_best = FALSE
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE;
            """, (user_id,))

        results = []
        best_setup_id = None
        best_score = -1

        # ======================================================
        # 4️⃣ Evaluatie per setup
        # ======================================================
        for (
            setup_id, name, symbol,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market
        ) in setups:

            macro_match = score_overlap(macro_score, min_macro, max_macro)
            tech_match = score_overlap(technical_score, min_tech, max_tech)
            market_match = score_overlap(market_score, min_market, max_market)

            total_score = round((macro_match + tech_match + market_match) / 3)

            is_active = total_score > 0

            if total_score > best_score:
                best_score = total_score
                best_setup_id = setup_id

            ai_comment = ask_gpt_text(
                f"Marktscores: macro {macro_score}, technical {technical_score}, market {market_score}. "
                f"Setup '{name}' matcht {total_score}/100. Geef 1 korte reden."
            )

            results.append({
                "setup_id": setup_id,
                "name": name,
                "symbol": symbol,
                "score": total_score,
                "active": is_active,
            })

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores
                        (setup_id, user_id, report_date, score, is_active, explanation)
                    VALUES (%s, %s, CURRENT_DATE, %s, %s, %s)
                    ON CONFLICT (setup_id, user_id, report_date)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation,
                        created_at = NOW();
                """, (
                    setup_id,
                    user_id,
                    total_score,
                    is_active,
                    ai_comment,
                ))

        # ======================================================
        # 5️⃣ Markeer BEST setup
        # ======================================================
        if best_setup_id is not None:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s
                      AND user_id = %s
                      AND report_date = CURRENT_DATE;
                """, (best_setup_id, user_id))

        # ======================================================
        # 6️⃣ AI category insight
        # ======================================================
        avg_score = round(sum(r["score"] for r in results) / len(results), 2)

        summary = (
            f"Beste {asset}-setup scoort {best_score}/100."
            if best_score > 0 else
            f"Geen duidelijke {asset}-setup vandaag."
        )

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                avg_score,
                "Actief" if best_score >= 50 else "Neutraal",
                "Kansrijk" if best_score >= 50 else "Afwachten",
                "Gemiddeld",
                summary,
                json.dumps(results[:3]),
            ))

        conn.commit()
        logger.info(f"✅ [Setup-Agent] Voltooid (user_id={user_id})")

    except Exception:
        conn.rollback()
        logger.error("❌ Setup AI Agent crash", exc_info=True)

    finally:
        conn.close()

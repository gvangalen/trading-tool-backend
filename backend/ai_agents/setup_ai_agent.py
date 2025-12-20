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
    Geeft overlap-score 0–100 voor value binnen [min_v, max_v]
    """
    value = to_float(value)
    min_v = to_float(min_v)
    max_v = to_float(max_v)

    if value is None or min_v is None or max_v is None:
        return 0

    if value < min_v or value > max_v:
        return 0

    mid = (min_v + max_v) / 2
    dist = abs(value - mid)
    max_dist = (max_v - min_v) / 2

    if max_dist <= 0:
        return 100

    return round(100 - (dist / max_dist * 100))


# ======================================================
# 🤖 SETUP AI AGENT — PURE LOGICA (USER + ASSET)
# ======================================================

def run_setup_agent(*, user_id: int, asset: str = "BTC"):
    """
    Bepaalt beste setup van de dag voor één user + asset.

    Schrijft:
    - daily_setup_scores
    - ai_category_insights (category='setup')

    ⚠️ Geen Celery decorator (wordt altijd via task aangeroepen)
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
        # 1️⃣ Daily scores (macro / technical / market)
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
        # 2️⃣ Setups ophalen (user + asset)
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
        # 3️⃣ Reset best-of-day (BELANGRIJK)
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
        # 4️⃣ Matching + opslaan daily_setup_scores
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
            is_active = macro_match > 0 and tech_match > 0 and market_match > 0

            if total_score > best_score:
                best_score = total_score
                best_setup_id = setup_id

            # Korte AI uitleg (max 1 zin)
            prompt = (
                f"Marktscore: macro {macro_score}, technical {technical_score}, market {market_score}. "
                f"Setup '{name}' matcht {total_score}/100. Geef 1 korte reden."
            )
            ai_comment = ask_gpt_text(prompt)

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
        if best_setup_id:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s
                      AND user_id = %s
                      AND report_date = CURRENT_DATE;
                """, (best_setup_id, user_id))

        # ======================================================
        # 6️⃣ AI CATEGORY INSIGHT (SETUP)
        # ======================================================
        avg_score = round(sum(r["score"] for r in results) / len(results), 2)
        active_count = sum(1 for r in results if r["active"])

        trend = (
            "Sterk" if best_score >= 70 else
            "Gemiddeld" if best_score >= 40 else
            "Zwak"
        )
        bias = "Kansrijk" if active_count > 0 else "Afwachten"
        risk = (
            "Laag" if market_score >= 60 else
            "Gemiddeld" if market_score >= 40 else
            "Hoog"
        )

        summary = (
            f"Beste {asset}-setup scoort {best_score}/100."
            if best_setup_id else
            f"Geen actieve {asset}-setup vandaag."
        )

        top_signals = sorted(results, key=lambda r: r["score"], reverse=True)[:3]

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                avg_score,
                trend,
                bias,
                risk,
                summary,
                json.dumps(top_signals),
            ))

        conn.commit()
        logger.info(f"✅ [Setup-Agent] Voltooid (user_id={user_id}, asset={asset})")

    except Exception:
        conn.rollback()
        logger.error("❌ Setup AI Agent crash", exc_info=True)

    finally:
        conn.close()

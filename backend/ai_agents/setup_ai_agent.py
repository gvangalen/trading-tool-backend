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
    Overlap-score (0–100)
    NULL min/max = geen filter = 100
    """
    value = to_float(value)
    min_v = to_float(min_v)
    max_v = to_float(max_v)

    if value is None:
        return 0

    if min_v is None and max_v is None:
        return 100
    if min_v is not None and value < min_v:
        return 0
    if max_v is not None and value > max_v:
        return 0
    if min_v is None or max_v is None:
        return 100

    mid = (min_v + max_v) / 2
    max_dist = (max_v - min_v) / 2
    if max_dist <= 0:
        return 100

    return round(100 - abs(value - mid) / max_dist * 100)


# ======================================================
# 🤖 SETUP AGENT — DEFINITIEF
# ======================================================

def run_setup_agent(*, user_id: int, asset: str = "BTC"):
    """
    Doel:
    - daily_setup_scores vullen (technisch)
    - 1 setup-advies genereren (ai_category_insights)

    GEEN setup krijgt ooit automatisch score 0
    → altijd relatieve ranking
    """

    if not user_id:
        raise ValueError("❌ Setup agent vereist user_id")

    logger.info(f"🤖 [Setup-Agent] Start (user_id={user_id}, asset={asset})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # ==================================================
        # 1️⃣ Daily scores (marktcontext)
        # ==================================================
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                  AND user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen daily_scores")
            return

        macro, technical, market = map(to_float, row)

        # ==================================================
        # 2️⃣ Setups ophalen
        # ==================================================
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, name,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score
                FROM setups
                WHERE user_id = %s AND symbol = %s
                ORDER BY created_at DESC
                """,
                (user_id, asset),
            )
            setups = cur.fetchall()

        if not setups:
            logger.info("ℹ️ Geen setups")
            return

        # Reset best-flag
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE daily_setup_scores
                SET is_best = FALSE
                WHERE user_id = %s AND report_date = CURRENT_DATE
                """,
                (user_id,),
            )

        evaluations = []

        # ==================================================
        # 3️⃣ Per setup: ALTIJD score berekenen
        # ==================================================
        for (
            setup_id, name,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market
        ) in setups:

            m = score_overlap(macro, min_macro, max_macro)
            t = score_overlap(technical, min_tech, max_tech)
            mk = score_overlap(market, min_market, max_market)

            score = round((m + t + mk) / 3)

            explanation = ask_gpt_text(
                f"Marktscore: macro {macro}, technical {technical}, market {market}. "
                f"Waarom past setup '{name}' vandaag beter of slechter?"
            )

            evaluations.append({
                "setup_id": setup_id,
                "name": name,
                "score": score,
                "components": {
                    "macro": m,
                    "technical": t,
                    "market": mk,
                },
            })

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO daily_setup_scores
                        (setup_id, user_id, report_date, score, is_active, explanation)
                    VALUES (%s, %s, CURRENT_DATE, %s, %s, %s)
                    ON CONFLICT (setup_id, user_id, report_date)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        is_active = TRUE,
                        explanation = EXCLUDED.explanation,
                        created_at = NOW()
                    """,
                    (setup_id, user_id, score, explanation),
                )

        # ==================================================
        # 4️⃣ Beste setup bepalen (RELATIEF)
        # ==================================================
        ranked = sorted(evaluations, key=lambda x: x["score"], reverse=True)
        best = ranked[0]

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE daily_setup_scores
                SET is_best = TRUE
                WHERE setup_id = %s AND user_id = %s
                      AND report_date = CURRENT_DATE
                """,
                (best["setup_id"], user_id),
            )

        avg_score = round(
            sum(e["score"] for e in evaluations) / len(evaluations), 1
        )

        trend = "Actief" if best["score"] >= 60 else "Neutraal"
        bias = "Kansrijk" if best["score"] >= 60 else "Afwachten"

        summary = (
            f"Beste {asset}-setup vandaag: "
            f"{best['name']} ({best['score']}/100)."
        )

        # ==================================================
        # 5️⃣ AI CATEGORY INSIGHT — SETUP CARD
        # ==================================================
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW()
                """,
                (
                    user_id,
                    best["score"],
                    trend,
                    bias,
                    "Gemiddeld",
                    summary,
                    json.dumps(ranked[:3], ensure_ascii=False),
                ),
            )

        conn.commit()
        logger.info(f"✅ [Setup-Agent] Klaar (user_id={user_id})")

    except Exception:
        conn.rollback()
        logger.error("❌ Setup agent crash", exc_info=True)

    finally:
        conn.close()


# ======================================================
# 🧠 UITLEG PER SETUP (API)
# ======================================================

def generate_setup_explanation(setup_id: int, user_id: int) -> str:
    conn = get_db_connection()
    if not conn:
        return ""

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, symbol, strategy_type, description, action
                FROM setups
                WHERE id = %s AND user_id = %s
                """,
                (setup_id, user_id),
            )
            row = cur.fetchone()

        if not row:
            return ""

        name, symbol, strategy_type, description, action = row

        return ask_gpt_text(
            f"Leg kort uit waarom setup '{name}' ({symbol}) logisch is. "
            f"Strategie: {strategy_type}. Beschrijving: {description}. Actie: {action}."
        )

    except Exception:
        logger.error("❌ generate_setup_explanation fout", exc_info=True)
        return ""

    finally:
        conn.close()

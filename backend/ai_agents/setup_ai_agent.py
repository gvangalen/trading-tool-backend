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
# 🤖 SETUP AGENT — ADVIES-GERICHT
# ======================================================

def run_setup_agent(*, user_id: int, asset: str = "BTC"):
    """
    Doel:
    - Technisch: daily_setup_scores vullen
    - Advies: 1 duidelijke setup-aanbeveling per dag

    UI gebruikt:
    - ai_category_insights (category='setup')
    """

    if not user_id:
        raise ValueError("Setup agent vereist user_id")

    logger.info(f"🤖 [Setup-Agent] Start (user_id={user_id}, asset={asset})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # ==================================================
        # 1️⃣ Daily scores (MARKT CONTEXT)
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

        # Reset best
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
        best = None

        # ==================================================
        # 3️⃣ Evaluatie per setup (TECHNISCH)
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
            active = score > 0

            explanation = ask_gpt_text(
                f"Marktscore: macro {macro}, technical {technical}, market {market}. "
                f"Waarom past setup '{name}' vandaag wel of niet?"
            )

            evaluations.append({
                "setup_id": setup_id,
                "name": name,
                "score": score,
                "active": active,
                "reasons": {
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
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation,
                        created_at = NOW()
                    """,
                    (setup_id, user_id, score, active, explanation),
                )

            if active and (not best or score > best["score"]):
                best = {
                    "setup_id": setup_id,
                    "name": name,
                    "score": score,
                }

        # ==================================================
        # 4️⃣ Advies bepalen (DIT IS DE CARD)
        # ==================================================
        if best:
            advice = f"Actieve setup vandaag: {best['name']} ({best['score']}/100)."
            trend = "Actief"
            bias = "Kansrijk"
            top_signals = [
                f"Setup {best['name']} past bij huidige marktscore",
                "Scores binnen ingestelde ranges",
            ]

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
        else:
            advice = f"Geen actieve {asset}-setup vandaag. Afwachten."
            trend = "Neutraal"
            bias = "Afwachten"
            top_signals = [
                "Marktscore buiten setup-ranges",
                "Onvoldoende technische bevestiging",
            ]

        avg_score = round(sum(e["score"] for e in evaluations) / len(evaluations), 1)

        # ==================================================
        # 5️⃣ AI CATEGORY INSIGHT (SETUP)
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
                    avg_score,
                    trend,
                    bias,
                    "Gemiddeld",
                    advice,
                    json.dumps(top_signals, ensure_ascii=False),
                ),
            )

        conn.commit()
        logger.info(f"✅ [Setup-Agent] Advies opgeslagen (user_id={user_id})")

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

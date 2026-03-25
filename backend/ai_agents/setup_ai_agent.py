import logging
import json
from decimal import Decimal
from typing import Optional

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt
from backend.ai_core.agent_context import build_agent_context  # ✅ gedeelde context

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🔢 HELPERS
# ======================================================

def to_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def score_overlap(value, min_v, max_v) -> int:
    """
    Overlap-score (0–100)
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

    return round(100 - (abs(value - mid) / max_dist * 100))


# ======================================================
# 🤖 SETUP AI AGENT — MET GEHEUGEN
# ======================================================
def run_setup_agent(*, user_id: int, asset: str = "BTC"):

    if not user_id:
        raise ValueError("❌ Setup agent vereist user_id")

    logger.info(f"🤖 [Setup-Agent] Start (user_id={user_id}, asset={asset})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # ==================================================
        # 1️⃣ DAGELIJKSE MARKTCONTEXT
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                  AND user_id = %s
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen daily_scores gevonden")
            return

        macro, technical, market = map(to_float, row)

        # ==================================================
        # 2️⃣ SETUPS OPHALEN (🔥 NIEUWE STRUCTUUR)
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    name,
                    setup_type,
                    dca_frequency,
                    dca_day,
                    dca_month_day,
                    min_macro_score,
                    max_macro_score,
                    min_technical_score,
                    max_technical_score,
                    min_market_score,
                    max_market_score
                FROM setups
                WHERE user_id = %s
                  AND symbol = %s
                ORDER BY created_at DESC
            """, (user_id, asset))
            setups = cur.fetchall()

        if not setups:
            logger.info("ℹ️ Geen setups gevonden")
            return

        # ==================================================
        # 3️⃣ RESET BEST-FLAG
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_best = FALSE
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
            """, (user_id,))

        evaluations = []

        # ==================================================
        # 4️⃣ AI TASK
        # ==================================================
        SETUP_TASK = """
Je bent een trading decision agent.

Gebruik:
- macro / technical / market scores
- overlap-scores per setup
- setup_type (belangrijk!)
- context t.o.v. gisteren

Leg uit:
- of deze setup sterker / zwakker / gelijk is
- of dit rotatie is of continuatie
- waarom deze setup NU logisch is

GEEN:
- voorspellingen
- educatie

Output: 2–3 zinnen.
"""

        system_prompt = build_system_prompt(agent="setup", task=SETUP_TASK)

        # ==================================================
        # 5️⃣ PER SETUP
        # ==================================================
        for row in setups:
            (
                setup_id,
                name,
                setup_type,
                dca_frequency,
                dca_day,
                dca_month_day,
                min_macro,
                max_macro,
                min_tech,
                max_tech,
                min_market,
                max_market
            ) = row

            m  = score_overlap(macro, min_macro, max_macro)
            t  = score_overlap(technical, min_tech, max_tech)
            mk = score_overlap(market, min_market, max_market)

            raw_score = round((m + t + mk) / 3)
            score = max(25, raw_score)

            explanation = ask_gpt_text(
                prompt=json.dumps({
                    "setup": name,
                    "setup_type": setup_type,
                    "dca_config": {
                        "frequency": dca_frequency,
                        "day": dca_day,
                        "month_day": dca_month_day
                    },
                    "macro_score": macro,
                    "technical_score": technical,
                    "market_score": market,
                    "component_overlap": {
                        "macro": m,
                        "technical": t,
                        "market": mk
                    }
                }, ensure_ascii=False, indent=2),
                system_role=system_prompt
            )

            evaluations.append({
                "setup_id": setup_id,
                "name": name,
                "setup_type": setup_type,
                "score": score,
            })

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores
                        (setup_id, user_id, report_date, score, is_active, explanation)
                    VALUES (%s, %s, CURRENT_DATE, %s, TRUE, %s)
                    ON CONFLICT (setup_id, user_id, report_date)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        is_active = TRUE,
                        explanation = EXCLUDED.explanation,
                        created_at = NOW()
                """, (setup_id, user_id, score, explanation))

        # ==================================================
        # 6️⃣ BESTE SETUP
        # ==================================================
        ranked = sorted(evaluations, key=lambda x: x["score"], reverse=True)
        best = ranked[0]

        agent_context = build_agent_context(
            user_id=user_id,
            category="setup",
            current_score=best["score"],
            current_items=ranked[:3],
            lookback_days=1
        )

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_best = TRUE
                WHERE setup_id = %s
                  AND user_id = %s
                  AND report_date = CURRENT_DATE
            """, (best["setup_id"], user_id))

        # ==================================================
        # 7️⃣ SCORE OPSLAAN
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_scores
                SET setup_score = %s
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
            """, (best["score"], user_id))

        # ==================================================
        # 8️⃣ INSIGHT
        # ==================================================
        summary = f"Beste {asset}-setup: {best['name']} ({best['setup_type']})"

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    summary = EXCLUDED.summary,
                    created_at = NOW()
            """, (
                user_id,
                best["score"],
                "Actief" if best["score"] >= 60 else "Neutraal",
                "Kansrijk" if best["score"] >= 60 else "Afwachten",
                "Gemiddeld",
                summary,
                json.dumps([
                    f"{best['name']} beste match",
                    f"Type: {best['setup_type']}"
                ])
            ))

        conn.commit()
        logger.info("✅ Setup agent klaar")

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
            cur.execute("""
                SELECT
                    name,
                    symbol,
                    setup_type,
                    dca_frequency,
                    dca_day,
                    dca_month_day,
                    description,
                    action
                FROM setups
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return ""

        (
            name,
            symbol,
            setup_type,
            dca_frequency,
            dca_day,
            dca_month_day,
            description,
            action
        ) = row

        TASK = """
Leg kort uit waarom deze setup logisch is.
Gebruik setup_type en gedrag.
Geen educatie of voorspellingen.
"""

        system_prompt = build_system_prompt(agent="setup", task=TASK)

        return ask_gpt_text(
            prompt=json.dumps({
                "setup": name,
                "symbol": symbol,
                "setup_type": setup_type,
                "dca_config": {
                    "frequency": dca_frequency,
                    "day": dca_day,
                    "month_day": dca_month_day
                },
                "description": description,
                "action": action
            }, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

    except Exception:
        logger.error("❌ generate_setup_explanation fout", exc_info=True)
        return ""

    finally:
        conn.close()

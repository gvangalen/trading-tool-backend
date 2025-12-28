import logging
import json
from decimal import Decimal
from typing import Optional

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt

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
    - NULL min/max = geen filter = 100
    - Buiten range = 0
    - Binnen range = relatieve score
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
# 🤖 SETUP AI AGENT — BESLISLOGICA + AI CONTEXT
# ======================================================

def run_setup_agent(*, user_id: int, asset: str = "BTC"):
    """
    Doel:
    - daily_setup_scores vullen (per setup)
    - 1 duidelijke setup-aanbeveling bepalen
    - ai_category_insights (category='setup')
    - setup_score wegschrijven naar daily_scores
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
            logger.warning("⚠️ Geen daily_scores gevonden — setup agent stopt")
            return

        macro, technical, market = map(to_float, row)

        # ==================================================
        # 2️⃣ SETUPS OPHALEN
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    name,
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
        # 4️⃣ PER SETUP: SCORE + AI UITLEG
        # ==================================================
        SETUP_TASK = """
Leg uit of een setup logisch past bij de huidige marktscores.

Gebruik:
- macro score
- technical score
- market score

Doel:
- verklaren waarom de setup geschikt of ongeschikt is
- geen voorspellingen
- geen nieuwe data
"""

        system_prompt = build_system_prompt(
            agent="setup",
            task=SETUP_TASK
        )

        for row in setups:
            setup_id, name, min_macro, max_macro, min_tech, max_tech, min_market, max_market = row

            m  = score_overlap(macro, min_macro, max_macro)
            t  = score_overlap(technical, min_tech, max_tech)
            mk = score_overlap(market, min_market, max_market)

            raw_score = round((m + t + mk) / 3)
            score = max(25, raw_score)  # 🔒 FLOOR

            explanation = ask_gpt_text(
                prompt=(
                    f"Setup: {name}\n"
                    f"Macro score: {macro}\n"
                    f"Technical score: {technical}\n"
                    f"Market score: {market}\n"
                    f"Component scores: macro={m}, technical={t}, market={mk}"
                ),
                system_role=system_prompt
            )

            evaluations.append({
                "setup_id": setup_id,
                "name": name,
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
        # 5️⃣ BESTE SETUP
        # ==================================================
        ranked = sorted(evaluations, key=lambda x: x["score"], reverse=True)
        best = ranked[0]

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_setup_scores
                SET is_best = TRUE
                WHERE setup_id = %s
                  AND user_id = %s
                  AND report_date = CURRENT_DATE
            """, (best["setup_id"], user_id))

        # ==================================================
        # 🔥 5b️⃣ SETUP SCORE → DAILY_SCORES
        # ==================================================
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_scores
                SET setup_score = %s
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
            """, (best["score"], user_id))

        # ==================================================
        # 6️⃣ AI CATEGORY INSIGHT — DASHBOARD
        # ==================================================
        trend = "Actief" if best["score"] >= 60 else "Neutraal"
        bias  = "Kansrijk" if best["score"] >= 60 else "Afwachten"

        summary = f"Beste {asset}-setup vandaag: {best['name']}."

        top_signals = [
            f"{best['name']} sluit het best aan bij huidige marktscores",
            "Technische en macrocontext beperken agressie",
            "Relatief hoogste setup-score vandaag",
        ]

        with conn.cursor() as cur:
            cur.execute("""
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
            """, (
                user_id,
                best["score"],
                trend,
                bias,
                "Gemiddeld",
                summary,
                json.dumps(top_signals, ensure_ascii=False),
            ))

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
            cur.execute("""
                SELECT name, symbol, strategy_type, description, action
                FROM setups
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return ""

        name, symbol, strategy_type, description, action = row

        TASK = """
Leg beknopt uit waarom deze setup logisch is.
Geen educatie, geen hype, geen voorspellingen.
"""

        system_prompt = build_system_prompt(
            agent="setup",
            task=TASK
        )

        return ask_gpt_text(
            prompt=(
                f"Setup: {name} ({symbol})\n"
                f"Strategie: {strategy_type}\n"
                f"Beschrijving: {description}\n"
                f"Actie: {action}"
            ),
            system_role=system_prompt
        )

    except Exception:
        logger.error("❌ generate_setup_explanation fout", exc_info=True)
        return ""

    finally:
        conn.close()

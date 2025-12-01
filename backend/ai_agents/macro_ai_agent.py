import logging
import traceback
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🌍 MACRO AI AGENT — FIXED (range_min / range_max)
# ======================================================

@shared_task(name="backend.ai_agents.macro_ai_agent.generate_macro_insight")
def generate_macro_insight():
    """
    Macro AI Agent (V2)
    - Haalt macro scoreregels op (range_min/range_max)
    - Haalt macro_data van vandaag op
    - Laat AI context + reflecties genereren
    - Slaat op in ai_category_insights & ai_reflections
    """

    logger.info("🌍 Start Macro AI Agent (V2)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =====================================================
        # 1️⃣ Regels ophalen per macro-indicator (FIXED)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator ASC, score ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, interpretation, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "interpretation": interpretation,
                "action": action,
            })

        logger.info(f"📘 Macro-regels geladen voor {len(rules_by_indicator)} indicatoren.")

        # =====================================================
        # 2️⃣ Macro-data van vandaag ophalen
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg
                FROM macro_data
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("⚠️ Geen macro_data gevonden voor vandaag.")
            return

        combined = []
        for (ind, value, score, advies, uitleg) in rows:
            combined.append({
                "indicator": ind,
                "value": value,
                "score": score,
                "advies": advies,
                "uitleg": uitleg,
                "rules": rules_by_indicator.get(ind, []),
            })

        # Text voor AI prompt
        data_text = "\n".join([
            f"{c['indicator']}: value={c['value']}, score={c['score']}, advies={c['advies']}, rules={json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined
        ])

        # =====================================================
        # 3️⃣ AI: Macro-interpretatie
        # =====================================================
        prompt_context = f"""
Je bent een macro-economische analyse-AI gespecialiseerd in Bitcoin.

Hieronder staan de actuele macro-indicatoren en hun scoreregels:

{data_text}

Geef antwoord als JSON:
- trend: bullish | bearish | neutraal
- bias: risk-on | risk-off | gemengd
- risk: laag | gemiddeld | hoog
- summary: max 2 zinnen
- top_signals: lijst (tekst)
"""

        ai_context = ask_gpt(
            prompt_context,
            system_role="Je bent een professionele macro-economie AI. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            logger.warning("⚠️ AI-context was geen dict – fallback.")
            txt = ai_context.get("raw_text", "")[:300] if isinstance(ai_context, dict) else str(ai_context)
            ai_context = {
                "trend": None,
                "bias": None,
                "risk": None,
                "summary": txt,
                "top_signals": [],
            }

        # =====================================================
        # 4️⃣ AI: Reflecties per indicator
        # =====================================================
        prompt_reflection = f"""
Je bent dezelfde Macro-AI.

Hieronder alle indicatoren:
{data_text}

Genereer een JSON-lijst met entries:
- indicator
- ai_score
- compliance
- comment
- recommendation
"""

        ai_reflections = ask_gpt(
            prompt_reflection,
            system_role="Je bent een professionele macro-analist. Geef JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            logger.warning("⚠️ Reflecties niet in lijstvorm – fallback.")
            ai_reflections = []

        logger.info(f"🧠 Macro interpretatie: {ai_context}")
        logger.info(f"🪞 Reflecties: {len(ai_reflections)} items")

        # =====================================================
        # 5️⃣ Opslaan categorie-samenvatting
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', NULL, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date)
                DO UPDATE SET
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("risk"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # =====================================================
        # 6️⃣ Opslaan reflecties
        # =====================================================
        for r in ai_reflections:
            ind = r.get("indicator")
            if not ind:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (
                        category, indicator, raw_score, ai_score, compliance, comment, recommendation
                    )
                    VALUES ('macro', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (
                    ind,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info("✅ Macro AI insights + reflecties opgeslagen.")

    except Exception:
        logger.error("❌ Fout in Macro AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

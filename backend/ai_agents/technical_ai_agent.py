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
# 📊 TECHNICAL AI AGENT — FIXED (range_min / range_max)
# ======================================================
@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight():
    logger.info("📊 Start Technical AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ------------------------------------------------------
        # 1️⃣ Technische scoreregels ophalen (FIXED)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, interpretation, action
                FROM technical_indicator_rules
                ORDER BY indicator ASC, score ASC
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, interpretation, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "interpretation": interpretation,
                "action": action
            })

        logger.info(f"📘 {len(rules_by_indicator)} technische indicatortypes geladen.")

        # ------------------------------------------------------
        # 2️⃣ Technische indicatorwaarden ophalen
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg
                FROM technical_indicators
                WHERE date = CURRENT_DATE
                ORDER BY indicator ASC
            """)
            data_rows = cur.fetchall()

        if not data_rows:
            logger.warning("⚠️ Geen technical_indicators gevonden voor vandaag.")
            return

        # ------------------------------------------------------
        # 3️⃣ Combineer data + scoreregels → AI prompt text
        # ------------------------------------------------------
        combined = []
        for ind, val, score, advies, uitleg in data_rows:
            combined.append({
                "indicator": ind,
                "value": val,
                "score": score,
                "advies": advies,
                "uitleg": uitleg,
                "rules": rules_by_indicator.get(ind, [])
            })

        data_text = "\n".join([
            f"{c['indicator']} → value={c['value']} | score={c['score']} | advies={c['advies']} "
            f"| rules={json.dumps(c['rules'], ensure_ascii=False)}"
            for c in combined
        ])

        # ------------------------------------------------------
        # 4️⃣ AI Context (trend / bias / momentum)
        # ------------------------------------------------------
        prompt_context = f"""
Je bent een technische analyse AI gespecialiseerd in Bitcoin.

Hieronder staan de technische indicatoren + hun scoreregels:
{data_text}

Geef antwoord als geldige JSON:
- trend: bullish | bearish | neutraal
- bias: short-term | long-term
- momentum: sterk | neutraal | zwak
- summary: max 2 zinnen
- top_signals: lijst met belangrijkste indicatoren
"""

        ai_context = ask_gpt(prompt_context)
        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": None,
                "bias": None,
                "momentum": None,
                "summary": str(ai_context)[:200],
                "top_signals": []
            }

        # ------------------------------------------------------
        # 5️⃣ AI Reflecties per indicator
        # ------------------------------------------------------
        prompt_reflection = f"""
Beoordeel onderstaande indicatoren:

{data_text}

Geef een JSON-lijst, bv:
[
  {{
    "indicator": "RSI",
    "ai_score": 70,
    "compliance": 85,
    "comment": "RSI daalt uit overbought-zone",
    "recommendation": "Wacht op RSI < 50"
  }}
]
"""

        ai_reflections = ask_gpt(prompt_reflection)
        if not isinstance(ai_reflections, list):
            ai_reflections = []

        logger.info(f"🧠 AI Technical Context: {ai_context}")
        logger.info(f"🪞 Reflecties: {len(ai_reflections)} items")

        # ------------------------------------------------------
        # 6️⃣ Opslaan categorie-samenvatting (ai_category_insights)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights 
                    (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('technical', NULL, %s, %s, NULL, %s, %s)
                ON CONFLICT (category, date) DO UPDATE
                SET trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW()
            """, (
                ai_context.get("trend"),
                ai_context.get("bias"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # ------------------------------------------------------
        # 7️⃣ Opslaan individuele reflecties (ai_reflections)
        # ------------------------------------------------------
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (
                        category, indicator, raw_score, ai_score, compliance, comment, recommendation
                    )
                    VALUES ('technical', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW()
                """, (
                    indicator,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info("✅ Technical AI Agent voltooid.")

    except Exception:
        logger.error("❌ Fout in Technical AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

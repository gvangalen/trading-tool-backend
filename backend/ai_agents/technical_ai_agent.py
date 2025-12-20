import logging
import traceback
import json

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================================
# 📊 TECHNICAL AI AGENT — USER-AWARE (FINAL)
# =====================================================================

@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight(user_id: int):

    if user_id is None:
        raise ValueError("❌ Technical AI Agent vereist een user_id")

    logger.info(f"📊 Start Technical AI Agent — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ------------------------------------------------------
        # 1️⃣ Scoreregels (GLOBAAL)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM technical_indicator_rules
                ORDER BY indicator ASC, range_min ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for indicator, rmin, rmax, score, trend, interp, action in rule_rows:
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Technical scoreregels geladen ({len(rule_rows)} regels)")

        # ------------------------------------------------------
        # 2️⃣ Laatste technische indicatoren (PER USER)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, advies, uitleg, timestamp
                FROM technical_indicators
                WHERE user_id = %s
                ORDER BY indicator ASC, timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen technische data gevonden voor user_id={user_id}")
            return

        latest = {}
        for name, value, score, advies, uitleg, ts in rows:
            if name not in latest:
                latest[name] = (value, score, advies, uitleg, ts)

        combined = []
        scores = []

        for name, (value, score, advies, uitleg, ts) in latest.items():
            score_f = float(score) if score is not None else 50  # defensieve fallback

            combined.append({
                "indicator": name,
                "value": float(value) if value is not None else None,
                "score": score_f,
                "trend": next(
                    (r["trend"] for r in rules_by_indicator.get(name, []) if r["score"] == score_f),
                    None
                ),
                "advies": advies or "",
                "uitleg": uitleg or "",
                "timestamp": ts.isoformat(),
                "rules": rules_by_indicator.get(name, [])
            })
            scores.append(score_f)

        avg_score = round(sum(scores) / len(scores), 2)

        # ------------------------------------------------------
        # 3️⃣ AI CONTEXT
        # ------------------------------------------------------
        prompt = f"""
Je bent een professionele technische analyse expert.

Analyseer onderstaande technische indicatoren en geef een samenvattend oordeel.

DATA:
{json.dumps(combined, ensure_ascii=False, indent=2)}

ANTWOORD ALLEEN GELDIGE JSON:
{{
  "trend": "",
  "bias": "",
  "risk": "",
  "momentum": "",
  "summary": "",
  "top_signals": []
}}
"""

        ai_context = ask_gpt(
            prompt,
            system_role="Je bent een technische analyse expert. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            raise ValueError("❌ Technical AI response is geen geldige JSON")

        # ------------------------------------------------------
        # 4️⃣ AI REFLECTIES
        # ------------------------------------------------------
        prompt_reflections = f"""
Maak reflecties per technische indicator.

DATA:
{json.dumps(combined, ensure_ascii=False, indent=2)}

ANTWOORD ALS JSON-LIJST:
[
  {{
    "indicator": "",
    "ai_score": 0,
    "compliance": 0,
    "comment": "",
    "recommendation": ""
  }}
]
"""

        ai_reflections = ask_gpt(
            prompt_reflections,
            system_role="Je bent een technische analyse expert. Antwoord uitsluitend in geldige JSON."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # ------------------------------------------------------
        # 5️⃣ Opslaan ai_category_insights
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES
                    ('technical', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score   = EXCLUDED.avg_score,
                    trend       = EXCLUDED.trend,
                    bias        = EXCLUDED.bias,
                    risk        = EXCLUDED.risk,
                    summary     = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at  = NOW();
            """, (
                user_id,
                avg_score,
                ai_context.get("trend", ""),
                ai_context.get("bias", ""),
                ai_context.get("risk", ""),
                ai_context.get("summary", ""),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # ------------------------------------------------------
        # 6️⃣ Opslaan ai_reflections
        # ------------------------------------------------------
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES
                        ('technical', %s, %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, indicator, date)
                    DO UPDATE SET
                        ai_score       = EXCLUDED.ai_score,
                        compliance     = EXCLUDED.compliance,
                        comment        = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp      = NOW();
                """, (
                    user_id,
                    indicator,
                    r.get("ai_score"),
                    r.get("compliance"),
                    r.get("comment"),
                    r.get("recommendation"),
                ))

        conn.commit()
        logger.info(f"✅ Technical AI Agent voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Technical AI Agent FOUT", exc_info=True)

    finally:
        conn.close()

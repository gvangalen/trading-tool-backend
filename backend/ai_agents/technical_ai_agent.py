import logging
import traceback
import json
from datetime import datetime
from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================================
# 📊 TECHNICAL AI AGENT — USER-AWARE (FINAL)
# =====================================================================
@shared_task(name="backend.ai_agents.technical_ai_agent.generate_technical_insight")
def generate_technical_insight(user_id: int | None = None):
    logger.info(f"📊 Start Technical AI Agent — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ------------------------------------------------------
        # 1️⃣ Scoreregels ophalen (globaal)
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, interpretation, action
                FROM technical_indicator_rules
                ORDER BY indicator ASC, range_min ASC
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for i, rmin, rmax, score, interp, action in rule_rows:
            rules_by_indicator.setdefault(i, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "interpretation": interp,
                "action": action
            })

        logger.info(f"📘 Scoreregels geladen voor {len(rule_rows)} rows.")

        # ------------------------------------------------------
        # 2️⃣ Technische data ophalen per gebruiker
        # ------------------------------------------------------
        with conn.cursor() as cur:

            if user_id is not None:
                cur.execute("""
                    SELECT indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    WHERE user_id=%s
                    ORDER BY indicator ASC, timestamp DESC;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT indicator, value, score, advies, uitleg, timestamp
                    FROM technical_indicators
                    ORDER BY indicator ASC, timestamp DESC;
                """)

            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen technische indicatoren gevonden (user_id={user_id})")
            return

        # Combine values: neem alleen de laatste value per indicator
        latest = {}
        for name, value, score, advies, uitleg, ts in rows:
            if name not in latest:
                latest[name] = (value, score, advies, uitleg, ts)

        combined = []
        score_list = []

        for name, (value, score, advies, uitleg, ts) in latest.items():
            score_float = float(score)
            combined.append({
                "indicator": name,
                "value": float(value),
                "score": score_float,
                "advies": advies or "",
                "uitleg": uitleg or "",
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "rules": rules_by_indicator.get(name, [])
            })
            score_list.append(score_float)

        avg_score = round(sum(score_list) / len(score_list), 2)

        # ------------------------------------------------------
        # 3️⃣ Bouw AI context prompt
        # ------------------------------------------------------
        data_text = "\n".join([
            f"{c['indicator']}: value={c['value']}, score={c['score']}, advies={c['advies']}"
            for c in combined
        ])

        prompt = f"""
Je bent een professionele technische analyse AI.

Hier zijn de technische indicatoren + scoreregels:

{json.dumps(combined, ensure_ascii=False, indent=2)}

Geef een geldige JSON terug:
{{
  "trend": "",
  "bias": "",
  "momentum": "",
  "summary": "",
  "top_signals": []
}}
"""

        ai_context = ask_gpt(
            prompt,
            system_role="Je bent een technische analyse expert. Antwoord ALTIJD in geldige JSON."
        )

        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": "",
                "bias": "",
                "momentum": "",
                "summary": "",
                "top_signals": []
            }

        # ------------------------------------------------------
        # 4️⃣ Reflecties prompt
        # ------------------------------------------------------
        prompt_reflections = f"""
Maak een JSON-lijst met reflecties per indicator.

Elk object:
{{
  "indicator": "",
  "ai_score": 0,
  "compliance": 0,
  "comment": "",
  "recommendation": ""
}}

Indicatoren:
{json.dumps(combined, ensure_ascii=False, indent=2)}
"""

        ai_reflections = ask_gpt(
            prompt_reflections,
            system_role="Je bent een technische analyse expert. Geef een geldige JSON-lijst."
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # ------------------------------------------------------
        # 5️⃣ Opslaan in ai_category_insights
        # ------------------------------------------------------
        with conn.cursor() as cur:
            if user_id is not None:
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, user_id, avg_score, trend, bias, summary, top_signals)
                    VALUES ('technical', %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, date)
                    DO UPDATE SET
                        avg_score=EXCLUDED.avg_score,
                        trend=EXCLUDED.trend,
                        bias=EXCLUDED.bias,
                        summary=EXCLUDED.summary,
                        top_signals=EXCLUDED.top_signals,
                        created_at=NOW();
                """, (
                    user_id,
                    avg_score,
                    ai_context["trend"],
                    ai_context["bias"],
                    ai_context["summary"],
                    json.dumps(ai_context["top_signals"])
                ))

            else:
                # Backwards compatible
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, avg_score, trend, bias, summary, top_signals)
                    VALUES ('technical', %s, %s, %s, %s, %s)
                    ON CONFLICT (category, date)
                    DO UPDATE SET
                        avg_score=EXCLUDED.avg_score,
                        trend=EXCLUDED.trend,
                        bias=EXCLUDED.bias,
                        summary=EXCLUDED.summary,
                        top_signals=EXCLUDED.top_signals,
                        created_at=NOW();
                """, (
                    avg_score,
                    ai_context["trend"],
                    ai_context["bias"],
                    ai_context["summary"],
                    json.dumps(ai_context["top_signals"])
                ))

        # ------------------------------------------------------
        # 6️⃣ Reflecties opslaan in ai_reflections
        # ------------------------------------------------------
        for item in ai_reflections:
            indicator = item.get("indicator")
            if not indicator:
                continue

            with conn.cursor() as cur:
                if user_id is not None:
                    cur.execute("""
                        INSERT INTO ai_reflections
                            (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                        VALUES ('technical', %s, %s, NULL, %s, %s, %s, %s)
                        ON CONFLICT (category, user_id, indicator, date)
                        DO UPDATE SET
                            ai_score=EXCLUDED.ai_score,
                            compliance=EXCLUDED.compliance,
                            comment=EXCLUDED.comment,
                            recommendation=EXCLUDED.recommendation,
                            timestamp=NOW();
                    """, (
                        user_id,
                        indicator,
                        item.get("ai_score"),
                        item.get("compliance"),
                        item.get("comment"),
                        item.get("recommendation")
                    ))
                else:
                    cur.execute("""
                        INSERT INTO ai_reflections
                            (category, indicator, raw_score, ai_score, compliance, comment, recommendation)
                        VALUES ('technical', %s, NULL, %s, %s, %s, %s)
                        ON CONFLICT (category, indicator, date)
                        DO UPDATE SET
                            ai_score=EXCLUDED.ai_score,
                            compliance=EXCLUDED.compliance,
                            comment=EXCLUDED.comment,
                            recommendation=EXCLUDED.recommendation,
                            timestamp=NOW();
                    """, (
                        indicator,
                        item.get("ai_score"),
                        item.get("compliance"),
                        item.get("comment"),
                        item.get("recommendation")
                    ))

        conn.commit()
        logger.info(f"✅ Technical AI insights + reflecties opgeslagen (user_id={user_id})")

    except Exception:
        logger.error("❌ Technical AI Agent error:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

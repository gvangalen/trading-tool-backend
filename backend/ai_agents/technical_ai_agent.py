import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import normalize_indicator_name

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================================
# 📊 TECHNICAL AI AGENT — PURE LOGICA (NO CELERY) + DEDUP FIX
# =====================================================================

def run_technical_agent(user_id: int):
    """
    Genereert technical AI insights voor één user.
    Schrijft:
    - ai_category_insights (category='technical')
    - ai_reflections (category='technical')
    """

    if user_id is None:
        raise ValueError("❌ Technical AI Agent vereist een user_id")

    logger.info(f"📊 [Technical-Agent] Start — user_id={user_id}")

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
            key = normalize_indicator_name(indicator)
            rules_by_indicator.setdefault(key, []).append({
                "range_min": float(rmin) if rmin is not None else None,
                "range_max": float(rmax) if rmax is not None else None,
                "score": int(score) if score is not None else None,
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Technical scoreregels geladen ({len(rule_rows)} regels)")

        # ------------------------------------------------------
        # 2️⃣ Laatste technische indicatoren (PER USER)
        # ✅ FIX: alleen indicators die in indicators-tabel als technical+active staan
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ti.indicator, ti.value, ti.score, ti.advies, ti.uitleg, ti.timestamp
                FROM technical_indicators ti
                JOIN indicators i
                  ON i.name = ti.indicator
                 AND i.category = 'technical'
                 AND i.active = TRUE
                WHERE ti.user_id = %s
                ORDER BY ti.indicator ASC, ti.timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            logger.warning(f"⚠️ Geen technische data gevonden voor user_id={user_id}")
            return

        # Dedup: alleen de nieuwste per indicator (normalised key)
        latest = {}
        for name, value, score, advies, uitleg, ts in rows:
            key = normalize_indicator_name(name)
            if key not in latest:
                latest[key] = (name, value, score, advies, uitleg, ts)

        combined = []
        scores = []

        for key, (name, value, score, advies, uitleg, ts) in latest.items():
            score_f = float(score) if score is not None else 50.0

            rule_trend = next(
                (r["trend"] for r in rules_by_indicator.get(key, []) if float(r.get("score") or -1) == score_f),
                None
            )

            combined.append({
                "indicator": normalize_indicator_name(name),
                "value": float(value) if value is not None else None,
                "score": score_f,
                "trend": rule_trend,
                "advies": advies or "",
                "uitleg": uitleg or "",
                "timestamp": ts.isoformat() if ts else None,
                "rules": rules_by_indicator.get(key, [])
            })
            scores.append(score_f)

        avg_score = round(sum(scores) / len(scores), 2) if scores else 50.0

        # ------------------------------------------------------
        # 3️⃣ AI CONTEXT (samenvatting)
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
        # 4️⃣ AI REFLECTIES (per indicator)
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
        # 🧹 FIX: verwijder oude technical reflecties van vandaag
        # ------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM ai_reflections
                WHERE category = 'technical'
                  AND user_id = %s
                  AND date = CURRENT_DATE;
            """, (user_id,))

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
        # 6️⃣ Opslaan ai_reflections (per indicator, 1 per dag)
        # ------------------------------------------------------
        for r in ai_reflections:
            indicator = r.get("indicator")
            if not indicator:
                continue

            indicator_norm = normalize_indicator_name(indicator)

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
                    indicator_norm,
                    r.get("ai_score", 50),
                    r.get("compliance", 50),
                    r.get("comment", ""),
                    r.get("recommendation", ""),
                ))

        conn.commit()
        logger.info(f"✅ [Technical-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Technical-Agent] FOUT", exc_info=True)

    finally:
        conn.close()

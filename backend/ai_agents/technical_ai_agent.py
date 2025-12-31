import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================================
# 📊 TECHNICAL AI AGENT — ORIGINEEL + MINIMALE FIX
# =====================================================================
def run_technical_agent(user_id: int):
    """
    Genereert technical AI insights voor één user.

    Schrijft:
    - ai_category_insights (category='technical')
    - ai_reflections (category='technical')

    ✔ 7 stappen (zoals origineel)
    ✔ technical_indicator_rules blijven leidend
    ✔ AI = interpretatie
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
        # 1️⃣ TECHNICAL SCOREREGELS
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
        # 2️⃣ LAATSTE TECHNICAL DATA
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

        # ------------------------------------------------------
        # 3️⃣ DEDUP — LAATSTE METING PER INDICATOR
        # ------------------------------------------------------
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
                (r["trend"] for r in rules_by_indicator.get(key, []) if r.get("score") == int(score_f)),
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
                "rules": rules_by_indicator.get(key, []),
            })

            scores.append(score_f)

        avg_score = round(sum(scores) / len(scores), 2) if scores else 50.0

        # ------------------------------------------------------
        # 4️⃣ AI TECHNICAL CONTEXT
        # ------------------------------------------------------
        TECHNICAL_TASK = """
Analyseer technische indicatoren voor Bitcoin.

Gebruik uitsluitend:
- indicatorwaarden
- scores
- trends
- uitleg en advies

Geef altijd:
- trend
- bias
- risico
- momentum
- korte samenvatting
- belangrijkste technische signalen

Antwoord uitsluitend in geldige JSON.
"""

        system_prompt = build_system_prompt(agent="technical", task=TECHNICAL_TASK)

        ai_context = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(ai_context, dict):
            raise ValueError("❌ Technical AI response is geen geldige JSON")

        # 🔧 🔥 ENIGE FIX T.O.V. OUDE FILE
        if "analysis" in ai_context and isinstance(ai_context["analysis"], dict):
            ai_context = ai_context["analysis"]

        # ------------------------------------------------------
        # 5️⃣ AI REFLECTIES (PER INDICATOR)
        # ------------------------------------------------------
        REFLECTION_TASK = """
Maak reflecties per technische indicator.

Per indicator:
- indicator
- ai_score (0–100)
- compliance (0–100)
- korte comment
- concrete aanbeveling

Antwoord uitsluitend als JSON-lijst.
"""

        reflection_prompt = build_system_prompt(agent="technical", task=REFLECTION_TASK)

        ai_reflections = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=reflection_prompt
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # ------------------------------------------------------
        # 6️⃣ OPSLAAN AI_CATEGORY_INSIGHTS
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
        # 7️⃣ OPSLAAN AI_REFLECTIONS
        # ------------------------------------------------------
        for r in ai_reflections:
            if not r.get("indicator"):
                continue

            indicator_norm = normalize_indicator_name(r.get("indicator"))

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
        raise

    finally:
        conn.close()

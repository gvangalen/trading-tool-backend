import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db, normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🧠 Helpers
# ======================================================
def is_empty_macro_context(ctx: dict) -> bool:
    """
    Checkt of de AI wel inhoud heeft gegenereerd
    (niet alleen geldige JSON, maar ook betekenis)
    """
    if not isinstance(ctx, dict):
        return True

    return not any([
        ctx.get("summary"),
        ctx.get("trend"),
        ctx.get("bias"),
        ctx.get("risk"),
        ctx.get("top_signals"),
    ])


def fallback_macro_context(macro_items: list) -> dict:
    """
    Veilige fallback bij te weinig AI-output
    """
    indicators = {i["indicator"] for i in macro_items}

    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "summary": (
            "De macro-analyse is gebaseerd op een beperkt aantal indicatoren. "
            "BTC-dominantie en marktsentiment geven richting, maar vragen om "
            "voorzichtigheid en bevestiging."
        ),
        "top_signals": [
            f"{ind} blijft richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte macrodata beschikbaar"],
    }


# ======================================================
# 🌍 MACRO AI AGENT — PURE AI + DB LOGICA
# ======================================================
def run_macro_agent(user_id: int):
    """
    Genereert macro AI insights voor één gebruiker.

    Schrijft:
    - ai_category_insights (macro)
    - ai_reflections (macro)

    ❗️Geen Celery
    ❗️Geen data-ingestie
    """

    if user_id is None:
        raise ValueError("❌ Macro AI Agent vereist een user_id")

    logger.info(f"🌍 [Macro-Agent] Start voor user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # =========================================================
        # 1️⃣ Macro scoreregels (globaal)
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM macro_indicator_rules
                ORDER BY indicator, range_min;
            """)
            rows = cur.fetchall()

        rules_by_indicator = {}
        for ind, rmin, rmax, score, trend, interp, action in rows:
            ind_norm = normalize_indicator_name(ind)
            rules_by_indicator.setdefault(ind_norm, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        # =========================================================
        # 2️⃣ Macro data — LAATSTE SNAPSHOT
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, value, trend, interpretation, action, score, timestamp
                FROM macro_data
                WHERE user_id = %s
                ORDER BY timestamp DESC
                LIMIT 50;
            """, (user_id,))
            macro_rows = cur.fetchall()

        if not macro_rows:
            logger.info(f"ℹ️ [Macro-Agent] Geen macro_data beschikbaar (user_id={user_id})")
            return

        macro_items = [
            {
                "indicator": normalize_indicator_name(name),
                "value": float(value) if value is not None else None,
                "trend": trend,
                "interpretation": interp,
                "action": action,
                "score": float(score) if score is not None else None,
                "timestamp": ts.isoformat() if ts else None,
            }
            for name, value, trend, interp, action, score, ts in macro_rows
        ]

        # =========================================================
        # 3️⃣ Macro score (DB-gedreven)
        # =========================================================
        macro_scores = generate_scores_db("macro", user_id=user_id)
        macro_avg = macro_scores.get("total_score", 10)

        top_contributors = sorted(
            macro_scores.get("scores", {}).items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True
        )[:3]

        top_pretty = [
            {
                "indicator": k,
                "value": v.get("value"),
                "score": v.get("score"),
                "trend": v.get("trend"),
                "interpretation": v.get("interpretation"),
            }
            for k, v in top_contributors
        ]

        # =========================================================
        # 4️⃣ AI MACRO SAMENVATTING
        # =========================================================
        payload = {
            "macro_items": macro_items,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "top_contributors": top_pretty,
        }

        macro_task = """
Analyseer de beschikbare macrodata in beslistermen voor Bitcoin.

Belangrijk:
- Gebruik uitsluitend de indicatoren die aanwezig zijn
- Ook bij weinig data moet je een concrete analyse geven
- Vermijd lege antwoorden of 'onvoldoende data'

Geef altijd:
- trend (bullish / bearish / neutraal)
- bias (positief / negatief / neutraal)
- risico (laag / gemiddeld / hoog)
- korte samenvatting (minstens 1 zin)
- belangrijkste macro-signalen (minstens 1 punt)

Antwoord uitsluitend in geldige JSON.
"""

        system_prompt = build_system_prompt(
            agent="macro",
            task=macro_task
        )

        ai_context = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(ai_context, dict):
            raise ValueError("❌ Macro AI response geen geldige JSON")

        # 🛟 Inhoudelijke fallback
        if is_empty_macro_context(ai_context):
            logger.warning("⚠️ Macro AI gaf lege inhoud → fallback toegepast")
            ai_context = fallback_macro_context(macro_items)

        # =========================================================
        # 5️⃣ AI REFLECTIES PER INDICATOR
        # =========================================================
        reflections_task = """
Maak per macro-indicator een reflectie.

Per item:
- ai_score (0–100)
- compliance (0–100)
- korte comment
- concrete aanbeveling

Antwoord uitsluitend als JSON-lijst.
"""

        reflections_prompt = build_system_prompt(
            agent="macro",
            task=reflections_task
        )

        ai_reflections = ask_gpt(
            prompt=json.dumps(macro_items, ensure_ascii=False, indent=2),
            system_role=reflections_prompt
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 6️⃣ Opslaan ai_category_insights
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('macro', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                macro_avg,
                ai_context.get("trend", ""),
                ai_context.get("bias", ""),
                ai_context.get("risk", ""),
                ai_context.get("summary", ""),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # =========================================================
        # 7️⃣ Opslaan ai_reflections
        # =========================================================
        for r in ai_reflections:
            if not r.get("indicator"):
                continue

            indicator = normalize_indicator_name(r.get("indicator"))

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('macro', %s, %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (
                    user_id,
                    indicator,
                    r.get("ai_score", 50),
                    r.get("compliance", 50),
                    r.get("comment", ""),
                    r.get("recommendation", ""),
                ))

        conn.commit()
        logger.info(f"✅ [Macro-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Macro-Agent] Fout", exc_info=True)

    finally:
        conn.close()

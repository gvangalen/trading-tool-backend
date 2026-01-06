import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db, normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt
from backend.ai_core.agent_context import build_agent_context  # ✅ gedeelde context

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🧠 Helpers
# ======================================================
def is_empty_macro_context(ctx: dict) -> bool:
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
    indicators = {i["indicator"] for i in macro_items}

    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "summary": (
            "De macro-analyse is gebaseerd op een beperkt aantal indicatoren. "
            "De huidige signalen vragen om bevestiging voordat er agressieve "
            "positionering logisch wordt."
        ),
        "top_signals": [
            f"{ind} blijft richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte macrodata beschikbaar"],
    }


def normalize_ai_context(ai_ctx: dict, macro_items: list) -> dict:
    """
    ✅ Unwrap-fix:
    accepteert geneste AI-responses zoals:
    { analysis: {...} } / { analyse: {...} } / { context: {...} }
    """

    if not isinstance(ai_ctx, dict):
        return fallback_macro_context(macro_items)

    # 🔧 unwrap bekende wrappers
    for key in ("analysis", "analyse", "context"):
        if key in ai_ctx and isinstance(ai_ctx[key], dict):
            ai_ctx = ai_ctx[key]
            break

    normalized = {
        "trend": ai_ctx.get("trend", ""),
        "bias": ai_ctx.get("bias", ""),
        "risk": ai_ctx.get("risk") or ai_ctx.get("risico", ""),
        "summary": ai_ctx.get("summary") or ai_ctx.get("samenvatting", ""),
        "top_signals": ai_ctx.get("top_signals", []),
    }

    if is_empty_macro_context(normalized):
        logger.warning("⚠️ Macro AI gaf lege inhoud → fallback toegepast")
        return fallback_macro_context(macro_items)

    return normalized


# ======================================================
# 🌍 MACRO AI AGENT
# ======================================================
def run_macro_agent(user_id: int):
    """
    Genereert macro AI insights voor één gebruiker.

    Schrijft:
    - ai_category_insights (macro)
    - ai_reflections (macro)

    ✔ gedeelde agent-context
    ✔ tijdsbewust (t.o.v. gisteren)
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
        # 1️⃣ Macro scoreregels
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
        # 2️⃣ Macro data (laatste snapshot)
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
            logger.info(f"ℹ️ [Macro-Agent] Geen macro_data (user_id={user_id})")
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
        # 3️⃣ Macro score
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
        # 4️⃣ 🧠 SHARED AGENT CONTEXT
        # =========================================================
        agent_context = build_agent_context(
            user_id=user_id,
            category="macro",
            current_score=macro_avg,
            current_items=top_pretty,
            lookback_days=1,  # bewust 1 dag
        )

        # =========================================================
        # 5️⃣ AI MACRO ANALYSE
        # =========================================================
        payload = {
            "context": agent_context,
            "macro_items": macro_items,
            "macro_rules": rules_by_indicator,
            "macro_avg_score": macro_avg,
            "top_contributors": top_pretty,
        }

        macro_task = """
Je bent een ervaren macro-analist voor Bitcoin.

Je krijgt:
- actuele macro-indicatoren
- macro score van vandaag
- score en bias van gisteren
- dominante contributors

Analyseer de macro-omgeving als REGIME, niet als losse signalen.

Verplicht behandelen:
1. Wat is vandaag structureel veranderd t.o.v. gisteren?
2. Welke macro-kracht is dominant (risk-off, risk-on, flight to quality)?
3. Is dit een voortzetting of intensivering?
4. Wat betekent dit voor positionering en timing?

Schrijf GEEN samenvatting in headline-stijl.
Schrijf GEEN uitleg van indicatoren.

Antwoord uitsluitend in geldige JSON met:
- trend
- bias
- risico
- samenvatting (minstens 3 zinnen, inhoudelijk verbonden)
- top_signals (max 5, verklarend)
"""

        system_prompt = build_system_prompt(agent="macro", task=macro_task)

        raw_ai_context = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(raw_ai_context, dict):
            raise ValueError("❌ Macro AI response geen geldige JSON")

        ai_context = normalize_ai_context(raw_ai_context, macro_items)

        # =========================================================
        # 6️⃣ AI REFLECTIES (✅ MET CONTEXT)
        # =========================================================
        reflections_task = """
Maak per macro-indicator een reflectie.

Gebruik expliciet:
- verandering t.o.v. gisteren
- rol van deze indicator in het macrobeeld

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
            prompt=json.dumps({
                "context": agent_context,
                "items": macro_items
            }, ensure_ascii=False, indent=2),
            system_role=reflections_prompt
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =========================================================
        # 7️⃣ Opslaan ai_category_insights
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
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context["top_signals"]),
            ))

        # =========================================================
        # 8️⃣ Opslaan ai_reflections
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

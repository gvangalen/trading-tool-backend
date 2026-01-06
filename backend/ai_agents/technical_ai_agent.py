import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt
from backend.ai_core.agent_context import build_agent_context  # ✅ gedeelde context

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🧠 Helpers
# ======================================================
def is_empty_technical_context(ctx: dict) -> bool:
    if not isinstance(ctx, dict):
        return True

    return not any([
        ctx.get("summary"),
        ctx.get("trend"),
        ctx.get("bias"),
        ctx.get("risk"),
        ctx.get("top_signals"),
    ])


def fallback_technical_context(items: list) -> dict:
    indicators = {i["indicator"] for i in items if i.get("indicator")}

    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "summary": (
            "De technische analyse is gebaseerd op een beperkt aantal indicatoren. "
            "Gebruik scores en trendveranderingen als leidraad."
        ),
        "top_signals": [
            f"{ind} blijft technisch richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte technische data beschikbaar"],
    }


def normalize_ai_context(ai_ctx: dict, items: list) -> dict:
    """
    ✅ Unwrap-fix:
    accepteert {analysis:{…}}, {analyse:{…}}, {context:{…}}
    """

    if not isinstance(ai_ctx, dict):
        return fallback_technical_context(items)

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

    if is_empty_technical_context(normalized):
        logger.warning("⚠️ Technical AI gaf lege inhoud → fallback toegepast")
        return fallback_technical_context(items)

    return normalized


# =====================================================================
# 📊 TECHNICAL AI AGENT (MET GEDEELD GEHEUGEN)
# =====================================================================
def run_technical_agent(user_id: int):
    """
    Genereert technical AI insights voor één gebruiker.

    ✔ gebruikt gedeelde agent-context
    ✔ vergelijkt met gisteren
    ✔ verklaart score-veranderingen
    ✔ schrijft ai_category_insights + ai_reflections
    """

    if user_id is None:
        raise ValueError("❌ Technical AI Agent vereist een user_id")

    logger.info(f"📊 [Technical-Agent] Start — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # =====================================================
        # 1️⃣ TECHNICAL DATA (LAATSTE SNAPSHOT)
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ti.indicator, ti.value, ti.score, ti.advies, ti.uitleg, ti.timestamp
                FROM technical_indicators ti
                JOIN indicators i
                  ON i.name = ti.indicator
                 AND i.category = 'technical'
                 AND i.active = TRUE
                WHERE ti.user_id = %s
                ORDER BY ti.indicator, ti.timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            logger.info("ℹ️ Geen technical data beschikbaar")
            return

        latest = {}
        for name, value, score, advies, uitleg, ts in rows:
            key = normalize_indicator_name(name)
            if key not in latest:
                latest[key] = {
                    "indicator": key,
                    "value": float(value) if value is not None else None,
                    "score": float(score) if score is not None else None,
                    "advies": advies,
                    "uitleg": uitleg,
                    "timestamp": ts.isoformat() if ts else None,
                }

        combined = list(latest.values())

        avg_score = round(
            sum(i["score"] for i in combined if i["score"] is not None) / len(combined),
            2
        )

        # =====================================================
        # 2️⃣ 🧠 SHARED AGENT CONTEXT (GISTEREN)
        # =====================================================
        agent_context = build_agent_context(
            user_id=user_id,
            category="technical",
            current_score=avg_score,
            current_items=combined,
            lookback_days=1,
        )

        # =====================================================
        # 3️⃣ AI TECHNICAL ANALYSE (MET CONTEXT)
        # =====================================================
        technical_task = """
Je bent een ervaren technische marktanalist voor Bitcoin.

Je krijgt:
- actuele technische indicatoren
- de technische score van vandaag
- het verschil t.o.v. gisteren
- je vorige technische analyse

Analyseer dit op STRUCTUUR, niet alleen signalen.

Verplicht behandelen:
1. Trendstructuur
   - voortzetting, verzwakking of omslag
2. Momentum & timing
   - is oververhitting gevaarlijk of contextueel logisch?
3. Betrouwbaarheid
   - hoe betrouwbaar zijn de huidige signalen?
4. Implicatie
   - wat betekent dit voor handelen vandaag?

Schrijf GEEN algemene uitleg van indicatoren.

Antwoord uitsluitend in geldige JSON met:
- trend
- bias
- risico
- samenvatting (minstens 3 zinnen, samenhangend)
- top_signals (max 5, inhoudelijk, geen herhaling)
"""

        system_prompt = build_system_prompt(
            agent="technical",
            task=technical_task
        )

        payload = {
            "context": agent_context,
            "current_indicators": combined,
            "avg_score_today": avg_score,
        }

        raw_ai_context = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(raw_ai_context, dict):
            raise ValueError("❌ Technical AI response geen geldige JSON")

        ai_context = normalize_ai_context(raw_ai_context, combined)

        # =====================================================
        # 4️⃣ AI REFLECTIES (MET CONTEXT)
        # =====================================================
        reflections_task = """
Maak per technische indicator een reflectie.

Gebruik:
- huidige waarde
- verandering t.o.v. gisteren
- rol in het totaalbeeld

Per item:
- ai_score (0–100)
- compliance (0–100)
- korte comment
- concrete aanbeveling

Antwoord uitsluitend als JSON-lijst.
"""

        reflections_prompt = build_system_prompt(
            agent="technical",
            task=reflections_task
        )

        ai_reflections = ask_gpt(
            prompt=json.dumps({
                "context": agent_context,
                "items": combined
            }, ensure_ascii=False, indent=2),
            system_role=reflections_prompt
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # =====================================================
        # 5️⃣ OPSLAAN ai_category_insights
        # =====================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('technical', %s, %s, %s, %s, %s, %s, %s)
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
                avg_score,
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context["top_signals"]),
            ))

        # =====================================================
        # 6️⃣ OPSLAAN ai_reflections
        # =====================================================
        for r in ai_reflections:
            if not r.get("indicator"):
                continue

            indicator = normalize_indicator_name(r["indicator"])

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('technical', %s, %s, NULL, %s, %s, %s, %s)
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
        logger.info(f"✅ [Technical-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Technical-Agent] Fout", exc_info=True)
        raise

    finally:
        conn.close()

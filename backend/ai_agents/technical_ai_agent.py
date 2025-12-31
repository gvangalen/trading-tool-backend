import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 🧠 HELPERS
# ======================================================
def normalize_top_signals(val):
    """Zorgt dat top_signals altijd een JSON-serialiseerbare lijst is."""
    if val is None:
        return []
    if isinstance(val, list):
        # lijst van strings/objects → alles naar string
        out = []
        for x in val:
            if x is None:
                continue
            if isinstance(x, (dict, list)):
                out.append(json.dumps(x, ensure_ascii=False))
            else:
                out.append(str(x))
        return out
    if isinstance(val, dict):
        return [f"{k}: {v}" for k, v in val.items()]
    return [str(val)]


def is_empty_technical_context(ctx: dict) -> bool:
    """Checkt of de AI inhoudelijk iets heeft gezegd."""
    if not isinstance(ctx, dict):
        return True

    summary = ctx.get("summary") or ctx.get("samenvatting")
    trend = ctx.get("trend")
    bias = ctx.get("bias")
    risk = ctx.get("risk") or ctx.get("risico")
    momentum = ctx.get("momentum")
    top_signals = ctx.get("top_signals")

    return not any([summary, trend, bias, risk, momentum, top_signals])


def fallback_technical_context(combined: list) -> dict:
    """Veilige fallback als AI echt te weinig output geeft."""
    indicators = {i.get("indicator") for i in combined if i.get("indicator")}
    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "momentum": "zwak",
        "summary": (
            "Technische analyse is beschikbaar, maar AI-output was onvoldoende gestructureerd. "
            "Gebruik de indicator-scores en trends als leidraad."
        ),
        "top_signals": [
            f"{ind} is richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte technische data beschikbaar"],
    }


def normalize_ai_context(raw: dict, combined: list) -> dict:
    """
    ✅ DE FIX:
    - Unwrap nested output (analyse / technical_analysis / technicalAnalysis)
    - Map NL/EN keys naar 1 standaard
    - Maak top_signals altijd een lijst
    - Als AI alleen per-indicator output geeft → maak alsnog een bruikbare summary
    """
    if not isinstance(raw, dict):
        return fallback_technical_context(combined)

    # 1) unwrap bekende nesting
    for key in ["analyse", "technical_analysis", "technicalAnalysis", "analysis"]:
        if key in raw and isinstance(raw[key], dict):
            raw = raw[key]
            break

    # 2) als het nog steeds alleen indicator-blokken zijn: probeer er context uit te trekken
    # (bijv. {"indicators": {...}} )
    indicators_block = raw.get("indicators") if isinstance(raw.get("indicators"), dict) else None

    trend = raw.get("trend") or raw.get("Trend")
    bias = raw.get("bias") or raw.get("Bias")
    risk = raw.get("risk") or raw.get("risico") or raw.get("Risk")
    momentum = raw.get("momentum") or raw.get("Momentum")
    summary = raw.get("summary") or raw.get("samenvatting") or raw.get("Samenvatting")
    top_signals = raw.get("top_signals") or raw.get("signals") or raw.get("belangrijkste_signalen")

    # 3) Als AI geen summary/trend etc gaf maar wel indicators-block: maak iets bruikbaars
    if (not summary) and indicators_block:
        # Pak 2-3 indicator regels als bullets
        bullets = []
        for ind_name, ind_info in list(indicators_block.items())[:3]:
            if isinstance(ind_info, dict):
                t = ind_info.get("trend") or ind_info.get("Trend")
                r = ind_info.get("risico") or ind_info.get("risk")
                b = ind_info.get("bias")
                line = f"{ind_name}: {t or 'n.v.t.'}"
                if b:
                    line += f" | bias: {b}"
                if r:
                    line += f" | risico: {r}"
                bullets.append(line)
            else:
                bullets.append(f"{ind_name}: {ind_info}")

        summary = "Technische signalen zijn gemengd; zie belangrijkste indicatoren hieronder."
        top_signals = bullets

        # trend/bias/risk/momentum eventueel ook invullen als leeg
        trend = trend or "gemengd"
        bias = bias or "neutraal"
        risk = risk or "gemiddeld"
        momentum = momentum or "gemengd"

    normalized = {
        "trend": (trend or "").strip(),
        "bias": (bias or "").strip(),
        "risk": (risk or "").strip(),
        "momentum": (momentum or "").strip(),
        "summary": (summary or "").strip(),
        "top_signals": normalize_top_signals(top_signals),
    }

    if is_empty_technical_context(normalized):
        logger.warning("⚠️ Technical AI gaf lege/onnuttige inhoud → fallback gebruikt")
        return fallback_technical_context(combined)

    return normalized


def normalize_reflections(raw):
    """
    Zorgt dat reflections altijd voldoen aan:
    indicator, ai_score, compliance, comment, recommendation
    (AI stuurt soms 'korte_comment' of andere keys)
    """
    if not isinstance(raw, list):
        return []

    out = []
    for r in raw:
        if not isinstance(r, dict):
            continue

        indicator = r.get("indicator") or r.get("name")
        if not indicator:
            continue

        out.append({
            "indicator": normalize_indicator_name(indicator),
            "ai_score": r.get("ai_score", r.get("score", 50)),
            "compliance": r.get("compliance", r.get("discipline", 50)),
            "comment": r.get("comment", r.get("korte_comment", r.get("opmerking", ""))) or "",
            "recommendation": r.get("recommendation", r.get("aanbeveling", "")) or "",
        })

    return out


# =====================================================================
# 📊 TECHNICAL AI AGENT — DB-GEDREVEN, AI = CONTEXT ONLY
# =====================================================================
def run_technical_agent(user_id: int):
    """
    Genereert technical AI insights voor één user.

    Schrijft:
    - ai_category_insights (category='technical')
    - ai_reflections (category='technical')

    ✔ DB is leidend
    ✔ AI = interpretatie (context + reflecties)
    ✔ Laatste snapshot per indicator
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
        # 1️⃣ TECHNICAL SCOREREGELS (GLOBAAL)
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
        # 2️⃣ LAATSTE TECHNICAL INDICATORS (actief)
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
- korte samenvatting (minstens 1 zin)
- belangrijkste technische signalen (minstens 1 punt)

Antwoord uitsluitend in geldige JSON.
"""

        system_prompt = build_system_prompt(agent="technical", task=TECHNICAL_TASK)

        raw_ai_context = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(raw_ai_context, dict):
            raise ValueError("❌ Technical AI response is geen geldige JSON")

        ai_context = normalize_ai_context(raw_ai_context, combined)

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

        raw_reflections = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=reflection_prompt
        )

        ai_reflections = normalize_reflections(raw_reflections)

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
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context["top_signals"], ensure_ascii=False),
            ))

        # ------------------------------------------------------
        # 7️⃣ OPSLAAN AI_REFLECTIONS
        # ------------------------------------------------------
        for r in ai_reflections:
            indicator_norm = normalize_indicator_name(r["indicator"])

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
                    float(r.get("ai_score", 50)),
                    float(r.get("compliance", 50)),
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

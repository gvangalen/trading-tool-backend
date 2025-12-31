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
EXPECTED_KEYS = {
    "trend", "bias", "risk", "risico", "momentum",
    "summary", "samenvatting", "top_signals"
}


def normalize_top_signals(val):
    """Zorgt dat top_signals altijd JSON-serialiseerbaar is (list)."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, dict):
        return [f"{k}: {v}" for k, v in val.items()]
    return [str(val)]


def find_best_analysis_dict(obj):
    """
    Zoek recursief in de AI response naar een dict die de meeste
    'EXPECTED_KEYS' bevat. Dit lost nesting op zoals:
    - {"analyse": {...}}
    - {"technical_analysis": {...}}
    - {"result": {...}}
    - {"technical_analysis": {"indicators": {...}}}
    """
    best = None
    best_score = 0

    def walk(x):
        nonlocal best, best_score
        if isinstance(x, dict):
            score = len(set(x.keys()) & EXPECTED_KEYS)
            if score > best_score:
                best_score = score
                best = x
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return best, best_score


def is_empty_technical_context(ctx: dict) -> bool:
    """Checkt inhoudelijk: heeft AI iets bruikbaars gezegd?"""
    if not isinstance(ctx, dict):
        return True

    return not any([
        ctx.get("summary"),
        ctx.get("samenvatting"),
        ctx.get("trend"),
        ctx.get("bias"),
        ctx.get("risk"),
        ctx.get("risico"),
        ctx.get("momentum"),
        ctx.get("top_signals"),
    ])


def build_overall_from_indicators(indicators_dict: dict) -> dict:
    """
    Als de AI alleen per-indicator output geeft (bijv. technical_analysis.indicators),
    bouwen we een overall trend/bias/risk + summary + top_signals.
    """
    if not isinstance(indicators_dict, dict) or not indicators_dict:
        return {}

    # Pak 1-3 bullets uit indicatoren
    bullets = []
    bias_notes = []
    risk_notes = []
    trend_votes = {"bullish": 0, "bearish": 0, "neutraal": 0, "neutral": 0}

    for ind, data in indicators_dict.items():
        if not isinstance(data, dict):
            continue

        t = (data.get("trend") or "").lower()
        b = (data.get("bias") or "").lower()
        r = (data.get("risk") or data.get("risico") or "").lower()

        if "bull" in t:
            trend_votes["bullish"] += 1
        elif "bear" in t:
            trend_votes["bearish"] += 1
        elif "neut" in t or "neutral" in t:
            trend_votes["neutraal"] += 1

        if b:
            bias_notes.append(f"{ind}: {b}")
        if r:
            risk_notes.append(f"{ind}: {r}")

        # probeer een bullet te maken
        expl = data.get("uitleg") or data.get("interpretation") or data.get("comment") or ""
        if expl:
            bullets.append(f"{ind}: {str(expl)[:160]}")
        else:
            bullets.append(f"{ind}: signaal actief")

    # overall trend
    if trend_votes["bullish"] > trend_votes["bearish"]:
        overall_trend = "bullish"
    elif trend_votes["bearish"] > trend_votes["bullish"]:
        overall_trend = "bearish"
    else:
        overall_trend = "neutraal"

    # overall bias/risk simpel
    overall_bias = "neutraal"
    if any("neg" in x or "bear" in x for x in bias_notes):
        overall_bias = "negatief"
    if any("pos" in x or "bull" in x for x in bias_notes) and overall_bias != "negatief":
        overall_bias = "positief"

    overall_risk = "gemiddeld"
    if any("hoog" in x or "high" in x for x in risk_notes):
        overall_risk = "hoog"
    elif any("laag" in x or "low" in x for x in risk_notes):
        overall_risk = "laag"

    return {
        "trend": overall_trend,
        "bias": overall_bias,
        "risk": overall_risk,
        "momentum": "gemengd" if overall_trend == "neutraal" else ("sterk" if overall_trend == "bullish" else "zwak"),
        "summary": (
            "Technische analyse is opgebouwd uit per-indicator signalen. "
            f"Overal beeld: {overall_trend} met {overall_risk} risico."
        ),
        "top_signals": bullets[:5],
    }


def fallback_technical_context(combined: list) -> dict:
    """Fallback als we echt niks bruikbaars hebben."""
    indicators = {i["indicator"] for i in combined}

    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "momentum": "zwak",
        "summary": (
            "De technische analyse is gebaseerd op een beperkt aantal actieve indicatoren. "
            "De huidige signalen geven richting, maar vereisen bevestiging voordat "
            "sterke posities worden ingenomen."
        ),
        "top_signals": [
            f"{ind} blijft technisch richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte technische data beschikbaar"],
    }


def normalize_ai_context(ai_context: dict) -> dict:
    """
    Normaliseert AI-output naar vlak formaat met keys:
    trend/bias/risk/momentum/summary/top_signals
    Ongeacht nesting: analyse / technical_analysis / result etc.
    """
    if not isinstance(ai_context, dict):
        return {}

    # 1) bekende wrappers
    for wrapper_key in ("analyse", "analysis", "technical_analysis", "result", "output", "data"):
        if wrapper_key in ai_context and isinstance(ai_context[wrapper_key], dict):
            ai_context = ai_context[wrapper_key]

    # 2) als nog steeds geen expected keys: zoek recursief best match
    best, best_score = find_best_analysis_dict(ai_context)
    if best and best_score >= 2:
        ai_context = best

    # 3) als het per-indicator is (zoals technical_analysis.indicators)
    indicators = None
    if isinstance(ai_context, dict):
        # nested indicators
        if "indicators" in ai_context and isinstance(ai_context["indicators"], dict):
            indicators = ai_context["indicators"]
        # soms heet dit "signals"
        if indicators is None and "signals" in ai_context and isinstance(ai_context["signals"], dict):
            indicators = ai_context["signals"]

    if indicators:
        overall = build_overall_from_indicators(indicators)
        if overall:
            return overall

    # 4) map NL/EN keys naar unified
    out = {
        "trend": ai_context.get("trend") or "",
        "bias": ai_context.get("bias") or "",
        "risk": ai_context.get("risk") or ai_context.get("risico") or "",
        "momentum": ai_context.get("momentum") or "",
        "summary": ai_context.get("summary") or ai_context.get("samenvatting") or "",
        "top_signals": normalize_top_signals(ai_context.get("top_signals")),
    }
    return out


def normalize_reflection_item(r: dict) -> dict:
    """Mapt verschillende key-namen naar wat wij opslaan."""
    if not isinstance(r, dict):
        return {}

    return {
        "indicator": r.get("indicator") or r.get("name"),
        "ai_score": r.get("ai_score", r.get("score", 50)),
        "compliance": r.get("compliance", r.get("discipline", 50)),
        "comment": r.get("comment", r.get("korte_comment", r.get("uitleg", ""))),
        "recommendation": r.get("recommendation", r.get("aanbeveling", r.get("actie", ""))),
    }


# =====================================================================
# 📊 TECHNICAL AI AGENT — ROBUUST & FAILSAFE
# =====================================================================
def run_technical_agent(user_id: int):
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
        # 3️⃣ DEDUP → LAATSTE METING PER INDICATOR
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
                (
                    r["trend"]
                    for r in rules_by_indicator.get(key, [])
                    if r.get("score") == int(score_f)
                ),
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
        # 4️⃣ AI CONTEXT
        # ------------------------------------------------------
        TECHNICAL_TASK = """
Analyseer technische indicatoren voor Bitcoin.

Geef altijd als JSON (geen nesting):
{
  "trend": "bullish/bearish/neutraal",
  "bias": "positief/negatief/neutraal",
  "risk": "laag/gemiddeld/hoog",
  "momentum": "...",
  "summary": "...",
  "top_signals": ["...", "..."]
}

Gebruik uitsluitend de aangeleverde indicatoren.
"""

        system_prompt = build_system_prompt(agent="technical", task=TECHNICAL_TASK)

        raw_ai_context = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(raw_ai_context, dict):
            raise ValueError("❌ Technical AI response is geen dict")

        ai_context = normalize_ai_context(raw_ai_context)

        # fallback als nog leeg
        if is_empty_technical_context(ai_context):
            logger.warning("⚠️ Technical AI gaf alsnog lege inhoud → fallback gebruikt")
            ai_context = fallback_technical_context(combined)

        # ------------------------------------------------------
        # 5️⃣ AI REFLECTIES
        # ------------------------------------------------------
        REFLECTION_TASK = """
Maak reflecties per technische indicator.

Per item (JSON):
- indicator
- ai_score (0-100)
- compliance (0-100)
- comment (korte uitleg)
- recommendation (concrete actie)

Antwoord uitsluitend als JSON-lijst.
"""

        reflection_prompt = build_system_prompt(agent="technical", task=REFLECTION_TASK)

        raw_reflections = ask_gpt(
            prompt=json.dumps(combined, ensure_ascii=False, indent=2),
            system_role=reflection_prompt
        )

        if not isinstance(raw_reflections, list):
            raw_reflections = []

        ai_reflections = []
        for r in raw_reflections:
            norm = normalize_reflection_item(r)
            if norm.get("indicator"):
                ai_reflections.append(norm)

        # ------------------------------------------------------
        # 6️⃣ OPSLAAN ai_category_insights
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
                json.dumps(normalize_top_signals(ai_context.get("top_signals"))),
            ))

        # ------------------------------------------------------
        # 7️⃣ OPSLAAN ai_reflections
        # ------------------------------------------------------
        for r in ai_reflections:
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
                    float(r.get("ai_score", 50)),
                    float(r.get("compliance", 50)),
                    r.get("comment", "") or "",
                    r.get("recommendation", "") or "",
                ))

        conn.commit()
        logger.info(f"✅ [Technical-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Technical-Agent] FOUT", exc_info=True)

    finally:
        conn.close()

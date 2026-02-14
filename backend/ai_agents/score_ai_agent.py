import json
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_json
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DOMAIN_CATEGORIES = ["macro", "market", "technical", "setup", "strategy"]
MASTER_CATEGORY = "master"
WRITE_DAILY_SCORES = False


# ============================================================
# ✅ JSON Schema (hard enforced)
# ============================================================
MASTER_SCHEMA = {
    "name": "master_score",
    "schema": {
        "type": "object",
        "properties": {
            "master_trend": {"type": "string"},
            "master_bias": {"type": "string"},
            "master_risk": {"type": "string"},
            "master_score": {"type": "number"},
            "alignment_score": {"type": "number"},
            "weights": {"type": "object"},
            "data_warnings": {"type": "array", "items": {"type": "string"}},
            "summary": {"type": "string"},
            "outlook": {"type": "string"},
            "domains": {"type": "object"},
        },
        "required": [
            "master_trend",
            "master_bias",
            "master_risk",
            "master_score",
            "alignment_score",
            "summary",
            "outlook",
            "data_warnings",
            "weights",
            "domains",
        ],
        "additionalProperties": True,
    },
}


# ============================================================
# ⚙️ Helpers
# ============================================================
def convert_decimal(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    return obj


def safe_json(obj: Any, fallback: Any):
    if isinstance(obj, (dict, list)):
        return obj
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except Exception:
            return fallback
    return fallback


def to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, str):
        s = v.strip().lower().replace(",", ".")
        if s in ("", "none", "null", "nan"):
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def clamp01_100(x: float) -> float:
    return max(0.0, min(100.0, float(x)))


def stringify_top_signals(top_signals: Any) -> List[str]:
    ts = safe_json(top_signals, [])
    if not isinstance(ts, list):
        return []

    out = []
    for item in ts:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            name = (
                item.get("indicator")
                or item.get("name")
                or item.get("signal")
                or item.get("title")
            )
            out.append(str(name) if name else json.dumps(item, ensure_ascii=False))
        else:
            out.append(str(item))
    return out[:10]


def calculate_strategy_score(*, market: float, technical: float, setup: float) -> float:
    market = float(market) if market is not None else 0.0
    technical = float(technical) if technical is not None else 0.0
    setup = float(setup) if setup is not None else 0.0
    score = (0.33 * market) + (0.33 * technical) + (0.34 * setup)
    return round(score, 1)


# ============================================================
# 📥 Insights ophalen
# ============================================================
def fetch_today_insights(conn, user_id: int) -> Dict[str, dict]:
    insights: Dict[str, dict] = {}
    today = date.today()
    lookback = [today, today - timedelta(days=1), today - timedelta(days=2)]

    with conn.cursor() as cur:
        for cat in DOMAIN_CATEGORIES:
            result = None
            for d in lookback:
                cur.execute(
                    """
                    SELECT category, avg_score, trend, bias, risk, summary, top_signals, date
                    FROM ai_category_insights
                    WHERE category = %s AND user_id = %s AND date = %s
                    LIMIT 1;
                    """,
                    (cat, user_id, d),
                )
                row = cur.fetchone()
                if row:
                    result = {
                        "category": row[0],
                        "avg_score": float(row[1]) if row[1] is not None else None,
                        "trend": row[2] or "",
                        "bias": row[3] or "",
                        "risk": row[4] or "",
                        "summary": row[5] or "",
                        "top_signals": safe_json(row[6] or "[]", []),
                        "date": str(row[7] or d),
                    }
                    break

            if result:
                insights[cat] = result

    return insights


def fetch_setup_score_from_insights(insights: Dict[str, dict]) -> Optional[float]:
    try:
        v = insights.get("setup", {}).get("avg_score")
        return float(v) if v is not None else None
    except Exception:
        return None


# ============================================================
# 📊 Numerieke context
# ============================================================
def fetch_numeric_scores(conn, user_id: int, insights: Dict[str, dict]) -> Dict[str, Any]:
    numeric: Dict[str, Any] = {"daily_scores": {}, "ai_reflections": {}}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT macro_score, market_score, technical_score, setup_score
            FROM daily_scores
            WHERE report_date = CURRENT_DATE AND user_id = %s
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cur.fetchone()

        if row:
            macro, market, technical, setup_score = row
            strategy_score = calculate_strategy_score(
                market=market, technical=technical, setup=setup_score
            )
            numeric["daily_scores"] = {
                "macro": float(macro) if macro is not None else None,
                "market": float(market) if market is not None else None,
                "technical": float(technical) if technical is not None else None,
                "setup": float(setup_score) if setup_score is not None else None,
                "strategy": strategy_score,
            }

        cur.execute(
            """
            SELECT category,
                   ROUND(AVG(COALESCE(ai_score, 0))::numeric, 1),
                   ROUND(AVG(COALESCE(compliance, 0))::numeric, 1)
            FROM ai_reflections
            WHERE date = CURRENT_DATE AND user_id = %s
            GROUP BY category;
            """,
            (user_id,),
        )
        for cat, ai_score, comp in cur.fetchall() or []:
            numeric["ai_reflections"][cat] = {
                "avg_ai_score": float(ai_score),
                "avg_compliance": float(comp),
            }

    return convert_decimal(numeric)


# ============================================================
# 🧠 Prompt bouwen
# ============================================================
def build_prompt(insights: Dict[str, dict], numeric: Dict[str, Any]) -> str:
    def block(cat: str) -> str:
        i = insights.get(cat)
        if not i:
            return f"[{cat}] — ONVOLDOENDE DATA"

        sigs = stringify_top_signals(i.get("top_signals"))
        sigs_str = ", ".join(sigs) if sigs else "-"

        return (
            f"[{cat}] score={i.get('avg_score')} | trend={i.get('trend')} | "
            f"bias={i.get('bias')} | risk={i.get('risk')}\n"
            f"summary: {i.get('summary')}\n"
            f"signals: {sigs_str}\n"
            f"date: {i.get('date')}"
        )

    text = "\n\n".join(block(cat) for cat in DOMAIN_CATEGORIES)
    numeric_json = json.dumps(numeric, indent=2, ensure_ascii=False)

    return f"""
Return ONLY valid JSON (no markdown, no extra text).
Use numeric values for scores.

INPUT INSIGHTS:
{text}

NUMERIC CONTEXT:
{numeric_json}
""".strip()


# ============================================================
# 🧠 Next-level deterministic scoring (backup + penalties)
# ============================================================
def detect_conflicts(insights: Dict[str, dict], numeric: Dict[str, Any]) -> Tuple[List[str], float]:
    """
    Conflict rules:
    - macro extreem risk-off (<=35 of >=85 afhankelijk van jouw schaal) vs market/technical risk-on
    - missing/stale domeinen
    Returns: (warnings, penalty_points)
    """

    warnings: List[str] = []
    penalty = 0.0

    today = str(date.today())
    missing = [c for c in DOMAIN_CATEGORIES if c not in insights]
    if missing:
        warnings.append(f"Ontbrekende domeinen: {', '.join(missing)}")
        penalty += 15.0

    stale = [c for c, i in insights.items() if i.get("date") != today]
    if stale:
        warnings.append(f"Niet-verse data (fallback): {', '.join(stale)}")
        penalty += 10.0

    # pull scores
    macro = insights.get("macro", {}).get("avg_score")
    market = insights.get("market", {}).get("avg_score")
    technical = insights.get("technical", {}).get("avg_score")

    # basic contradiction check (je scoreschaal is 0-100)
    if macro is not None and market is not None:
        if macro >= 80 and market >= 70:
            # macro "defensief" maar market "bullish" → inconsistent
            warnings.append("Conflict: macro defensief maar market risk-on")
            penalty += 10.0

    if macro is not None and technical is not None:
        if macro >= 80 and technical >= 70:
            warnings.append("Conflict: macro defensief maar technical risk-on")
            penalty += 10.0

    # setup/strategy missing
    if "setup" not in insights:
        warnings.append("Geen setup-inzicht beschikbaar")
        penalty += 5.0
    if "strategy" not in insights:
        warnings.append("Geen strategy-inzicht beschikbaar")
        penalty += 5.0

    return warnings, penalty


def deterministic_master_score(insights: Dict[str, dict], numeric: Dict[str, Any]) -> Tuple[float, float, Dict[str, float]]:
    """
    Master_score = weighted avg van domeinen (indien aanwezig)
    Alignment_score = 100 - penalties (clamped)
    """

    weights = {"macro": 0.25, "market": 0.25, "technical": 0.25, "setup": 0.15, "strategy": 0.10}

    def get_score(cat: str) -> Optional[float]:
        v = insights.get(cat, {}).get("avg_score")
        return float(v) if isinstance(v, (int, float)) else None

    values = {c: get_score(c) for c in DOMAIN_CATEGORIES}
    # strategy: als domein score ontbreekt, pak numeric daily strategy als fallback
    if values.get("strategy") is None:
        v = numeric.get("daily_scores", {}).get("strategy")
        values["strategy"] = float(v) if isinstance(v, (int, float)) else None

    # normalize weights over available values
    present = {k: v for k, v in values.items() if v is not None}
    if not present:
        return 50.0, 0.0, weights

    w_sum = sum(weights[k] for k in present.keys())
    score = 0.0
    for k, v in present.items():
        score += (weights[k] / w_sum) * float(v)

    warnings, penalty = detect_conflicts(insights, numeric)
    master = clamp01_100(score - (penalty * 0.5))
    alignment = clamp01_100(100.0 - penalty)

    return round(master, 1), round(alignment, 1), weights


# ============================================================
# 💾 Opslaan
# ============================================================
def store_master_result(conn, result: dict, user_id: int):
    if not result or not isinstance(result, dict):
        result = {}

    master_score = to_float_or_none(result.get("master_score"))
    alignment_score = to_float_or_none(result.get("alignment_score"))

    if master_score is None:
        master_score = 50.0
    if alignment_score is None:
        alignment_score = 0.0

    master_score = clamp01_100(master_score)
    alignment_score = clamp01_100(alignment_score)
    alignment_score = min(alignment_score, master_score)

    domains = result.get("domains") or {}
    weights = result.get("weights") or {}
    data_warnings = result.get("data_warnings") or []
    if not isinstance(data_warnings, list):
        data_warnings = [str(data_warnings)]

    meta = {
        "weights": weights,
        "alignment_score": alignment_score,
        "data_warnings": data_warnings,
        "domains": domains,
        "outlook": result.get("outlook", "") or "",
    }

    trend = str(result.get("master_trend", "") or "")
    bias = str(result.get("master_bias", "") or "")
    risk = str(result.get("master_risk", "") or "")
    summary = str(result.get("summary", "") or "")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_category_insights
                (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
            VALUES ('master', %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (user_id, category, date)
            DO UPDATE SET
                avg_score = EXCLUDED.avg_score,
                trend = EXCLUDED.trend,
                bias = EXCLUDED.bias,
                risk = EXCLUDED.risk,
                summary = EXCLUDED.summary,
                top_signals = EXCLUDED.top_signals,
                updated_at = NOW();
            """,
            (user_id, master_score, trend, bias, risk, summary, json.dumps(meta, ensure_ascii=False)),
        )

    logger.info(f"💾 Master stored | user_id={user_id} | score={master_score} | alignment={alignment_score}")


# ============================================================
# 🚀 Runner
# ============================================================
def generate_master_score_for_user(user_id: int):
    logger.info(f"🧠 MASTER Orchestrator | user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        insights = fetch_today_insights(conn, user_id=user_id)
        numeric = fetch_numeric_scores(conn, user_id=user_id, insights=insights)

        # deterministische baseline + warnings
        base_master, base_alignment, base_weights = deterministic_master_score(insights, numeric)
        warnings, _penalty = detect_conflicts(insights, numeric)

        TASK = """
Je bent een master decision orchestrator voor een trading-systeem.
Je ordent en weegt uitsluitend de input; geen nieuwe analyses of data verzinnen.
Return ONLY valid JSON.
"""

        system_prompt = build_system_prompt(agent="master", task=TASK)
        prompt = build_prompt(insights, numeric)

        ai = ask_gpt_json(
            prompt=prompt,
            system_role=system_prompt
        )

        # ✅ hard fallback als AI lege output geeft
        if not ai:
            ai = {
                "master_trend": "–",
                "master_bias": "–",
                "master_risk": "–",
                "master_score": base_master,
                "alignment_score": base_alignment,
                "weights": base_weights,
                "data_warnings": warnings,
                "summary": "Fallback master score (AI output missing).",
                "outlook": "",
                "domains": {},
            }
        else:
            # AI mag nooit boven baseline alignment uitkomen als er conflicts zijn
            ai.setdefault("data_warnings", [])
            if isinstance(ai["data_warnings"], list):
                ai["data_warnings"] = list({*ai["data_warnings"], *warnings})
            else:
                ai["data_warnings"] = warnings

            # safety clamp
            ai["master_score"] = clamp01_100(to_float_or_none(ai.get("master_score")) or base_master)
            ai["alignment_score"] = clamp01_100(to_float_or_none(ai.get("alignment_score")) or base_alignment)
            ai["alignment_score"] = min(ai["alignment_score"], ai["master_score"], base_alignment)

            # ensure weights exist
            if not isinstance(ai.get("weights"), dict) or not ai["weights"]:
                ai["weights"] = base_weights

        store_master_result(conn, ai, user_id=user_id)

        if WRITE_DAILY_SCORES:
            # jij had dit al, laat default False
            pass

        conn.commit()
        logger.info(f"✅ Master score opgeslagen voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Crash in master-score", exc_info=True)
    finally:
        conn.close()


# ============================================================
# 🚀 Celery task — draait voor ALLE users
# ============================================================
@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    logger.info("🧠 Start MASTER Score AI — MULTI USER MODE...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users;")
            users = [row[0] for row in cur.fetchall()]
        logger.info(f"👥 {len(users)} gebruikers gevonden.")
    except Exception:
        logger.error("❌ Kon users niet ophalen", exc_info=True)
        return
    finally:
        conn.close()

    for user_id in users:
        generate_master_score_for_user(user_id)

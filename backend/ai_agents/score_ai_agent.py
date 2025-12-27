import json
import logging
import traceback
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt  # JSON-engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# ✔ Domeinen die we orchestreren (MASTER sluit je uit van input)
# ============================================================
DOMAIN_CATEGORIES = ["macro", "market", "technical", "setup", "strategy"]
MASTER_CATEGORY = "master"


# ============================================================
# ⚙️ Decimal → float converter
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


def stringify_top_signals(top_signals: Any) -> List[str]:
    """
    top_signals kan zijn:
    - list[str]
    - list[dict] (bv. {indicator, score, trend...})
    - string / json-string
    """
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
            if name:
                out.append(str(name))
            else:
                out.append(json.dumps(item, ensure_ascii=False))
        else:
            out.append(str(item))
    return out[:10]


# ============================================================
# 📥 1. Insights ophalen → ai_category_insights (lookback)
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


# ============================================================
# ✅ Helper: Setup-score ophalen (UIT SETUP AGENT)
# ============================================================
def fetch_setup_score_from_insights(insights: Dict[str, dict]) -> Optional[float]:
    """
    Setup-score bron = Setup Agent → ai_category_insights(category='setup')
    """
    try:
        v = insights.get("setup", {}).get("avg_score")
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# ============================================================
# 📊 2. Numerieke context uit daily_scores + ai_reflections
#    ✅ setup-score NIET uit daily_scores, maar uit setup-insights
# ============================================================
def fetch_numeric_scores(conn, user_id: int, insights: Dict[str, dict]) -> Dict[str, Any]:
    numeric: Dict[str, Any] = {"daily_scores": {}, "ai_reflections": {}}

    with conn.cursor() as cur:
        # daily_scores (macro/market/technical ONLY)
        cur.execute(
            """
            SELECT macro_score, market_score, technical_score
            FROM daily_scores
            WHERE report_date = CURRENT_DATE AND user_id = %s
            LIMIT 1;
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if row:
            numeric["daily_scores"] = {
                "macro": row[0],
                "market": row[1],
                "technical": row[2],
            }

        # setup-score komt UIT setup agent (ai_category_insights)
        numeric["daily_scores"]["setup"] = fetch_setup_score_from_insights(insights)

        # ai_reflections aggregatie (optioneel / bestaat al bij jou)
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
# 🧠 3. Master prompt bouwen
# ============================================================
def build_prompt(insights: Dict[str, dict], numeric: Dict[str, Any]) -> str:
    def block(cat: str) -> str:
        i = insights.get(cat)
        if not i:
            return f"[{cat}] — GEEN DATA"

        sigs = stringify_top_signals(i.get("top_signals"))
        sigs_str = ", ".join(sigs) if sigs else "-"

        return (
            f"[{cat}] score={i.get('avg_score')} | trend={i.get('trend')} | bias={i.get('bias')} | risk={i.get('risk')}\n"
            f"summary: {i.get('summary')}\n"
            f"signals: {sigs_str}"
        )

    text = "\n\n".join(block(cat) for cat in DOMAIN_CATEGORIES)
    numeric_json = json.dumps(numeric, indent=2, ensure_ascii=False)

    return f"""
Je bent de MASTER Orchestrator AI voor trading.

Antwoord ALLEEN met geldige JSON in dit format:

{{
  "master_trend": "",
  "master_bias": "",
  "master_risk": "",
  "master_score": 0,
  "alignment_score": 0,
  "weights": {{
    "macro": 0.25,
    "market": 0.25,
    "technical": 0.25,
    "setup": 0.15,
    "strategy": 0.10
  }},
  "data_warnings": [],
  "summary": "",
  "outlook": "",
  "domains": {{
    "macro": {{}},
    "market": {{}},
    "technical": {{}},
    "setup": {{}},
    "strategy": {{}}
  }}
}}

=== INPUT DATA ===
{text}

=== NUMBERS ===
{numeric_json}
"""


# ============================================================
# 💾 4. Opslaan → ai_category_insights (categorie: 'master')
# ============================================================
def store_master_result(conn, result: dict, user_id: int):
    meta = {
        "weights": result.get("weights"),
        "alignment_score": result.get("alignment_score"),
        "data_warnings": result.get("data_warnings"),
        "domains": result.get("domains"),
        "outlook": result.get("outlook"),
    }

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
            (
                user_id,
                result.get("master_score"),
                result.get("master_trend"),
                result.get("master_bias"),
                result.get("master_risk"),
                result.get("summary"),
                json.dumps(meta, ensure_ascii=False),
            ),
        )


# ============================================================
# 🕗 daily_scores vullen — per user (OPTIONEEL)
# ✅ setup_score komt UIT setup agent, niet berekend
# ============================================================
def store_daily_scores(conn, insights: Dict[str, dict], user_id: int):
    macro = insights.get("macro", {}).get("avg_score")
    market = insights.get("market", {}).get("avg_score")
    technical = insights.get("technical", {}).get("avg_score")
    setup_score = fetch_setup_score_from_insights(insights)  # ✅ FIX

    macro_sum = insights.get("macro", {}).get("summary", "")
    market_sum = insights.get("market", {}).get("summary", "")
    tech_sum = insights.get("technical", {}).get("summary", "")

    if macro is None or market is None or technical is None:
        logger.error(
            f"❌ daily_scores NIET opgeslagen — ontbrekende macro/market/technical (user_id={user_id})."
        )
        return

    # als setup agent nog niet gedraaid heeft: laat NULL of fallback
    # (maar NIET zelf berekenen)
    if setup_score is None:
        logger.warning("⚠️ Setup-score ontbreekt (setup agent nog niet gedraaid?) → setup_score blijft NULL/0")
        setup_score = 0  # of None als je kolom nullable is

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_scores
                (report_date, user_id, macro_score, market_score, technical_score,
                 macro_interpretation, market_interpretation, technical_interpretation,
                 setup_score, macro_top_contributors, market_top_contributors, technical_top_contributors)
            VALUES (
                CURRENT_DATE, %s, %s, %s, %s,
                %s, %s, %s,
                %s, '[]', '[]', '[]'
            )
            ON CONFLICT (report_date, user_id)
            DO UPDATE SET
                macro_score = EXCLUDED.macro_score,
                market_score = EXCLUDED.market_score,
                technical_score = EXCLUDED.technical_score,
                macro_interpretation = EXCLUDED.macro_interpretation,
                market_interpretation = EXCLUDED.market_interpretation,
                technical_interpretation = EXCLUDED.technical_interpretation,
                setup_score = EXCLUDED.setup_score;
             """,
            (
                user_id,
                macro,
                market,
                technical,
                macro_sum,
                market_sum,
                tech_sum,
                setup_score,
            ),
        )

    logger.info(f"💾 daily_scores opgeslagen voor user_id={user_id} (setup_score uit setup agent).")


# ============================================================
# 🚀 Per-user runner
# ============================================================
def generate_master_score_for_user(user_id: int):
    logger.info(f"🧩 MASTER Orchestrator voor user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # 1️⃣ Lees AL bestaande data
        insights = fetch_today_insights(conn, user_id=user_id)
        numeric = fetch_numeric_scores(conn, user_id=user_id)

        # 2️⃣ Denk & synthese
        prompt = build_prompt(insights, numeric)

        result = ask_gpt(
            prompt,
            system_role="Je bent een master trading orchestrator. Antwoord uitsluitend in geldige JSON.",
        )

        if not isinstance(result, dict):
            raise ValueError("❌ Master orchestrator gaf geen geldige JSON dict terug")

        # 3️⃣ Opslaan
        store_master_result(conn, result, user_id=user_id)

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

        logger.info(f"👥 {len(users)} gebruikers gevonden. Genereren per user...")

    except Exception:
        logger.error("❌ Kon users niet ophalen", exc_info=True)
        return

    finally:
        conn.close()

    for user_id in users:
        generate_master_score_for_user(user_id)

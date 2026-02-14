import json
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# ✔ Domeinen die we orchestreren (MASTER zelf is OUTPUT)
# ============================================================
DOMAIN_CATEGORIES = ["macro", "market", "technical", "setup", "strategy"]
MASTER_CATEGORY = "master"

# Zet dit op True als je master-agent óók daily_scores wil vullen
# (meestal niet nodig als macro/market/technical/setup agents dat al doen)
WRITE_DAILY_SCORES = False


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

def calculate_strategy_score(
    *,
    market: float,
    technical: float,
    setup: float,
) -> float:
    """
    Strategy score = gewogen verhouding van market + technical + setup
    GEEN Decimal-logica hier.
    """

    market = float(market) if market is not None else 0.0
    technical = float(technical) if technical is not None else 0.0
    setup = float(setup) if setup is not None else 0.0

    score = (
        0.33 * market +
        0.33 * technical +
        0.34 * setup
    )

    return round(score, 1)


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
# ✅ Helper: Setup-score ophalen (UIT SETUP agent insights)
# ============================================================
def fetch_setup_score_from_insights(insights: Dict[str, dict]) -> Optional[float]:
    try:
        v = insights.get("setup", {}).get("avg_score")
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# ============================================================
# 📊 2. Numerieke context uit daily_scores + ai_reflections
# ============================================================
def fetch_numeric_scores(conn, user_id: int, insights: Dict[str, dict]) -> Dict[str, Any]:
    numeric: Dict[str, Any] = {"daily_scores": {}, "ai_reflections": {}}

    with conn.cursor() as cur:
        # daily_scores (macro / market / technical / setup)
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

            # ✅ STRATEGY SCORE = market + technical + setup
            strategy_score = calculate_strategy_score(
                market=market,
                technical=technical,
                setup=setup_score,
            )

            numeric["daily_scores"] = {
                "macro": float(macro) if macro is not None else None,
                "market": float(market) if market is not None else None,
                "technical": float(technical) if technical is not None else None,
                "setup": float(setup_score) if setup_score is not None else None,
                "strategy": strategy_score,
            }

        # ai_reflections aggregatie (ongewijzigd)
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
    """
    Bouwt de prompt voor de Master AI.
    Extra JSON-bescherming toegevoegd om parsing fouten te voorkomen.
    """

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
            f"signals: {sigs_str}"
        )

    text = "\n\n".join(block(cat) for cat in DOMAIN_CATEGORIES)
    numeric_json = json.dumps(numeric, indent=2, ensure_ascii=False)

    return f"""
CRITICAL:
Return ONLY valid JSON.
No markdown.
No explanations.
No text outside JSON.
Use numeric values for scores.

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
# ✅ Helpers: numeric parsing voor AI output
# ============================================================
def to_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in {"none", "null", "nan"}:
            return None
        # "59,00" → "59.00"
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None


# ============================================================
# 💾 4. Opslaan → ai_category_insights (categorie: 'master')
# ============================================================
def store_master_result(conn, result: dict, user_id: int):
    """
    Slaat master score robuust op.
    Beschermt tegen:
    - AI die strings terugstuurt
    - ontbrekende velden
    - lege output
    - alignment > master
    - NULL waarden
    """

    # =====================================================
    # 1️⃣ AI RESULT VALIDATIE
    # =====================================================
    if not result or not isinstance(result, dict):
        logger.warning("⚠️ Master AI gaf lege of ongeldige output → fallback")
        result = {}

    # =====================================================
    # 2️⃣ SAFE NUMERIC PARSING
    # =====================================================
    def to_float_or_none(v):
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

    master_score = to_float_or_none(result.get("master_score"))
    alignment_score = to_float_or_none(result.get("alignment_score"))

    # AI gebruikt soms avg_score i.p.v master_score
    if master_score is None:
        master_score = to_float_or_none(result.get("avg_score"))

    # =====================================================
    # 3️⃣ HARD FAILSAFE DEFAULTS (UX belangrijk)
    # =====================================================
    if master_score is None:
        master_score = 50.0

    if alignment_score is None:
        alignment_score = 0.0

    # clamp waarden
    master_score = max(0.0, min(100.0, master_score))
    alignment_score = max(0.0, min(100.0, alignment_score))

    # alignment mag nooit hoger zijn dan master score
    alignment_score = min(alignment_score, master_score)

    # =====================================================
    # 4️⃣ META DATA VEILIG MAKEN
    # =====================================================
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

    # =====================================================
    # 5️⃣ STRING FIELDS SANITIZE
    # =====================================================
    trend = str(result.get("master_trend", "") or "")
    bias = str(result.get("master_bias", "") or "")
    risk = str(result.get("master_risk", "") or "")
    summary = str(result.get("summary", "") or "")

    # =====================================================
    # 6️⃣ OPSLAAN
    # =====================================================
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
                master_score,
                trend,
                bias,
                risk,
                summary,
                json.dumps(meta, ensure_ascii=False),
            ),
        )

    logger.info(
        f"💾 Master stored | user_id={user_id} "
        f"| score={master_score} "
        f"| alignment={alignment_score} "
        f"| warnings={len(data_warnings)}"
    )


# ============================================================
# 🕗 (OPTIONEEL) daily_scores vullen
# ============================================================
def store_daily_scores(conn, insights: Dict[str, dict], user_id: int):
    macro = insights.get("macro", {}).get("avg_score")
    market = insights.get("market", {}).get("avg_score")
    technical = insights.get("technical", {}).get("avg_score")
    setup_score = fetch_setup_score_from_insights(insights)

    if macro is None or market is None or technical is None:
        logger.warning(
            f"⚠️ daily_scores niet bijgewerkt (macro/market/technical missen) user_id={user_id}"
        )
        return

    # ✅ STRATEGY SCORE = market + technical + setup
    strategy_score = calculate_strategy_score(
        market=market,
        technical=technical,
        setup=setup_score,
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_scores
                (
                    report_date,
                    user_id,
                    macro_score,
                    market_score,
                    technical_score,
                    setup_score,
                    strategy_score
                )
            VALUES (
                CURRENT_DATE,
                %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (report_date, user_id)
            DO UPDATE SET
                macro_score = EXCLUDED.macro_score,
                market_score = EXCLUDED.market_score,
                technical_score = EXCLUDED.technical_score,
                setup_score = EXCLUDED.setup_score,
                strategy_score = EXCLUDED.strategy_score;
            """,
            (
                user_id,
                macro,
                market,
                technical,
                setup_score,
                strategy_score,
            ),
        )

    logger.info(
        f"💾 daily_scores bijgewerkt incl. strategy_score={strategy_score} user_id={user_id}"
    )
    
# ============================================================
# 🚀 Per-user runner
# ============================================================
def generate_master_score_for_user(user_id: int):
    logger.info(f"🧠 MASTER Orchestrator | user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # ======================================================
        # 1️⃣ DATA OPHALEN
        # ======================================================
        insights = fetch_today_insights(conn, user_id=user_id)
        numeric = fetch_numeric_scores(conn, user_id=user_id, insights=insights)

        # ======================================================
        # 2️⃣ PRE-FLIGHT DATA WARNINGS (TECHNISCH AFDWINGEN)
        # ======================================================
        data_warnings = []

        # Ontbrekende domeinen
        missing_domains = [
            cat for cat in DOMAIN_CATEGORIES if cat not in insights
        ]
        if missing_domains:
            data_warnings.append(
                f"Ontbrekende domeinen: {', '.join(missing_domains)}"
            )

        # Fallback / niet-verse data
        stale_domains = [
            cat for cat, i in insights.items()
            if i.get("date") != str(date.today())
        ]
        if stale_domains:
            data_warnings.append(
                f"Niet-verse data (fallback): {', '.join(stale_domains)}"
            )

        # Setup / strategy expliciet checken
        if "setup" not in insights:
            data_warnings.append("Geen setup-inzicht beschikbaar")
        if "strategy" not in insights:
            data_warnings.append("Geen strategy-inzicht beschikbaar")

        # Doorgeven aan AI (mag NIET verdwijnen)
        numeric.setdefault("data_warnings", []).extend(data_warnings)

        # ======================================================
        # 3️⃣ MASTER TASK (JOUW DEFINITIEVE VERSIE)
        # ======================================================
        TASK = """
Je bent een master decision orchestrator voor een trading-systeem.

Je doel:
- Synthetiseer bestaande AI-insights tot één consistente besliscontext.
- Je analyseert NIET opnieuw — je ordent, weegt en signaleert.

JE KRIJGT:
- Samenvattingen, scores en trends per domein (macro, market, technical, setup, strategy)
- Numerieke context (daily_scores, setup_score, ai_reflections)

REGELS (ZEER BELANGRIJK):
- Maak GEEN nieuwe analyses
- Verzin GEEN nieuwe data
- Herinterpreteer GEEN indicatoren
- Trek GEEN conclusies die niet expliciet in de input staan
- Gebruik ALLEEN aangeleverde informatie

JE MOET EXPLICIET:
1. Benoemen of domeinen elkaar VERSTERKEN of TEGENSPREKEN
2. Controleren of setup + strategy logisch passen bij macro/market/technical
3. Alignment_score VERLAGEN bij conflicten of ontbrekende data
4. Data_warnings invullen bij:
   - ontbrekende domeinen
   - fallback-data (niet van vandaag)
   - ontbrekende setup- of strategy-inzichten
5. Master_score laten volgen uit samenhang, niet uit optimisme

VERBODEN:
- Educatie
- Marktvoorspellingen
- “Bullish/bearish omdat…”
- Nieuwe trends bedenken

OUTPUT — ALLEEN GELDIGE JSON:

{
  "master_trend": "",
  "master_bias": "",
  "master_risk": "",
  "master_score": 0,
  "alignment_score": 0,
  "weights": {
    "macro": 0.25,
    "market": 0.25,
    "technical": 0.25,
    "setup": 0.15,
    "strategy": 0.10
  },
  "data_warnings": [],
  "summary": "",
  "outlook": "",
  "domains": {
    "macro": {},
    "market": {},
    "technical": {},
    "setup": {},
    "strategy": {}
  }
}

RICHTLIJNEN:
- master_trend/bias/risk: kort, beslisgericht
- summary: 3–4 zinnen, beschrijvend (geen analyse)
- outlook: scenario-gebaseerd, voorwaardelijk (“als… dan…”)
- alignment_score: lager bij conflicten of missende context
"""

        system_prompt = build_system_prompt(
            agent="master",
            task=TASK
        )

        # ======================================================
        # 4️⃣ PROMPT BOUWEN + AI CALL
        # ======================================================
        prompt = build_prompt(insights, numeric)

        result = ask_gpt(
            prompt=prompt,
            system_role=system_prompt
        )

        if not isinstance(result, dict):
            raise ValueError("❌ Master orchestrator gaf geen geldige JSON dict terug")

        # ======================================================
        # 5️⃣ OPSLAAN
        # ======================================================
        store_master_result(conn, result, user_id=user_id)

        if WRITE_DAILY_SCORES:
            store_daily_scores(conn, insights, user_id=user_id)

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

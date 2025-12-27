import logging
from decimal import Decimal
from datetime import date

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# Helpers
# =====================================================
def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def nv(v):
    return v if v not in [None, "", "None"] else "–"


# =====================================================
# 🔒 DEFINITIEVE AI ROL — BESLISCONTEXT
# =====================================================
REPORT_STYLE_GUIDE = """
Je bent de persoonlijke trading-analist van een ervaren Bitcoin-trader.

Belangrijk:
- Je legt NOOIT basisbegrippen uit.
- Je herhaalt geen definities.
- Je schrijft uitsluitend beslisrelevante informatie.

Je rol:
- context duiden
- implicaties benoemen
- risico’s expliciet maken
- aangeven of actie vandaag logisch is of niet

Stijl:
- compact
- zakelijk
- professioneel
- geen marketingtaal

Verplicht:
- Sluit ELKE sectie af met een expliciete conclusie in CAPS.
- Gebruik vaste labels (ACTIE, STATUS, IMPACT).

Als data ontbreekt:
- benoem dit expliciet
- verzin niets
"""


# =====================================================
# 1️⃣ DAILY SCORES — SINGLE SOURCE OF TRUTH
# =====================================================
def get_daily_scores(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    macro_score,
                    technical_score,
                    market_score,
                    setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ Geen daily_scores gevonden (user_id={user_id})")
            return {}

        return {
            "macro_score": to_float(row[0]),
            "technical_score": to_float(row[1]),
            "market_score": to_float(row[2]),
            "setup_score": to_float(row[3]),
            "_meta": {"setup_score_source": "db"},
        }

    finally:
        conn.close()


# =====================================================
# 2️⃣ AI CATEGORY INSIGHTS
# =====================================================
def get_ai_insights(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                  AND date = CURRENT_DATE;
            """, (user_id,))
            rows = cur.fetchall()

        insights = {}
        for cat, avg, trend, bias, risk, summary in rows:
            insights[cat] = {
                "avg_score": to_float(avg),
                "trend": trend or "–",
                "bias": bias or "–",
                "risk": risk or "–",
                "summary": summary or "Geen data."
            }

        return insights

    finally:
        conn.close()


# =====================================================
# 3️⃣ LATEST STRATEGY PER SETUP
# =====================================================
def get_latest_strategy(setup_id: int, user_id: int) -> dict | None:
    if not setup_id:
        return None

    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entry, target, stop_loss, explanation, risk_profile, data
                FROM strategies
                WHERE setup_id = %s AND user_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return None

        entry, target, stop, expl, risk, data = row

        if isinstance(data, dict):
            entry = data.get("entry", entry)
            target = data.get("targets", target)
            stop = data.get("stop_loss", stop)
            expl = data.get("explanation", expl)
            risk = data.get("risk_reward", risk)

        targets = []
        if isinstance(target, str):
            targets = [t.strip() for t in target.split(",") if t.strip()]
        elif isinstance(target, list):
            targets = target

        return {
            "entry": entry or "n.v.t.",
            "targets": targets,
            "stop_loss": stop or "n.v.t.",
            "risk_reward": risk or "?",
            "explanation": expl or "Geen strategie beschikbaar."
        }

    finally:
        conn.close()


# =====================================================
# 4️⃣ MARKET SNAPSHOT
# =====================================================
def get_latest_market_data() -> dict:
    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

        if not row:
            return {}

        return {
            "price": to_float(row[0]),
            "volume": to_float(row[1]),
            "change_24h": to_float(row[2]),
        }

    finally:
        conn.close()


# =====================================================
# 5️⃣ MARKET INDICATOR SCORES
# =====================================================
def get_market_indicator_scores(user_id: int) -> list:
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, interpretation
                FROM market_indicator_scores
                WHERE user_id = %s
                  AND timestamp::date = CURRENT_DATE
                ORDER BY timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        return [
            {
                "indicator": r[0],
                "value": to_float(r[1]),
                "score": to_float(r[2]),
                "interpretation": r[3] or "–"
            }
            for r in rows
        ]

    finally:
        conn.close()


# =====================================================
# 6️⃣ GPT HELPER
# =====================================================
def generate_section(prompt: str) -> str:
    text = ask_gpt_text(prompt, system_role=REPORT_STYLE_GUIDE)
    return text.strip() if text else "AI-generatie mislukt."


# =====================================================
# 7️⃣ PROMPTS — VERSIE C (HYBRIDE)
# =====================================================
def prompt_executive_summary(setup, scores, market, indicators):
    return f"""
Schrijf een executive summary (4–6 zinnen).

Weeg:
- macro-context
- marktconditie
- setup-validiteit

Scores:
Macro: {nv(scores.get('macro_score'))}
Technisch: {nv(scores.get('technical_score'))}
Market: {nv(scores.get('market_score'))}
Setup: {nv(scores.get('setup_score'))}

Prijs: ${nv(market.get('price'))}
24h: {nv(market.get('change_24h'))}%

Sluit af met exact:
BESLISSING VANDAAG: ACTIE_VANDAAG / GEEN_ACTIE / OBSERVEREN
CONFIDENCE: LAAG / MIDDEL / HOOG
"""


def prompt_macro(ai):
    return f"""
Beschrijf de macro-context (4–6 zinnen).

Trend: {ai.get('trend')}
Bias: {ai.get('bias')}
Risico: {ai.get('risk')}

Sluit af met exact:
MACRO-IMPACT: STEUNEND / NEUTRAAL / REMMEND
"""


def prompt_setup_validation(setup, scores):
    return f"""
Beoordeel of de setup vandaag valide is.

Setup: {setup.get('name')} ({setup.get('timeframe')})

Scores:
Macro: {nv(scores.get('macro_score'))}
Technisch: {nv(scores.get('technical_score'))}
Market: {nv(scores.get('market_score'))}

Geef:
- korte validatie
- belangrijkste reden
- wat moet verbeteren

Sluit af met exact:
SETUP-STATUS: GO / NO-GO / CONDITIONAL
RELEVANTIE: VANDAAG / KOMENDE_DAGEN / LATER
"""


def prompt_strategy_implication(strategy):
    return f"""
Analyseer de strategie-implicatie.

Entry: {strategy['entry']}
Targets: {strategy['targets']}
Stop-loss: {strategy['stop_loss']}

Beoordeel:
- uitvoerbaarheid vandaag
- discipline-risico
- trigger-afhankelijkheid

Sluit af met exact:
STRATEGIE-STATUS: UITVOERBAAR_VANDAAG / WACHT_OP_TRIGGER / NIET_ACTUEEL
"""


def prompt_outlook():
    return """
Geef een korte vooruitblik (2–4 zinnen).

Scenario’s:
- bullish
- bearish
- consolidatie

Benoem per scenario de implicatie voor actie of geduld.
"""


# =====================================================
# 8️⃣ MAIN REPORT BUILDER
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC", user_id: int = None) -> dict:
    logger.info(f"📄 Rapport genereren | {symbol} | user_id={user_id}")

    setup = sanitize_json_input(
        get_latest_setup_for_symbol(symbol=symbol, user_id=user_id) or {},
        context="setup"
    )

    scores = get_daily_scores(user_id)
    ai = get_ai_insights(user_id)
    market = get_latest_market_data()
    indicators = get_market_indicator_scores(user_id)

    if scores.get("setup_score") is None and ai.get("setup"):
        scores["setup_score"] = ai["setup"].get("avg_score")
        scores["_meta"]["setup_score_source"] = "ai"

    strategy = get_latest_strategy(setup.get("id"), user_id) or {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "?",
        "explanation": "Geen strategie beschikbaar."
    }

    return {
        "executive_summary": generate_section(
            prompt_executive_summary(setup, scores, market, indicators)
        ),
        "macro_summary": generate_section(
            prompt_macro(ai.get("macro", {}))
        ),
        "setup_validation": generate_section(
            prompt_setup_validation(setup, scores)
        ),
        "strategy_implication": generate_section(
            prompt_strategy_implication(strategy)
        ),
        "outlook": generate_section(
            prompt_outlook()
        ),
        "scores": scores,
        "strategy": strategy,
        "market_data": market,
        "market_indicator_scores": indicators,
    }

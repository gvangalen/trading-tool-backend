import os
import logging
import json
from decimal import Decimal

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.openai_client import ask_gpt_text

# =====================================================
# Logging
# =====================================================
LOG_FILE = "/tmp/daily_report_debug.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# =====================================================
# Helpers
# =====================================================
def log_and_print(msg: str):
    logger.info(msg)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass
    print(msg)


def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        val = obj.get(key, fallback)
        if isinstance(val, Decimal):
            return to_float(val)
        return val if val not in [None, "None", ""] else fallback
    return fallback


def nv(v):
    return v if v not in [None, "", "None"] else "–"


# =====================================================
# Report style
# =====================================================
REPORT_STYLE_GUIDE = """
Je bent een professionele Bitcoin- en macro-analist.
Schrijf in het Nederlands in de stijl van een premium nieuwsbrief
(een mix van Glassnode, BitcoinStrategy en TIA).
"""


# =====================================================
# 1. SCORES (USER-SPECIFIC)
# =====================================================
def get_scores_from_db(user_id: int):
    try:
        scores = get_scores_for_symbol(user_id=user_id, include_metadata=True)
        if scores:
            return {k: to_float(v) for k, v in scores.items()}
    except Exception:
        pass

    return {
        "macro_score": None,
        "technical_score": None,
        "setup_score": None,
        "market_score": None,
    }


# =====================================================
# 2. AI INSIGHTS (USER-SPECIFIC)
# =====================================================
def get_ai_insights_from_db(user_id: int):
    try:
        conn = get_db_connection()
        if not conn:
            return {}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE
                  AND user_id = %s;
            """, (user_id,))
            rows = cur.fetchall()

        insights = {}
        for cat, avg_score, trend, bias, risk, summary in rows:
            insights[cat] = {
                "avg_score": to_float(avg_score),
                "trend": trend or "–",
                "bias": bias or "–",
                "risk": risk or "–",
                "summary": summary or "Geen data."
            }

        return insights

    except Exception:
        logger.error("Fout in get_ai_insights_from_db()", exc_info=True)
        return {}

    finally:
        try:
            conn.close()
        except Exception:
            pass


# =====================================================
# 3. STRATEGY (USER-SPECIFIC)
# =====================================================
def get_latest_strategy_for_setup(setup_id: int, user_id: int):
    if not setup_id:
        return None

    try:
        conn = get_db_connection()
        if not conn:
            return None

        with conn.cursor() as cur:
            cur.execute("""
                SELECT entry, target, stop_loss, explanation, risk_profile, data, created_at
                FROM strategies
                WHERE setup_id = %s
                  AND user_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return None

        entry, target, stop_loss, explanation, risk_profile, data_json, created_at = row

        if isinstance(data_json, dict):
            entry = data_json.get("entry", entry)
            target = data_json.get("targets", target)
            stop_loss = data_json.get("stop_loss", stop_loss)
            explanation = data_json.get("explanation", explanation)
            risk_profile = data_json.get("risk_reward", risk_profile)

        targets = []
        if isinstance(target, str):
            targets = [t.strip() for t in target.split(",") if t.strip()]
        elif isinstance(target, list):
            targets = target

        return {
            "entry": entry or "n.v.t.",
            "targets": targets,
            "stop_loss": stop_loss or "n.v.t.",
            "risk_reward": risk_profile or "?",
            "explanation": explanation or "Geen uitleg beschikbaar."
        }

    except Exception as e:
        logger.error(f"Strategy fout: {e}", exc_info=True)
        return None

    finally:
        try:
            conn.close()
        except Exception:
            pass


# =====================================================
# 4. MARKET DATA (GLOBAL SNAPSHOT)
# =====================================================
def get_latest_market_data():
    try:
        conn = get_db_connection()
        if not conn:
            return {}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

        if row:
            return {
                "price": to_float(row[0]),
                "volume": to_float(row[1]),
                "change_24h": to_float(row[2]),
            }

    except Exception:
        logger.error("Market data fout", exc_info=True)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {}


# =====================================================
# 5. MARKET INDICATOR SCORES (DAILY MARKET ANALYSIS)
# =====================================================
def get_market_indicator_scores(user_id: int):
    try:
        conn = get_db_connection()
        if not conn:
            return []

        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, value, score, interpretation
                FROM market_indicator_scores
                WHERE user_id = %s
                ORDER BY timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        results = []
        for ind, value, score, interp in rows:
            results.append({
                "indicator": ind,
                "value": to_float(value),
                "score": to_float(score),
                "interpretation": interp or "–",
            })

        return results

    except Exception:
        logger.error("Fout in get_market_indicator_scores()", exc_info=True)
        return []

    finally:
        try:
            conn.close()
        except Exception:
            pass


# =====================================================
# 6. GPT helper
# =====================================================
def generate_section(prompt: str, retries: int = 3) -> str:
    text = ask_gpt_text(prompt, system_role=REPORT_STYLE_GUIDE, retries=retries)
    return text.strip() if text else "AI-generatie mislukt."


# =====================================================
# 7. PROMPTS
# =====================================================
def prompt_for_btc_summary(setup, scores, market_data, market_indicators):
    lines = []
    for m in market_indicators[:3]:
        lines.append(
            f"- {m['indicator']}: waarde {nv(m['value'])}, score {nv(m['score'])} → {nv(m['interpretation'])}"
        )

    market_block = "\n".join(lines) if lines else "–"

    return f"""
Schrijf een krachtige openingssectie (6–8 zinnen).

Scores:
- Macro: {nv(scores.get("macro_score"))}
- Technisch: {nv(scores.get("technical_score"))}
- Setup: {nv(scores.get("setup_score"))}
- Markt: {nv(scores.get("market_score"))}

Live markt:
- Prijs: ${nv(safe_get(market_data, "price"))}
- Volume: {nv(safe_get(market_data, "volume"))}
- 24h verandering: {nv(safe_get(market_data, "change_24h"))}%

Dagelijkse market-indicatoren:
{market_block}

Actieve setup:
- {nv(setup.get("name"))} ({nv(setup.get("timeframe"))})
"""


def prompt_for_macro_summary(scores, ai_insights):
    macro = ai_insights.get("macro", {})
    return f"""
Maak een compacte macro-update (5–8 zinnen).

Macro score vandaag: {nv(scores.get("macro_score"))}
Trend: {nv(macro.get("trend"))}
Bias: {nv(macro.get("bias"))}
Risico: {nv(macro.get("risk"))}
Samenvatting: {nv(macro.get("summary"))}
"""


def prompt_for_setup_checklist(setup):
    return f"""
Schrijf 6–8 bullets over sterktes, zwaktes, activatie, invalidatie en praktische tips.

Setup: {nv(setup.get('name'))} ({nv(setup.get('timeframe'))})
"""


def prompt_for_priorities(setup, scores):
    return "Genereer 3–7 dagelijkse prioriteiten voor traders."


def prompt_for_wyckoff_analysis(setup):
    return f"""
Maak een Wyckoff-analyse (5–10 zinnen).
Setup: {nv(setup.get('name'))}
Phase: {nv(setup.get('wyckoff_phase'))}
"""


def prompt_for_recommendations(strategy):
    return f"""
Schrijf een premium strategie-uitleg (6–10 zinnen).

Entry: {nv(strategy['entry'])}
Targets: {strategy['targets']}
Stop-loss: {nv(strategy['stop_loss'])}
"""


def prompt_for_conclusion(scores):
    return "Schrijf een slotconclusie (4–8 zinnen)."


def prompt_for_outlook(setup):
    return "Schrijf een 2–5 dagen outlook met bullish, bearish en sideways scenario."


# =====================================================
# 8. MAIN REPORT BUILDER
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC", user_id: int = None) -> dict:
    log_and_print(f"Rapportgeneratie gestart voor {symbol} (user_id={user_id})")

    setup = sanitize_json_input(
        get_latest_setup_for_symbol(symbol=symbol, user_id=user_id) or {},
        context="setup"
    )

    scores = sanitize_json_input(get_scores_from_db(user_id=user_id), context="scores")
    ai_insights = get_ai_insights_from_db(user_id=user_id)
    market_data = get_latest_market_data()
    market_indicators = get_market_indicator_scores(user_id=user_id)

    strategy = get_latest_strategy_for_setup(setup.get("id"), user_id=user_id) or {
        "entry": "n.v.t.",
        "targets": [],
        "stop_loss": "n.v.t.",
        "risk_reward": "?",
        "explanation": "Geen strategy beschikbaar."
    }

    report = {
        "btc_summary": generate_section(
            prompt_for_btc_summary(setup, scores, market_data, market_indicators)
        ),
        "macro_summary": generate_section(
            prompt_for_macro_summary(scores, ai_insights)
        ),
        "setup_checklist": generate_section(
            prompt_for_setup_checklist(setup)
        ),
        "priorities": generate_section(
            prompt_for_priorities(setup, scores)
        ),
        "wyckoff_analysis": generate_section(
            prompt_for_wyckoff_analysis(setup)
        ),
        "recommendations": generate_section(
            prompt_for_recommendations(strategy)
        ),
        "conclusion": generate_section(
            prompt_for_conclusion(scores)
        ),
        "outlook": generate_section(
            prompt_for_outlook(setup)
        ),

        # Raw data
        "scores": scores,
        "market_data": market_data,
        "market_indicator_scores": market_indicators,
        "strategy": strategy,
    }

    log_and_print("Rapport succesvol gegenereerd.")
    return report


if __name__ == "__main__":
    print(json.dumps(
        generate_daily_report_sections("BTC", user_id=1),
        indent=2,
        ensure_ascii=False
    ))

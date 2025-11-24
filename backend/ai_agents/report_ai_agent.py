import os
import logging
import json

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.ai_agents.strategy_ai_agent import generate_strategy_from_setup
from backend.utils.json_utils import sanitize_json_input
from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.openai_client import ask_gpt_text

# =====================================================
# 🪵 Logging
# =====================================================
LOG_FILE = "/tmp/daily_report_debug.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def log_and_print(msg: str):
    logger.info(msg)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass
    print(msg)


def safe_get(obj, key, fallback="–"):
    if isinstance(obj, dict):
        return obj.get(key, fallback)
    return fallback


# =====================================================
# 🎨 PREMIUM REPORT STYLE – centraal gebruikt door ALLES
# =====================================================
REPORT_STYLE_GUIDE = """
Je bent een professionele Bitcoin- en macro-analist.
Schrijf in het Nederlands in de stijl van een premium nieuwsbrief
(een mix van Glassnode, BitcoinStrategy en TIA).

Richtlijnen:
- Schrijf beknopt, analytisch en menselijk.
- Focus op context, beweging, risico, niveaus en kansen.
- Gebruik korte alinea’s en duidelijke bullets waar relevant.
- Leg geen basisconcepten uit (zoals RSI, MA, support/resistance).
- Geen hype, geen doom — professioneel en rustig.
- Elke sectie heeft een unieke functie; geen herhaling.
- Vermijd schoolboek-toon, schrijf alsof het een marktupdate is voor ervaren traders.
"""

# =====================================================
# 📊 Scores uit DB
# =====================================================
def get_scores_from_db():
    try:
        scores = get_scores_for_symbol(include_metadata=True)
        if scores:
            log_and_print(f"📊 Live scores geladen uit DB: {scores}")
            return scores
    except Exception as e:
        log_and_print(f"⚠️ Live scoreberekening mislukt: {e}")

    conn = get_db_connection()
    if not conn:
        log_and_print("❌ Geen DB-verbinding voor fallback-scores.")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, setup_score, market_score
                FROM daily_scores
                ORDER BY report_date DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                scores = {
                    "macro_score": row[0],
                    "technical_score": row[1],
                    "setup_score": row[2],
                    "market_score": row[3],
                }
                log_and_print(f"📊 Fallback scores geladen: {scores}")
                return scores
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen fallback-scores: {e}")
    finally:
        conn.close()

    return {}


# =====================================================
# 🧠 AI insights laden
# =====================================================
def get_ai_insights_from_db():
    conn = get_db_connection()
    if not conn:
        log_and_print("❌ Geen DB-verbinding voor AI insights.")
        return {}

    insights = {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE date = CURRENT_DATE;
            """)
            rows = cur.fetchall()
            for category, avg_score, trend, bias, risk, summary in rows:
                insights[category] = {
                    "score": float(avg_score or 0),
                    "trend": trend,
                    "bias": bias,
                    "risk": risk,
                    "summary": summary,
                }
            log_and_print(f"🧩 AI insights geladen: {list(insights.keys())}")
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen AI insights: {e}")
    finally:
        conn.close()

    return insights


# =====================================================
# 📈 Laatste marktdata
# =====================================================
def get_latest_market_data():
    conn = get_db_connection()
    if not conn:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, volume, change_24h
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                return {
                    "price": round(row[0], 2),
                    "volume": row[1],
                    "change_24h": row[2],
                }
    except Exception as e:
        log_and_print(f"❌ Fout bij ophalen market_data: {e}")
        return {}
    finally:
        conn.close()


# =====================================================
# 🧠 Premium text generator
# =====================================================
def generate_section(prompt: str, retries: int = 3) -> str:
    text = ask_gpt_text(
        prompt,
        system_role=REPORT_STYLE_GUIDE,
        retries=retries,
    )
    if not text:
        return "AI-generatie mislukt of gaf geen output."
    return text.strip()


# =====================================================
# 🧩 NEW PREMIUM PROMPTS — Glassnode + Newsletter stijl
# =====================================================
def prompt_for_btc_summary(setup, scores, market_data=None, ai_insights=None) -> str:
    price = safe_get(market_data or {}, "price")
    volume = safe_get(market_data or {}, "volume")
    change = safe_get(market_data or {}, "change_24h")

    macro = safe_get(scores, "macro_score")
    tech = safe_get(scores, "technical_score")
    setup_score = safe_get(scores, "setup_score")
    market_score = safe_get(scores, "market_score")

    master = ai_insights.get("score") if isinstance(ai_insights, dict) else {}
    m_score = safe_get(master, "score")
    m_trend = safe_get(master, "trend")
    m_bias = safe_get(master, "bias")
    m_risk = safe_get(master, "risk")

    return f"""
Schrijf een krachtige openingssectie (max 6–8 zinnen) voor een premium Bitcoin-rapport.
Beschrijf:
- De huidige staat van de markt (trend, momentum, sentiment).
- Hoe de vier scores samen het beeld vormen:
  Macro {macro}, Technisch {tech}, Setup {setup_score}, Markt {market_score}.
- Hoe de AI Master Score de situatie interpreteert:
  Score {m_score}, trend {m_trend}, bias {m_bias}, risico {m_risk}.
- Hoe prijs ${price}, volume {volume} en 24u verandering {change}% passen in het verhaal.
- Benoem indien relevant één belangrijk prijsniveau dat vandaag telt.

Setup context:
Naam: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
"""


def prompt_for_macro_summary(scores, ai_insights=None) -> str:
    macro_score = safe_get(scores, "macro_score")
    macro = ai_insights.get("macro") if isinstance(ai_insights, dict) else {}

    trend = safe_get(macro, "trend")
    bias = safe_get(macro, "bias")
    risk = safe_get(macro, "risk")
    summary = safe_get(macro, "summary")

    return f"""
Maak een compacte macro-economische marktupdate (5–8 zinnen).

Gebruik deze data:
- Macro score: {macro_score}
- Trend: {trend}
- Bias: {bias}
- Risico: {risk}
- AI macro samenvatting: {summary}

Beschrijf:
- Hoe de macro-omgeving risk assets beïnvloedt.
- Of macro nu eerder rugwind of tegenwind geeft voor Bitcoin.
- Wat de belangrijkste drivers zijn (liquiditeit, rente, inflatie, equities).
"""


def prompt_for_setup_checklist(setup) -> str:
    return f"""
Schrijf een concrete 'Setup Snapshot' voor traders.
Geen basisuitleg, wel toepasbare bullets.

Setup:
- Naam: {safe_get(setup, 'name')}
- Timeframe: {safe_get(setup, 'timeframe')}
- Type: {safe_get(setup, 'type', '–')}
- Indicatoren: {safe_get(setup, 'indicators', [])}

Beschrijf in maximaal 8 bullets:
- Wat deze setup sterk maakt
- Wat hem zwak maakt
- Wat nodig is voor activatie
- Wat invalidatie is
- Hoe een trader het beste met deze setup omgaat
"""


def prompt_for_priorities(setup, scores) -> str:
    return f"""
Genereer een sectie 'Dagelijkse Prioriteiten' voor traders.
Maximaal 3–7 bullets.

Context:
- Setup: {safe_get(setup, 'name')} ({safe_get(setup, 'timeframe')})
- Scores: {scores}

Beschrijf:
- Concrete focuspunten: niveau X, volume, trendbreuken.
- Risico's: fakeouts, liquiditeitsgrabs, sentiment shifts.
- Wanneer wel/niet handelen vandaag.
"""


def prompt_for_wyckoff_analysis(setup) -> str:
    return f"""
Maak een Wyckoff-analyse van de huidige marktstructuur (max 5–10 zinnen).

Data:
- Wyckoff fase (ruw): {safe_get(setup, 'wyckoff_phase')}
- Extra context: {safe_get(setup, 'explanation')}

Beschrijf:
- In welke fase BTC waarschijnlijk zit
- Wat dit zegt over grote spelers
- Waar de kans ligt voor de volgende grote beweging
- Wat de structurele invalidatie is
"""


def prompt_for_recommendations(strategy) -> str:
    return f"""
Schrijf een premium 'Aanbevolen Strategie' sectie (6–10 zinnen).

Data:
- Entry: {safe_get(strategy, 'entry')}
- Targets: {safe_get(strategy, 'targets')}
- Stop-loss: {safe_get(strategy, 'stop_loss')}
- Ruwe uitleg: {safe_get(strategy, 'explanation')}

Beschrijf:
- Logica achter het plan
- Hoe een trader idealiter instapt (delen/confirmatie/pas na reclamatie)
- Wanneer je targets opschuift of trade afsluit
- Duidelijk benadrukken dat dit geen financieel advies is
"""


def prompt_for_conclusion(scores, ai_insights=None) -> str:
    macro = safe_get(scores, "macro_score")
    tech = safe_get(scores, "technical_score")
    setup_score = safe_get(scores, "setup_score")
    market = safe_get(scores, "market_score")

    master = ai_insights.get("score") if isinstance(ai_insights, dict) else {}
    ms = safe_get(master, "score")
    mt = safe_get(master, "trend")
    mb = safe_get(master, "bias")
    mr = safe_get(master, "risk")
    msum = safe_get(master, "summary")

    return f"""
Schrijf een slotconclusie (4–8 zinnen).
Beschrijf:
- Wat macro ({macro}), techniek ({tech}), setup ({setup_score}) en markt ({market}) samen zeggen.
- Hoe de AI Master Score (score {ms}, trend {mt}, bias {mb}, risico {mr}) dit bevestigt of nuanceert.
- Of de markt zich in een fase van kansen, risico's of neutraliteit bevindt.
- Wat het algemene 'gevoel' van de dag is voor traders.
"""


def prompt_for_outlook(setup) -> str:
    return f"""
Maak een vooruitblik van 2–5 dagen (5–9 zinnen).
Gebruik scenario-denken:
- Bullish scenario: trigger + targets
- Bearish scenario: trigger + downside levels
- Sideways scenario: range + signalen

Setup context:
Naam: {safe_get(setup, 'name')}
Timeframe: {safe_get(setup, 'timeframe')}
"""


# =====================================================
# 🚀 Main Report Builder
# =====================================================
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    log_and_print(f"🚀 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    scores_raw = get_scores_from_db()
    ai_insights = get_ai_insights_from_db()
    market_data = get_latest_market_data()

    log_and_print(f"📦 setup_raw: {repr(setup_raw)[:180]}")
    log_and_print(f"📦 scores_raw: {repr(scores_raw)[:180]}")
    log_and_print(f"📦 ai_insights: {repr(ai_insights)[:180]}")

    setup = sanitize_json_input(setup_raw, context="setup")
    scores = sanitize_json_input(scores_raw, context="scores")
    strategy_raw = generate_strategy_from_setup(setup)
    strategy = sanitize_json_input(strategy_raw, context="strategy")

    if not isinstance(setup, dict) or not setup:
        return {"error": "Ongeldige setup"}
    if not isinstance(scores, dict) or not scores:
        return {"error": "Ongeldige scores"}
    if not isinstance(strategy, dict) or not strategy:
        return {"error": "Ongeldige strategy"}

    try:
        report = {
            "btc_summary": generate_section(
                prompt_for_btc_summary(setup, scores, market_data, ai_insights)
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
                prompt_for_conclusion(scores, ai_insights)
            ),
            "outlook": generate_section(
                prompt_for_outlook(setup)
            ),

            # ruwe scores
            "macro_score": safe_get(scores, "macro_score", 0),
            "technical_score": safe_get(scores, "technical_score", 0),
            "setup_score": safe_get(scores, "setup_score", 0),
            "market_score": safe_get(scores, "market_score", 0),

            # AI info
            "ai_insights": ai_insights,
            "ai_master_score": ai_insights.get("score", {}),

            "market_data": market_data,
        }
        log_and_print(f"✅ Rapport succesvol gegenereerd ({len(report)} velden)")
        return report

    except Exception as e:
        log_and_print(f"❌ Exception tijdens rapportgeneratie: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    result = generate_daily_report_sections("BTC")
    print("\n🎯 RESULTAAT:")
    print(json.dumps(result, indent=2, ensure_ascii=False))

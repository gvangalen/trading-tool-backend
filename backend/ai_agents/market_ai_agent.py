import logging
import traceback
import json
from datetime import datetime
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🪙 MARKET AI AGENT – met scoreregels + reflectielaag
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight():
    """
    Analyseert marktdata (prijs, volume, momentum, volatiliteit)
    met hun scoreregels en genereert AI-interpretatie én reflectie per indicator.

    ⚙️ AI krijgt zowel dagelijkse als 7-daagse data om trend en kracht te beoordelen.
    """
    logger.info("🪙 Start Market AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # === 1️⃣ Scoreregels ophalen voor marktindicatoren ===
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, rule_range, score, interpretation, action
                FROM market_indicator_rules
                ORDER BY indicator ASC, score ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for r in rule_rows:
            indicator, rule_range, score, interpretation, action = r
            rules_by_indicator.setdefault(indicator, []).append({
                "range": rule_range,
                "score": score,
                "interpretation": interpretation,
                "action": action
            })

        logger.info(f"📘 Regels geladen voor {len(rules_by_indicator)} marktindicatoren.")

        # === 2️⃣ Laatste marktdata ophalen ===
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 'price' AS indicator, price AS value, change_24h AS score, volume, timestamp
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            last_snapshot = cur.fetchone()

            cur.execute("""
                SELECT date, open, high, low, close, change, volume
                FROM market_data_7d
                ORDER BY date DESC
                LIMIT 7;
            """)
            ohlc_rows = cur.fetchall()

        if not last_snapshot or not ohlc_rows:
            logger.warning("⚠️ Onvoldoende marktdata gevonden.")
            return

        price_info = {
            "price": float(last_snapshot[1]),
            "change_24h": float(last_snapshot[2]),
            "volume": float(last_snapshot[3])
        }

        # OHLC samenvatting
        ohlc_summary = [
            f"{r[0]}: O={r[1]}, H={r[2]}, L={r[3]}, C={r[4]}, Δ{r[5]}%, Vol={r[6]}"
            for r in ohlc_rows
        ]
        ohlc_text = "\n".join(ohlc_summary)

        # === 3️⃣ Combineer met regels ===
        data_text = f"""
        Actuele marktdata:
        Prijs: {price_info['price']}, 24h: {price_info['change_24h']}%, Volume: {price_info['volume']}
        
        Laatste 7 dagen OHLC:
        {ohlc_text}

        Scoreregels:
        {json.dumps(rules_by_indicator, ensure_ascii=False)}
        """

        # === 4️⃣ Contextuele interpretatie (trend/bias/risk) ===
        prompt_context = f"""
        Je bent een marktanalist-AI gespecialiseerd in Bitcoin.
        Hieronder zie je recente marktdata met OHLC, volume en prijsverandering:

        {data_text}

        Geef je antwoord als JSON met:
        - trend: bullish, bearish of neutraal
        - momentum: sterk, zwak of neutraal
        - volatility: laag, gemiddeld of hoog
        - liquidity: goed, matig of laag
        - summary: max 2 zinnen met interpretatie van de marktstructuur
        - top_signals: lijst van de belangrijkste prijs/volume-observaties
        """

        ai_response_context = ask_gpt(prompt_context)
        try:
            ai_context = json.loads(ai_response_context)
        except Exception:
            logger.warning("⚠️ AI-context kon niet als JSON worden geparsed.")
            ai_context = {"summary": ai_response_context[:200]}

        # === 5️⃣ Reflectie per indicator ===
        prompt_reflection = f"""
        Beoordeel de marktcondities hieronder.
        Geef voor elke factor (prijs, volume, volatiliteit, momentum):
        - ai_score (0-100): jouw beoordeling van de huidige marktsterkte
        - compliance (0-100): volgt de gebruiker zijn plan of wijkt hij af?
        - comment: korte reflectie (max 1 zin)
        - recommendation: 1 zin met advies of aandachtspunt

        {data_text}

        Geef als JSON-lijst, bijvoorbeeld:
        [
          {{
            "indicator": "volume",
            "ai_score": 60,
            "compliance": 80,
            "comment": "Volume stijgt, maar prijs daalt — distributie mogelijk",
            "recommendation": "Wacht op bevestiging van vraagsteun"
          }},
          ...
        ]
        """

        ai_response_reflection = ask_gpt(prompt_reflection)
        try:
            ai_reflections = json.loads(ai_response_reflection)
            if not isinstance(ai_reflections, list):
                ai_reflections = []
        except Exception:
            logger.warning("⚠️ Reflectie kon niet als JSON worden geparsed.")
            ai_reflections = []

        logger.info(f"🧠 AI market interpretatie: {ai_context}")
        logger.info(f"🪞 AI market reflecties: {len(ai_reflections)} items")

        # === 6️⃣ Opslaan interpretatie (categorie samenvatting) ===
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('market', NULL, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date) DO UPDATE SET
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                ai_context.get("trend"),
                ai_context.get("momentum"),
                ai_context.get("volatility"),
                ai_context.get("summary"),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # === 7️⃣ Opslaan individuele reflecties ===
        for r in ai_reflections:
            indicator = r.get("indicator")
            ai_score = r.get("ai_score")
            compliance = r.get("compliance")
            comment = r.get("comment")
            recommendation = r.get("recommendation")

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections (
                        category, indicator, raw_score, ai_score, compliance, comment, recommendation
                    )
                    VALUES ('market', %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (indicator, ai_score, compliance, comment, recommendation))

        conn.commit()
        logger.info("✅ Market AI insights + reflecties succesvol opgeslagen.")

    except Exception:
        logger.error("❌ Fout in Market AI Agent:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

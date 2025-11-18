import logging
import traceback
import json
from datetime import datetime

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================================================
# 🪙 MARKET AI AGENT – met DB-scoreregels + reflectielaag
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight():
    """
    Analyseert marktdata (prijs, volume, change_24h, 7d OHLC)
    in combinatie met de scoreregels in `market_indicator_rules`
    en de gecombineerde market-score via `generate_scores_db("market")`.

    Output:
    - AI-samenvatting voor categorie 'market' in ai_category_insights
    - Per-indicator reflecties in ai_reflections
    """

    logger.info("🪙 Start Market AI Agent...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Scoreregels voor market-indicatoren ophalen
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                ORDER BY indicator ASC, range_min ASC;
            """)
            rule_rows = cur.fetchall()

        rules_by_indicator = {}
        for r in rule_rows:
            indicator, range_min, range_max, score, trend, interpretation, action = r
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(range_min),
                "range_max": float(range_max),
                "score": int(score),
                "trend": trend,
                "interpretation": interpretation,
                "action": action,
            })

        logger.info(f"📘 Regels geladen voor {len(rules_by_indicator)} market-indicatoren.")

        # =========================================================
        # 2️⃣ Laatste market snapshot + 7d OHLC uit DB
        # =========================================================
        with conn.cursor() as cur:
            # Laatste point uit market_data
            cur.execute("""
                SELECT price, change_24h, volume, timestamp
                FROM market_data
                WHERE symbol = 'BTC'
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
            last_snapshot = cur.fetchone()

            # Laatste 7 dagen OHLC
            cur.execute("""
                SELECT date, open, high, low, close, change, volume
                FROM market_data_7d
                WHERE symbol = 'BTC'
                ORDER BY date DESC
                LIMIT 7;
            """)
            ohlc_rows = cur.fetchall()

        if not last_snapshot:
            logger.warning("⚠️ Geen entry gevonden in market_data voor BTC.")
            return

        if not ohlc_rows:
            logger.warning("⚠️ Geen data gevonden in market_data_7d voor BTC.")
            return

        price, change_24h, volume, ts = last_snapshot
        price_info = {
            "price": float(price) if price is not None else None,
            "change_24h": float(change_24h) if change_24h is not None else None,
            "volume": float(volume) if volume is not None else None,
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
        }

        # 7d OHLC samenvatting (recentste eerst in DB → omdraaien voor leesbaarheid)
        ohlc_rows_sorted = list(ohlc_rows)
        ohlc_rows_sorted.reverse()
        ohlc_summary = [
            {
                "date": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
                "open": float(r[1]) if r[1] is not None else None,
                "high": float(r[2]) if r[2] is not None else None,
                "low": float(r[3]) if r[3] is not None else None,
                "close": float(r[4]) if r[4] is not None else None,
                "change": float(r[5]) if r[5] is not None else None,
                "volume": float(r[6]) if r[6] is not None else None,
            }
            for r in ohlc_rows_sorted
        ]

        # Compacte tekstweergave voor prompt
        ohlc_text_lines = [
            f"{row['date']}: O={row['open']}, H={row['high']}, L={row['low']}, "
            f"C={row['close']}, Δ={row['change']}%, Vol={row['volume']}"
            for row in ohlc_summary
        ]
        ohlc_text = "\n".join(ohlc_text_lines)

        # =========================================================
        # 3️⃣ Market-score via DB-logica (generate_scores_db("market"))
        # =========================================================
        market_scores = generate_scores_db("market")
        market_avg = market_scores.get("total_score", 0)
        market_score_items = market_scores.get("scores", {})

        # Topcontributors voor prompt (max 3)
        top_contributors = sorted(
            market_score_items.items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True
        )[:3]

        top_contributors_pretty = [
            {
                "indicator": name,
                "value": data.get("value"),
                "score": data.get("score"),
                "trend": data.get("trend"),
                "interpretation": data.get("interpretation"),
            }
            for name, data in top_contributors
        ]

        # =========================================================
        # 4️⃣ Prompt: contextuele interpretatie (trend / bias / risk)
        # =========================================================
        data_payload = {
            "price_snapshot": price_info,
            "ohlc_7d": ohlc_summary,
            "market_rules": rules_by_indicator,
            "market_avg_score": market_avg,
            "market_top_contributors": top_contributors_pretty,
            "ohlc_text": ohlc_text,
        }

        prompt_context = f"""
Je bent een marktanalist-AI gespecialiseerd in Bitcoin.

Hieronder zie je:
- De laatste BTC marktdata (prijs, 24h verandering, volume)
- De afgelopen 7 dagen OHLC + volume
- Scoreregels voor marktindicatoren (range_min/range_max → score, trend, interpretatie, actie)
- De samengestelde market-score en belangrijkste contributors uit een eerder scoringssysteem.

DATA:
{json.dumps(data_payload, ensure_ascii=False, indent=2)}

Geef je antwoord in **geldige JSON** met exact deze keys:
- trend: "bullish", "bearish" of "neutraal"
- momentum: "sterk", "zwak" of "neutraal"
- volatility: "laag", "gemiddeld" of "hoog"
- liquidity: "goed", "matig" of "laag"
- summary: een korte samenvatting (max 2 zinnen) van de huidige marktsituatie
- top_signals: een lijst met de belangrijkste 3 observaties (korte bulletteksten)
"""

        # 👉 NU: direct via ask_gpt (al JSON-geparsed)
        ai_context = ask_gpt(
            prompt_context,
            system_role=(
                "Je bent een professionele crypto-marktanalist. "
                "Antwoord in het Nederlands en in geldige JSON."
            ),
        )

        if not isinstance(ai_context, dict):
            logger.warning("⚠️ AI-context is geen dict, fallback naar lege structuur.")
            # Als openai_client een fallback dict met 'raw_text' teruggeeft:
            raw = ""
            if isinstance(ai_context, dict) and "raw_text" in ai_context:
                raw = ai_context.get("raw_text", "")[:300]
            ai_context = {
                "trend": None,
                "momentum": None,
                "volatility": None,
                "liquidity": None,
                "summary": raw,
                "top_signals": [],
            }

        # =========================================================
        # 5️⃣ Prompt: reflectie per indicator (volume / prijs / volatiliteit etc.)
        # =========================================================
        prompt_reflection = f"""
Je bent dezelfde marktanalist-AI. Gebruik dezelfde data hieronder:

{json.dumps(data_payload, ensure_ascii=False, indent=2)}

Maak nu een korte reflectie per factor:
- prijs
- volume
- change_24h
- volatiliteit (afgeleid uit de laatste 7 dagen)
- marktdruk (bull vs bear, af te leiden uit prijs + volume)

Geef je antwoord als **geldige JSON-lijst**.
Elke entry moet deze keys hebben:
- indicator: bijvoorbeeld "price", "volume", "change_24h", "volatiliteit", "druk"
- ai_score: integer 0–100 (hoe sterk/uitgesproken is deze factor nu)
- compliance: integer 0–100 (hoe goed deze situatie past bij een gedisciplineerde swingtrader setup)
- comment: max 1 zin met observatie
- recommendation: max 1 zin met advies of aandachtspunt
"""

        ai_reflections = ask_gpt(
            prompt_reflection,
            system_role=(
                "Je bent een professionele crypto-marktanalist. "
                "Antwoord in het Nederlands en in geldige JSON."
            ),
        )

        # ask_gpt kan een lijst of dict teruggeven
        if isinstance(ai_reflections, list):
            reflections_list = ai_reflections
        elif isinstance(ai_reflections, dict) and "raw_text" in ai_reflections:
            logger.warning("⚠️ Reflectie niet als lijst ontvangen, raw_text aanwezig – fallback naar lege lijst.")
            reflections_list = []
        else:
            logger.warning("⚠️ Reflectie niet als JSON-lijst ontvangen, fallback naar lege lijst.")
            reflections_list = []

        logger.info(f"🧠 AI market interpretatie: {ai_context}")
        logger.info(f"🪞 AI market reflecties: {len(reflections_list)} items")

        # =========================================================
        # 6️⃣ Opslaan categorie-samenvatting in ai_category_insights
        # =========================================================
        trend = ai_context.get("trend")
        momentum = ai_context.get("momentum")
        volatility = ai_context.get("volatility")
        summary = ai_context.get("summary")
        top_signals = ai_context.get("top_signals", [])

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights (category, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('market', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, date) DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                market_avg,
                trend,
                momentum,         # bias ~ momentum
                volatility,       # risk ~ volatiliteit
                summary,
                json.dumps(top_signals, ensure_ascii=False),
            ))

        # =========================================================
        # 7️⃣ Opslaan individuele reflecties in ai_reflections
        # =========================================================
        for r in reflections_list:
            indicator = r.get("indicator")
            ai_score = r.get("ai_score")
            compliance = r.get("compliance")
            comment = r.get("comment")
            recommendation = r.get("recommendation")

            if not indicator:
                continue

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

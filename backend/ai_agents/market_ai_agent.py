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
# 🪙 MARKET AI AGENT — USER-AWARE VARIANT
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight(user_id: int | None = None):
    """
    Analyseert marktdata (prijs, volume, change_24h, 7d OHLC)
    in combinatie met de scoreregels in `market_indicator_rules`
    en de samengestelde market-score via generate_scores_db("market").

    Output:
    - ai_category_insights per gebruiker
    - ai_reflections per gebruiker
    """

    logger.info(f"🪙 Start Market AI Agent... user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        # =========================================================
        # 1️⃣ Scoreregels laden (globaal)
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
            indicator, rmin, rmax, score, trend, interp, action = r
            rules_by_indicator.setdefault(indicator, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        logger.info(f"📘 Scoreregels geladen ({len(rules_by_indicator)} indicatoren)")

        # =========================================================
        # 2️⃣ MARKET DATA — USER SPECIFIEK
        # =========================================================
        with conn.cursor() as cur:
            if user_id is not None:
                cur.execute("""
                    SELECT price, change_24h, volume, timestamp
                    FROM market_data
                    WHERE symbol='BTC' AND user_id=%s
                    ORDER BY timestamp DESC
                    LIMIT 1;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT price, change_24h, volume, timestamp
                    FROM market_data
                    WHERE symbol='BTC'
                    ORDER BY timestamp DESC
                    LIMIT 1;
                """)

            last_snapshot = cur.fetchone()

            if user_id is not None:
                cur.execute("""
                    SELECT date, open, high, low, close, change, volume
                    FROM market_data_7d
                    WHERE symbol='BTC' AND user_id=%s
                    ORDER BY date DESC
                    LIMIT 7;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT date, open, high, low, close, change, volume
                    FROM market_data_7d
                    WHERE symbol='BTC'
                    ORDER BY date DESC
                    LIMIT 7;
                """)

            ohlc_rows = cur.fetchall()

        if not last_snapshot:
            logger.warning(f"⚠️ Geen market_data snapshot gevonden (user_id={user_id})")
            return

        if not ohlc_rows:
            logger.warning(f"⚠️ Geen market_data_7d gevonden (user_id={user_id})")
            return

        price, change_24h, volume, ts = last_snapshot
        price_info = {
            "price": float(price),
            "change_24h": float(change_24h),
            "volume": float(volume),
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts)
        }

        ohlc_rows_sorted = list(ohlc_rows)
        ohlc_rows_sorted.reverse()

        ohlc_summary = [
            {
                "date": row[0].isoformat(),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "change": float(row[5]),
                "volume": float(row[6]),
            }
            for row in ohlc_rows_sorted
        ]

        ohlc_text = "\n".join([
            f"{o['date']}: O={o['open']} H={o['high']} L={o['low']} C={o['close']} Δ={o['change']}%"
            for o in ohlc_summary
        ])

        # =========================================================
        # 3️⃣ Market-score via scoringsengine
        # =========================================================
        market_scores = generate_scores_db("market")
        market_avg = market_scores.get("total_score", 0)
        score_items = market_scores.get("scores", {})

        top_contrib = sorted(
            score_items.items(),
            key=lambda x: x[1]["score"],
            reverse=True
        )[:3]

        top_contrib_pretty = [
            {
                "indicator": k,
                "value": v["value"],
                "score": v["score"],
                "trend": v["trend"],
                "interpretation": v["interpretation"]
            }
            for k, v in top_contrib
        ]

        # =========================================================
        # 4️⃣ Prompt voor AI interpretatie
        # =========================================================
        data_payload = {
            "user_id": user_id,
            "price_snapshot": price_info,
            "ohlc_7d": ohlc_summary,
            "market_rules": rules_by_indicator,
            "market_avg_score": market_avg,
            "market_top_contributors": top_contrib_pretty,
            "ohlc_text": ohlc_text,
        }

        prompt_context = f"""
Je bent een Bitcoin marktanalist.

Analyseer onderstaande data en geef antwoord in geldige JSON:

DATA:
{json.dumps(data_payload, ensure_ascii=False, indent=2)}

STRUCTUUR:
{{
  "trend": "",
  "momentum": "",
  "volatility": "",
  "liquidity": "",
  "summary": "",
  "top_signals": []
}}
"""

        ai_context = ask_gpt(
            prompt_context,
            system_role="Je bent een professionele crypto-marktanalist. Geef geldige JSON."
        )

        if not isinstance(ai_context, dict):
            ai_context = {
                "trend": "",
                "momentum": "",
                "volatility": "",
                "liquidity": "",
                "summary": "",
                "top_signals": []
            }

        # =========================================================
        # 5️⃣ Reflecties prompt
        # =========================================================
        prompt_reflections = f"""
DATA:
{json.dumps(data_payload, ensure_ascii=False)}

Maak JSON-lijst met reflecties:
[
  {{
    "indicator": "",
    "ai_score": 0,
    "compliance": 0,
    "comment": "",
    "recommendation": ""
  }}
]
"""

        reflections = ask_gpt(
            prompt_reflections,
            system_role="Je bent een professionele marktanalist. Antwoord in JSON-lijst."
        )

        if not isinstance(reflections, list):
            reflections = []

        # =========================================================
        # 6️⃣ Opslaan insights (user-specific)
        # =========================================================
        with conn.cursor() as cur:
            if user_id is not None:
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                    VALUES ('market', %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, date)
                    DO UPDATE SET
                        avg_score=EXCLUDED.avg_score,
                        trend=EXCLUDED.trend,
                        bias=EXCLUDED.bias,
                        risk=EXCLUDED.risk,
                        summary=EXCLUDED.summary,
                        top_signals=EXCLUDED.top_signals,
                        created_at=NOW();
                """, (
                    user_id,
                    market_avg,
                    ai_context["trend"],
                    ai_context["momentum"],
                    ai_context["volatility"],
                    ai_context["summary"],
                    json.dumps(ai_context["top_signals"])
                ))
            else:
                # Backwards compatible
                cur.execute("""
                    INSERT INTO ai_category_insights
                        (category, avg_score, trend, bias, risk, summary, top_signals)
                    VALUES ('market', %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (category, date)
                    DO UPDATE SET
                        avg_score=EXCLUDED.avg_score,
                        trend=EXCLUDED.trend,
                        bias=EXCLUDED.bias,
                        risk=EXCLUDED.risk,
                        summary=EXCLUDED.summary,
                        top_signals=EXCLUDED.top_signals,
                        created_at=NOW();
                """, (
                    market_avg,
                    ai_context["trend"],
                    ai_context["momentum"],
                    ai_context["volatility"],
                    ai_context["summary"],
                    json.dumps(ai_context["top_signals"])
                ))

        # =========================================================
        # 7️⃣ Opslaan reflecties (user-specific)
        # =========================================================
        for r in reflections:
            if not r.get("indicator"):
                continue

            with conn.cursor() as cur:
                if user_id is not None:
                    cur.execute("""
                        INSERT INTO ai_reflections
                            (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                        VALUES ('market', %s, %s, NULL, %s, %s, %s, %s)
                        ON CONFLICT (category, user_id, indicator, date)
                        DO UPDATE SET
                            ai_score=EXCLUDED.ai_score,
                            compliance=EXCLUDED.compliance,
                            comment=EXCLUDED.comment,
                            recommendation=EXCLUDED.recommendation,
                            timestamp=NOW();
                    """, (
                        user_id,
                        r["indicator"],
                        r.get("ai_score"),
                        r.get("compliance"),
                        r.get("comment"),
                        r.get("recommendation")
                    ))
                else:
                    # back compat
                    cur.execute("""
                        INSERT INTO ai_reflections
                            (category, indicator, raw_score, ai_score, compliance, comment, recommendation)
                        VALUES ('market', %s, NULL, %s, %s, %s, %s)
                        ON CONFLICT (category, indicator, date)
                        DO UPDATE SET
                            ai_score=EXCLUDED.ai_score,
                            compliance=EXCLUDED.compliance,
                            comment=EXCLUDED.comment,
                            recommendation=EXCLUDED.recommendation,
                            timestamp=NOW();
                    """, (
                        r["indicator"],
                        r.get("ai_score"),
                        r.get("compliance"),
                        r.get("comment"),
                        r.get("recommendation")
                    ))

        conn.commit()
        logger.info(f"✅ Market AI insights + reflecties opgeslagen (user_id={user_id})")

    except Exception:
        logger.error("❌ Market AI Agent FOUT:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

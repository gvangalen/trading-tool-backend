import logging
import traceback
import json
from datetime import datetime, date
from typing import Dict, Any, List

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYMBOL = "BTC"


# ======================================================
# 🪙 MARKET AI AGENT — STRICT PER USER
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight(user_id: int):
    """
    Analyseert MARKT voor exact één gebruiker.

    Bronnen:
    1. market_indicator_scores (user-specifiek)
    2. market_data_7d (user-specifiek)
    3. market_forward_returns (user-specifiek)
    4. market_indicator_rules (globaal)
    5. market score via generate_scores_db("market", user_id)

    Output:
    - ai_category_insights (market)
    - ai_reflections (market)
    """

    logger.info(f"🪙 Market AI Agent gestart — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    today = date.today()

    try:
        # ======================================================
        # 1️⃣ MARKET INDICATOR RULES (GLOBAAL)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                ORDER BY indicator, range_min;
            """)
            rule_rows = cur.fetchall()

        rules: Dict[str, List[Dict[str, Any]]] = {}
        for i, rmin, rmax, score, trend, interp, action in rule_rows:
            rules.setdefault(i, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        # ======================================================
        # 2️⃣ MARKET INDICATOR SCORES (USER)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (indicator)
                    indicator, value, score, trend, interpretation, action, timestamp
                FROM market_indicator_scores
                WHERE user_id = %s
                ORDER BY indicator, timestamp DESC;
            """, (user_id,))
            indicator_rows = cur.fetchall()

        market_indicators = [{
            "indicator": i,
            "value": float(v) if v is not None else None,
            "score": int(s) if s is not None else None,
            "trend": t,
            "interpretation": interp,
            "action": a,
            "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
        } for i, v, s, t, interp, a, ts in indicator_rows]

        # ======================================================
        # 3️⃣ 7-DAAGSE PRIJS & VOLUME TABEL
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, open, high, low, close, change, volume
                FROM market_data_7d
                WHERE user_id = %s AND symbol = %s
                ORDER BY date DESC
                LIMIT 7;
            """, (user_id, SYMBOL))
            rows_7d = cur.fetchall()

        price_7d = [{
            "date": d.isoformat(),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "change_pct": float(chg),
            "volume": float(v),
        } for d, o, h, l, c, chg, v in reversed(rows_7d)]

        # ======================================================
        # 4️⃣ FORWARD RETURNS
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM market_forward_returns
                WHERE user_id = %s AND symbol = %s
                ORDER BY created_at DESC;
            """, (user_id, SYMBOL))

            cols = [d.name for d in cur.description]
            fr_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        # ======================================================
        # 5️⃣ MARKET SCORE (SCORING ENGINE)
        # ======================================================
        market_scores = generate_scores_db("market", user_id=user_id)
        market_avg = float(market_scores.get("total_score", 0))
        score_items = market_scores.get("scores", {})

        top_contributors = sorted(
            score_items.items(),
            key=lambda x: float(x[1].get("score", 0)),
            reverse=True
        )[:5]

        top_contributors_pretty = [{
            "indicator": k,
            "value": v.get("value"),
            "score": v.get("score"),
            "trend": v.get("trend"),
            "interpretation": v.get("interpretation"),
        } for k, v in top_contributors]

        # ======================================================
        # 6️⃣ AI CONTEXT
        # ======================================================
        payload = {
            "user_id": user_id,
            "symbol": SYMBOL,
            "market_avg_score": market_avg,
            "market_top_contributors": top_contributors_pretty,
            "market_indicator_scores": market_indicators,
            "price_table_7d": price_7d,
            "forward_returns": fr_rows,
            "market_rules": rules,
        }

        # ======================================================
        # 7️⃣ AI – MARKET INSIGHT
        # ======================================================
        prompt = f"""
Je bent een professionele Bitcoin marktanalist.

Analyseer de volgende data:
- Gebruiker-specifieke market indicatoren
- 7-daagse prijs & volume tabel
- Forward returns statistieken
- Samengestelde market score

Geef een samenvatting en advies in **GELDIGE JSON**:

{{
  "trend": "",
  "bias": "",
  "risk": "",
  "momentum": "",
  "volatility": "",
  "liquidity": "",
  "summary": "",
  "top_signals": [{{"signal":"","why":"","impact":""}}]
}}

DATA:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""

        ai = ask_gpt(
            prompt,
            system_role="Je bent een professionele marktanalist. Antwoord uitsluitend in JSON."
        )

        if not isinstance(ai, dict):
            ai = {}

        insight = {
            "avg_score": market_avg,
            "trend": ai.get("trend", ""),
            "bias": ai.get("bias", ""),
            "risk": ai.get("risk", ""),
            "summary": ai.get("summary", ""),
            "top_signals": ai.get("top_signals", []),
        }

        # ======================================================
        # 8️⃣ OPSLAAN — CATEGORY INSIGHT
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM ai_category_insights
                WHERE category='market' AND user_id=%s AND date=%s;
            """, (user_id, today))

            cur.execute("""
                INSERT INTO ai_category_insights
                (category, user_id, avg_score, trend, bias, risk, summary, top_signals, date, created_at)
                VALUES ('market', %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW());
            """, (
                user_id,
                insight["avg_score"],
                insight["trend"],
                insight["bias"],
                insight["risk"],
                insight["summary"],
                json.dumps(insight["top_signals"]),
                today,
            ))

        # ======================================================
        # 9️⃣ AI REFLECTIONS
        # ======================================================
        prompt_reflect = f"""
Maak reflecties per indicator (max 12).

JSON schema:
[
  {{
    "indicator": "",
    "ai_score": 0,
    "compliance": 0,
    "comment": "",
    "recommendation": ""
  }}
]

DATA:
{json.dumps(payload, ensure_ascii=False)}
"""

        reflections = ask_gpt(
            prompt_reflect,
            system_role="Je bent een professionele marktanalist. Antwoord in JSON-lijst."
        )

        if not isinstance(reflections, list):
            reflections = []

        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM ai_reflections
                WHERE category='market' AND user_id=%s AND timestamp::date=%s;
            """, (user_id, today))

            for r in reflections:
                if not r.get("indicator"):
                    continue
                cur.execute("""
                    INSERT INTO ai_reflections
                    (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation, timestamp)
                    VALUES ('market', %s, %s, NULL, %s, %s, %s, %s, NOW());
                """, (
                    user_id,
                    r["indicator"],
                    r.get("ai_score", 0),
                    r.get("compliance", 0),
                    r.get("comment", ""),
                    r.get("recommendation", ""),
                ))

        conn.commit()
        logger.info(f"✅ Market AI Agent afgerond — user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Market AI Agent fout:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

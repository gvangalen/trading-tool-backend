import logging
import traceback
import json
from datetime import date

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import generate_scores_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYMBOL = "BTC"


# ======================================================
# 🪙 MARKET AI AGENT — HYBRIDE (USER + GLOBAAL)
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight(user_id: int):
    """
    Analyseert:
    - User-specifieke market indicator scores
    - Globale marktdata (7d prijs & forward returns)
    - Combineert dit tot één market-advies per user
    """

    logger.info(f"🪙 Market AI Agent gestart — user_id={user_id}")
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    today = date.today()

    try:
        # ======================================================
        # 1️⃣ GLOBALE MARKET RULES
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                ORDER BY indicator, range_min;
            """)
            rule_rows = cur.fetchall()

        rules = {}
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
        # 2️⃣ USER MARKET INDICATOR SCORES
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
            "timestamp": ts.isoformat(),
        } for i, v, s, t, interp, a, ts in indicator_rows]

        # ======================================================
        # 3️⃣ GLOBALE 7-DAAGSE PRIJS & VOLUME
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, open, high, low, close, change, volume
                FROM market_data_7d
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 7;
            """, (SYMBOL,))
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
        # 4️⃣ GLOBALE FORWARD RETURNS
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT period, start_date, end_date, change, avg_daily
                FROM market_forward_returns
                WHERE symbol = %s
                ORDER BY created_at DESC;
            """, (SYMBOL,))
            fr_rows = cur.fetchall()

        forward_returns = [{
            "period": p,
            "start": sd.isoformat(),
            "end": ed.isoformat() if ed else None,
            "change": float(ch),
            "avg_daily": float(ad) if ad else None,
        } for p, sd, ed, ch, ad in fr_rows]

        # ======================================================
        # 5️⃣ USER MARKET SCORE (ENGINE)
        # ======================================================
        scores = generate_scores_db("market", user_id=user_id)
        market_avg = float(scores.get("total_score", 0))
        score_items = scores.get("scores", {})

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
        # 6️⃣ AI PAYLOAD
        # ======================================================
        payload = {
            "symbol": SYMBOL,
            "market_avg_score": market_avg,
            "market_top_contributors": top_contributors_pretty,
            "user_market_indicators": market_indicators,
            "price_7d": price_7d,
            "forward_returns": forward_returns,
            "market_rules": rules,
        }

        prompt = f"""
Je bent een professionele Bitcoin marktanalist.

Analyseer:
- De 7-daagse prijs & volume tabel
- Forward returns statistieken
- User-specifieke market indicator scores
- De samengestelde market score

Geef antwoord in **GELDIGE JSON**:

{{
  "trend": "",
  "bias": "",
  "risk": "",
  "momentum": "",
  "volatility": "",
  "summary": "",
  "top_signals": []
}}

DATA:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""

        ai = ask_gpt(prompt, system_role="Antwoord uitsluitend in JSON.")
        if not isinstance(ai, dict):
            ai = {}

        # ======================================================
        # 7️⃣ OPSLAAN INSIGHT
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
                market_avg,
                ai.get("trend", ""),
                ai.get("bias", ""),
                ai.get("risk", ""),
                ai.get("summary", ""),
                json.dumps(ai.get("top_signals", [])),
                today,
            ))

        conn.commit()
        logger.info(f"✅ Market AI Agent afgerond — user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ Market AI Agent fout:")
        logger.error(traceback.format_exc())

    finally:
        conn.close()

import logging
import json
import traceback
from datetime import date

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYMBOL = "BTC"


# ======================================================
# Helpers
# ======================================================
def _to_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _to_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


# ======================================================
# 🪙 MARKET AI AGENT — DB-GEDREVEN (SINGLE SOURCE OF TRUTH)
# ======================================================
def run_market_agent(user_id: int, symbol: str = SYMBOL):
    """
    Genereert market AI insights.

    - Gebruikt ALLEEN market_data_indicators
    - Doet GEEN eigen berekeningen
    - Output → ai_category_insights + daily_scores
    """

    if user_id is None:
        raise ValueError("❌ Market AI Agent vereist een user_id")

    logger.info(f"🪙 [Market-Agent] Start voor user_id={user_id}, symbol={symbol}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # ======================================================
        # 1️⃣ LAATSTE MARKET INDICATOR SCORES (USER-SPECIFIEK)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (name)
                    name,
                    value,
                    score,
                    trend,
                    interpretation,
                    action,
                    timestamp
                FROM market_data_indicators
                WHERE user_id = %s
                ORDER BY name, timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        market_indicators = [
            {
                "indicator": name,
                "value": _to_float(value),
                "score": _to_int(score),
                "trend": trend,
                "interpretation": interpretation,
                "action": action,
                "timestamp": ts.isoformat() if ts else None,
            }
            for name, value, score, trend, interpretation, action, ts in rows
        ]

        if not market_indicators:
            logger.warning("⚠️ Geen market indicator scores gevonden")
            return

        # ======================================================
        # 2️⃣ MARKET SCORE (AVG — GEEN 0 ALS FALLBACK)
        # ======================================================
        valid_scores = [i["score"] for i in market_indicators if i["score"] is not None]
        market_avg = round(sum(valid_scores) / len(valid_scores)) if valid_scores else 10

        top_contributors = sorted(
            [i for i in market_indicators if i["score"] is not None],
            key=lambda x: x["score"],
            reverse=True
        )[:5]

        # ======================================================
        # 3️⃣ 7-DAAGSE PRIJS / VOLUME CONTEXT
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, open, high, low, close, change, volume
                FROM market_data_7d
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 7;
            """, (symbol,))
            rows_7d = cur.fetchall()

        price_7d = [
            {
                "date": d.isoformat() if d else None,
                "open": _to_float(o),
                "high": _to_float(h),
                "low": _to_float(l),
                "close": _to_float(c),
                "change_pct": _to_float(ch),
                "volume": _to_float(v),
            }
            for d, o, h, l, c, ch, v in reversed(rows_7d)
        ]

        # ======================================================
        # 4️⃣ AI PAYLOAD
        # ======================================================
        payload = {
            "symbol": symbol,
            "market_avg_score": market_avg,
            "top_contributors": top_contributors,
            "market_indicators": market_indicators,
            "price_7d": price_7d,
        }

        market_task = """
Analyseer marktdata voor Bitcoin in beslistermen.

Gebruik uitsluitend:
- gescoorde market-indicatoren
- recente prijs- en volumecontext

Geef:
- trend
- bias
- risico
- momentum
- volatiliteit
- samenvatting
- belangrijkste signalen

Antwoord uitsluitend in geldige JSON.
"""

        system_prompt = build_system_prompt(
            agent="market",
            task=market_task
        )

        ai = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(ai, dict):
            ai = {}

        top_signals = ai.get("top_signals", [])
        if not isinstance(top_signals, list):
            top_signals = []

        # ======================================================
        # 5️⃣ OPSLAAN AI INSIGHT (ai_category_insights)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES
                    ('market', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score   = EXCLUDED.avg_score,
                    trend       = EXCLUDED.trend,
                    bias        = EXCLUDED.bias,
                    risk        = EXCLUDED.risk,
                    summary     = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at  = NOW();
            """, (
                user_id,
                market_avg,
                ai.get("trend", ""),
                ai.get("bias", ""),
                ai.get("risk", ""),
                ai.get("summary", ""),
                json.dumps(top_signals),
            ))

        # ======================================================
        # 6️⃣ DAILY_SCORES BIJWERKEN (DASHBOARD METERS)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_scores
                SET
                    market_score = %s,
                    market_interpretation = %s
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
            """, (
                market_avg,
                ai.get("summary", ""),
                user_id
            ))

        conn.commit()
        logger.info(f"✅ [Market-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Market-Agent] Fout", exc_info=True)
        logger.error(traceback.format_exc())
    finally:
        conn.close()


# ======================================================
# ✅ Celery wrapper
# ======================================================
@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight(user_id: int):
    try:
        run_market_agent(user_id=user_id, symbol=SYMBOL)
    except Exception:
        logger.error("❌ Market AI task crash", exc_info=True)

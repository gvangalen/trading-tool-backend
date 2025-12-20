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
# 🪙 MARKET AI AGENT — PURE AI + DB LOGICA (HYBRIDE)
# ======================================================

def run_market_agent(user_id: int, symbol: str = SYMBOL):
    """
    Genereert market AI insights voor één gebruiker.

    Data:
    - Market score via engine (globaal: market_data + market_data_7d + rules)
    - Globale 7d price/volume
    - Globale forward returns (als aanwezig)
    - Optioneel: user-specifieke market_indicator_scores (als jullie die tabel gebruiken)

    Schrijft:
    - ai_category_insights (category='market') UNIQUE (user_id, category, date)
    """

    if user_id is None:
        raise ValueError("❌ Market AI Agent vereist een user_id")

    logger.info(f"🪙 [Market-Agent] Start voor user_id={user_id}, symbol={symbol}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    today = date.today()

    try:
        # ======================================================
        # 1️⃣ MARKET RULES (GLOBAAL)
        # ======================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indicator, range_min, range_max, score, trend, interpretation, action
                FROM market_indicator_rules
                ORDER BY indicator, range_min;
            """)
            rule_rows = cur.fetchall()

        market_rules = {}
        for ind, rmin, rmax, score, trend, interp, action in rule_rows:
            market_rules.setdefault(ind, []).append({
                "range_min": float(rmin),
                "range_max": float(rmax),
                "score": int(score),
                "trend": trend,
                "interpretation": interp,
                "action": action,
            })

        # ======================================================
        # 2️⃣ OPTIONEEL: USER MARKET INDICATOR SCORES (ALS BESTAAT)
        # ======================================================
        user_market_indicators = []
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (indicator)
                        indicator, value, score, trend, interpretation, action, timestamp
                    FROM market_indicator_scores
                    WHERE user_id = %s
                    ORDER BY indicator, timestamp DESC;
                """, (user_id,))
                rows = cur.fetchall()

            user_market_indicators = [{
                "indicator": i,
                "value": float(v) if v is not None else None,
                "score": int(s) if s is not None else None,
                "trend": t,
                "interpretation": interp,
                "action": a,
                "timestamp": ts.isoformat() if ts else None,
            } for i, v, s, t, interp, a, ts in rows]

        except Exception:
            # tabel bestaat misschien niet of wordt niet gebruikt → niet crashen
            logger.info("ℹ️ market_indicator_scores niet beschikbaar of leeg — skip (optioneel).")

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
            """, (symbol,))
            rows_7d = cur.fetchall()

        price_7d = [{
            "date": d.isoformat(),
            "open": float(o) if o is not None else None,
            "high": float(h) if h is not None else None,
            "low": float(l) if l is not None else None,
            "close": float(c) if c is not None else None,
            "change_pct": float(chg) if chg is not None else None,
            "volume": float(v) if v is not None else None,
        } for d, o, h, l, c, chg, v in reversed(rows_7d)]

        # ======================================================
        # 4️⃣ GLOBALE FORWARD RETURNS (ALS BESTAAT)
        # ======================================================
        forward_returns = []
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT period, start_date, end_date, change, avg_daily
                    FROM market_forward_returns
                    WHERE symbol = %s
                    ORDER BY created_at DESC
                    LIMIT 20;
                """, (symbol,))
                fr_rows = cur.fetchall()

            forward_returns = [{
                "period": p,
                "start": sd.isoformat() if sd else None,
                "end": ed.isoformat() if ed else None,
                "change": float(ch) if ch is not None else None,
                "avg_daily": float(ad) if ad is not None else None,
            } for p, sd, ed, ch, ad in fr_rows]

        except Exception:
            logger.info("ℹ️ market_forward_returns niet beschikbaar of leeg — skip (optioneel).")

        # ======================================================
        # 5️⃣ MARKET SCORE (ENGINE) — GLOBAAL, GEEN user_id!
        # ======================================================
        scores = generate_scores_db("market")
        market_avg = float(scores.get("total_score", 10))
        score_items = scores.get("scores", {}) or {}

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
        # 6️⃣ AI PAYLOAD + PROMPT
        # ======================================================
        payload = {
            "symbol": symbol,
            "market_avg_score": market_avg,
            "market_top_contributors": top_contributors_pretty,
            "engine_scores": score_items,  # volledige score map (kan handig zijn)
            "user_market_indicators": user_market_indicators,
            "price_7d": price_7d,
            "forward_returns": forward_returns,
            "market_rules": market_rules,
        }

        prompt = f"""
Je bent een professionele Bitcoin marktanalist.

Analyseer:
- 7-daagse prijs & volume
- forward returns (als aanwezig)
- samengestelde market score + top contributors
- optioneel user-indicator scores (als aanwezig)

Geef antwoord in GELDIGE JSON:

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

        ai = ask_gpt(prompt, system_role="Antwoord uitsluitend in geldige JSON.")
        if not isinstance(ai, dict):
            ai = {}

        trend = ai.get("trend", "")
        bias = ai.get("bias", "")
        risk = ai.get("risk", "")
        summary = ai.get("summary", "")
        top_signals = ai.get("top_signals", [])
        if not isinstance(top_signals, list):
            top_signals = []

        # ======================================================
        # 7️⃣ OPSLAAN ai_category_insights (UPSERT)
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
                trend,
                bias,
                risk,
                summary,
                json.dumps(top_signals),
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
# ✅ Celery Task wrapper (zoals jullie stijl)
# ======================================================

@shared_task(name="backend.ai_agents.market_ai_agent.generate_market_insight")
def generate_market_insight(user_id: int):
    """
    Wrapper task zodat dispatcher.dispatch_for_all_users dit kan aanroepen.
    """
    try:
        run_market_agent(user_id=user_id, symbol=SYMBOL)
    except Exception:
        logger.error("❌ Market AI task crash", exc_info=True)

import logging
import traceback
import json
from datetime import date

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.ai_core.system_prompt_builder import build_system_prompt
from backend.ai_core.agent_context import build_agent_context  # ✅ NIEUW

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


def _is_empty_market_context(ctx: dict) -> bool:
    if not isinstance(ctx, dict):
        return True

    return not any([
        ctx.get("summary"),
        ctx.get("trend"),
        ctx.get("bias"),
        ctx.get("risk"),
        ctx.get("top_signals"),
    ])


def _fallback_market_context(items: list) -> dict:
    indicators = {i.get("indicator") for i in items if i.get("indicator")}

    return {
        "trend": "neutraal",
        "bias": "afwachtend",
        "risk": "gemiddeld",
        "momentum": "",
        "volatility": "",
        "summary": (
            "De market-context is gebaseerd op een beperkt aantal market-indicatoren. "
            "Gebruik de score als richting, maar wacht op bevestiging voordat je agressief positioneert."
        ),
        "top_signals": [
            f"{ind} blijft richtinggevend"
            for ind in sorted(indicators)
        ] or ["Beperkte marketdata beschikbaar"],
    }


def normalize_ai_context(ai_ctx: dict, market_items: list) -> dict:
    """
    Normaliseert AI-output (incl. unwrap van analysis/analyse) + fallback.
    """
    if not isinstance(ai_ctx, dict):
        return _fallback_market_context(market_items)

    # ✅ unwrap-fix: accepteer EN + NL wrappers
    for key in ("analysis", "analyse"):
        if key in ai_ctx and isinstance(ai_ctx[key], dict):
            ai_ctx = ai_ctx[key]
            break

    normalized = {
        "trend": ai_ctx.get("trend", ""),
        "bias": ai_ctx.get("bias", ""),
        "risk": ai_ctx.get("risk") or ai_ctx.get("risico", ""),
        "momentum": ai_ctx.get("momentum", ""),
        "volatility": ai_ctx.get("volatility") or ai_ctx.get("volatiliteit", ""),
        "summary": ai_ctx.get("summary") or ai_ctx.get("samenvatting", ""),
        "top_signals": ai_ctx.get("top_signals", []),
    }

    if not isinstance(normalized["top_signals"], list):
        normalized["top_signals"] = []

    if _is_empty_market_context(normalized):
        logger.warning("⚠️ Market AI gaf lege inhoud → fallback toegepast")
        return _fallback_market_context(market_items)

    # keep keys consistent for DB writes
    return normalized


# ======================================================
# 🪙 MARKET AI AGENT — DB-GEDREVEN (ENIGE WAARHEID)
# ======================================================
def run_market_agent(user_id: int, symbol: str = SYMBOL):
    """
    Genereert market AI insights.

    - Gebruikt ALLEEN market_data_indicators (reeds berekend & gescoord)
    - Doet GEEN eigen berekeningen (behalve het gemiddelde zoals eerder)
    - + Gedeelde context (gisteren) via build_agent_context
    - + AI reflections per indicator
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
        # 1️⃣ LAATSTE MARKET INDICATOR SCORES (USER-SPECIFIC)
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

        market_indicators = [{
            "indicator": name,
            "value": _to_float(value),
            "score": _to_int(score),
            "trend": trend,
            "interpretation": interpretation,
            "action": action,
            "timestamp": ts.isoformat() if ts else None,
        } for name, value, score, trend, interpretation, action, ts in rows]

        if not market_indicators:
            logger.warning("⚠️ Geen market indicator scores gevonden")
            return

        # ======================================================
        # 2️⃣ MARKET SCORE (IDENTIEK AAN OUDE LOGICA)
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

        price_7d = [{
            "date": d.isoformat() if d else None,
            "open": _to_float(o),
            "high": _to_float(h),
            "low": _to_float(l),
            "close": _to_float(c),
            "change_pct": _to_float(ch),
            "volume": _to_float(v),
        } for d, o, h, l, c, ch, v in reversed(rows_7d)]

        # ======================================================
        # 4️⃣ 🧠 SHARED AGENT CONTEXT (GISTEREN + DELTA)
        # ======================================================
        agent_context = build_agent_context(
            user_id=user_id,
            category="market",
            current_score=market_avg,
            current_items=top_contributors,
            lookback_days=1,  # bewust 1 dag
        )

        # ======================================================
        # 5️⃣ AI ANALYSE (MET CONTEXT)
        # ======================================================
        payload = {
            "context": agent_context,
            "symbol": symbol,
            "market_avg_score": market_avg,
            "top_contributors": top_contributors,
            "market_indicators": market_indicators,
            "price_7d": price_7d,
        }

        MARKET_TASK = """
Je bent een ervaren Bitcoin market analyst.

Je krijgt:
- gescoorde market-indicatoren (leidend)
- 7-daagse prijs/volume context
- context van gisteren (score + samenvatting + top signals + delta)

Belangrijk:
- Gebruik expliciet verschillen t.o.v. gisteren (score/bias/trend)
- Leg uit WAAROM de belangrijkste signalen vandaag sterker/zwakker zijn
- Geen uitleg van basisbegrippen, geen marketingtaal
- Geen eigen berekeningen (behalve interpretatie op basis van score/indicatoren)

OUTPUT — ALLEEN GELDIGE JSON:

{
  "trend": "",
  "bias": "",
  "risk": "",
  "momentum": "",
  "volatility": "",
  "summary": "",
  "top_signals": []
}

REGELS:
- trend/bias/risk/momentum/volatility: kort en beslisgericht
- summary: max 3 zinnen, met verandering t.o.v. gisteren
- top_signals: max 5 bullets
- Bij ontbrekende data: gebruik exact "ONVOLDOENDE DATA"
"""

        system_prompt = build_system_prompt(agent="market", task=MARKET_TASK)

        raw_ai_context = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        ai_context = normalize_ai_context(raw_ai_context, market_indicators)

        # ======================================================
        # 6️⃣ AI REFLECTIES (UITGEBREID)
        # ======================================================
        reflections_task = """
Maak per market-indicator een reflectie.

Per item:
- indicator (naam)
- ai_score (0–100)
- compliance (0–100)
- korte comment (1–2 zinnen, beslisgericht)
- concrete aanbeveling (1 zin)

Antwoord uitsluitend als JSON-lijst.
"""

        reflections_prompt = build_system_prompt(agent="market", task=reflections_task)

        ai_reflections = ask_gpt(
            prompt=json.dumps({
                "context": agent_context,
                "market_indicators": market_indicators,
                "top_contributors": top_contributors,
                "market_avg_score": market_avg,
            }, ensure_ascii=False, indent=2),
            system_role=reflections_prompt
        )

        if not isinstance(ai_reflections, list):
            ai_reflections = []

        # ======================================================
        # 7️⃣ OPSLAAN AI INSIGHT (ai_category_insights)
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
                ai_context.get("trend", ""),
                ai_context.get("bias", ""),
                ai_context.get("risk", ""),
                ai_context.get("summary", ""),
                json.dumps(ai_context.get("top_signals", [])),
            ))

        # ======================================================
        # 8️⃣ OPSLAAN ai_reflections (market)
        # ======================================================
        for r in ai_reflections:
            indicator = r.get("indicator") or r.get("name")
            if not indicator:
                continue

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_reflections
                        (category, user_id, indicator, raw_score, ai_score, compliance, comment, recommendation)
                    VALUES ('market', %s, %s, NULL, %s, %s, %s, %s)
                    ON CONFLICT (category, user_id, indicator, date)
                    DO UPDATE SET
                        ai_score = EXCLUDED.ai_score,
                        compliance = EXCLUDED.compliance,
                        comment = EXCLUDED.comment,
                        recommendation = EXCLUDED.recommendation,
                        timestamp = NOW();
                """, (
                    user_id,
                    str(indicator),
                    r.get("ai_score", 50),
                    r.get("compliance", 50),
                    r.get("comment", "") or r.get("opmerking", ""),
                    r.get("recommendation", "") or r.get("aanbeveling", ""),
                ))

        # ======================================================
        # 9️⃣ DAILY_SCORES BIJWERKEN (IDENTIEK AAN OUDE FILE)
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
                ai_context.get("summary", ""),
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

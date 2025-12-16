import logging
import traceback
import json
from datetime import date
from decimal import Decimal

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# 🧮 HELPER – safe numeric + range score intersect
# ===================================================================

def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def score_overlap(value, min_v, max_v):
    value = to_float(value)
    min_v = to_float(min_v)
    max_v = to_float(max_v)

    if value is None or min_v is None or max_v is None:
        return 0

    if value < min_v or value > max_v:
        return 0

    mid = (min_v + max_v) / 2
    dist = abs(value - mid)
    max_dist = (max_v - min_v) / 2

    if max_dist <= 0:
        return 100

    return round(100 - (dist / max_dist * 100))


# ===================================================================
# 🤖 MAIN SETUP AGENT — USER AWARE
# ===================================================================
def run_setup_agent(asset="BTC", user_id: int | None = None):
    logger.info(f"🤖 Setup-Agent gestart (user_id={user_id})...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return {"active_setup": None, "all_setups": []}

    try:
        # ---------------------------------------------------------------
        # 1️⃣ DAILY SCORES
        # ---------------------------------------------------------------
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT macro_score, technical_score, market_score
                    FROM daily_scores
                    WHERE report_date = CURRENT_DATE AND user_id = %s
                    LIMIT 1;
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT macro_score, technical_score, market_score
                    FROM daily_scores
                    WHERE report_date = CURRENT_DATE
                    LIMIT 1;
                """)
            row = cur.fetchone()

        if not row:
            logger.error("❌ Geen daily_scores gevonden.")
            return {"active_setup": None, "all_setups": []}

        macro_score, technical_score, market_score = map(to_float, row)

        # ---------------------------------------------------------------
        # 2️⃣ SETUPS OPHALEN
        # ---------------------------------------------------------------
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT 
                        id, name, symbol,
                        min_macro_score, max_macro_score,
                        min_technical_score, max_technical_score,
                        min_market_score, max_market_score,
                        explanation, action, strategy_type,
                        dynamic_investment, created_at
                    FROM setups
                    WHERE symbol = %s AND user_id = %s
                    ORDER BY created_at DESC;
                """, (asset, user_id))
            else:
                cur.execute("""
                    SELECT 
                        id, name, symbol,
                        min_macro_score, max_macro_score,
                        min_technical_score, max_technical_score,
                        min_market_score, max_market_score,
                        explanation, action, strategy_type,
                        dynamic_investment, created_at
                    FROM setups
                    WHERE symbol = %s
                    ORDER BY created_at DESC;
                """, (asset,))

            setups = cur.fetchall()

        if not setups:
            logger.warning("⚠️ Geen setups gevonden.")
            return {"active_setup": None, "all_setups": []}

        results = []
        best_setup = None
        best_match_score = -1

        # ---------------------------------------------------------------
        # 3️⃣ MATCHING + OPSLAAN DAILY_SETUP_SCORES
        # ---------------------------------------------------------------
        for (
            setup_id, name, symbol,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market,
            explanation, action, strategy_type,
            dynamic_investment, created_at
        ) in setups:

            macro_match = score_overlap(macro_score, min_macro, max_macro)
            tech_match = score_overlap(technical_score, min_tech, max_tech)
            market_match = score_overlap(market_score, min_market, max_market)

            total_match = round((macro_match + tech_match + market_match) / 3)
            active = macro_match > 0 and tech_match > 0 and market_match > 0

            if total_match > best_match_score:
                best_match_score = total_match
                best_setup = {
                    "setup_id": setup_id,
                    "name": name,
                    "symbol": symbol,
                    "total_match": total_match,
                    "active": active,
                }

            prompt = f"""
Je bent een crypto analist.

MARKT SCORES:
Macro {macro_score}
Technical {technical_score}
Market {market_score}

Setup '{name}' ranges:
Macro {min_macro}-{max_macro}
Technical {min_tech}-{max_tech}
Market {min_market}-{max_market}

Geef één zin waarom deze match {total_match}/100 scoort.
"""
            ai_comment = ask_gpt_text(prompt)

            results.append({
                "setup_id": setup_id,
                "name": name,
                "symbol": symbol,
                "match_score": total_match,
                "active": active,
                "ai_comment": ai_comment,
                "best_of_day": False,
            })

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores
                        (setup_id, user_id, report_date, score, is_active, explanation, breakdown)
                    VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (setup_id, user_id, report_date)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation,
                        breakdown = EXCLUDED.breakdown,
                        created_at = NOW();
                """, (
                    setup_id,
                    user_id,
                    total_match,
                    active,
                    ai_comment,
                    json.dumps({
                        "macro": macro_match,
                        "technical": tech_match,
                        "market": market_match,
                    })
                ))

        # ---------------------------------------------------------------
        # 4️⃣ BEST OF DAY
        # ---------------------------------------------------------------
        if best_setup:
            for r in results:
                if r["setup_id"] == best_setup["setup_id"]:
                    r["best_of_day"] = True

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s AND user_id = %s AND report_date = CURRENT_DATE;
                """, (best_setup["setup_id"], user_id))

        # ---------------------------------------------------------------
        # 5️⃣ AI CATEGORY INSIGHT (SETUP)
        # ---------------------------------------------------------------
        avg_score = round(sum(r["match_score"] for r in results) / len(results), 2)
        active_count = sum(1 for r in results if r["active"])

        trend = (
            "Sterke match" if best_match_score >= 70 else
            "Gemiddelde match" if best_match_score >= 40 else
            "Zwakke match"
        )

        bias = "Kansrijk" if active_count > 0 else "Afwachten"
        risk = "Laag" if market_score >= 60 else "Gemiddeld" if market_score >= 40 else "Hoog"

        summary = (
            f"Vandaag is '{best_setup['name']}' de best passende setup "
            f"met een match-score van {best_setup['total_match']}/100."
            if best_setup else
            "Geen duidelijke setup-match gevonden."
        )

        top_signals = sorted(results, key=lambda r: r["match_score"], reverse=True)[:3]

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                avg_score,
                trend,
                bias,
                risk,
                summary,
                json.dumps(top_signals),
            ))

        conn.commit()
        logger.info("✅ Setup-Agent voltooid.")
        return {"active_setup": best_setup, "all_setups": results}

    except Exception:
        conn.rollback()
        logger.error("❌ Setup-Agent crash", exc_info=True)
        return {"active_setup": None, "all_setups": []}

    finally:
        conn.close()


# ===================================================================
# 🧠 SETUP UITLEG GENERATOR
# ===================================================================
def generate_setup_explanation(setup_id: int, user_id: int | None = None):
    conn = get_db_connection()
    if not conn:
        return "Geen databaseverbinding."

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, symbol, trend, timeframe,
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score
                FROM setups
                WHERE id = %s AND user_id = %s;
            """, (setup_id, user_id))

            row = cur.fetchone()

        if not row:
            return "Setup niet gevonden."

        name, symbol, trend, timeframe, min_macro, max_macro, min_tech, max_tech, min_market, max_market = row

        prompt = f"""
Je bent een crypto analist.

Naam: {name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}

Macro {min_macro}-{max_macro}
Technical {min_tech}-{max_tech}
Market {min_market}-{max_market}

Geef maximaal 3 zinnen uitleg.
"""

        explanation = ask_gpt_text(prompt)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET explanation = %s
                WHERE id = %s AND user_id = %s;
            """, (explanation, setup_id, user_id))

        conn.commit()
        return explanation

    except Exception:
        logger.error("❌ Uitleg fout", exc_info=True)
        return "Fout bij uitleg."

    finally:
        conn.close()

import logging
import traceback
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ===================================================================
# 🧮 HELPER – range score intersect
# ===================================================================
def score_overlap(value, min_v, max_v):
    if value is None or min_v is None or max_v is None:
        return 0

    if value < min_v or value > max_v:
        return 0

    mid = (min_v + max_v) / 2
    dist = abs(value - mid)
    max_dist = (max_v - min_v) / 2

    if max_dist == 0:
        return 100

    return round(100 - (dist / max_dist * 100))


# ===================================================================
# 🤖 HOOFDFUNCTIE — ACTIVE SETUP FINDER
# ===================================================================
def run_setup_agent(asset="BTC"):
    logger.info("🤖 Setup-Agent gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return {"active_setup": None, "all_setups": []}

    try:
        with conn.cursor() as cur:
            # 1️⃣ SCORES VAN VANDAAG
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                LIMIT 1;
            """)
            row = cur.fetchone()

        if not row:
            logger.error("❌ Geen daily_scores gevonden voor vandaag.")
            return {"active_setup": None, "all_setups": []}

        macro_score, technical_score, market_score = row

        # 2️⃣ SETUPS VOOR DIT ASSET
        with conn.cursor() as cur:
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
        best_match_score = -99999

        # ===================================================================
        # 3️⃣ MATCH SCORE PER SETUP
        # ===================================================================
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

            active = (
                macro_match > 0 and 
                tech_match > 0 and 
                market_match > 0
            )

            # Best of Day bepalen
            if total_match > best_match_score:
                best_match_score = total_match
                best_setup = {
                    "setup_id": setup_id,
                    "name": name,
                    "symbol": symbol,
                    "macro_match": macro_match,
                    "tech_match": tech_match,
                    "market_match": market_match,
                    "total_match": total_match,
                    "active": active,
                    "strategy_type": strategy_type,
                }

            # AI comment
            prompt = f"""
Je bent een crypto analist.

MARKT:
- Macro {macro_score}
- Technical {technical_score}
- Market {market_score}

Setup '{name}' ranges:
- Macro {min_macro}-{max_macro}
- Technical {min_tech}-{max_tech}
- Market {min_market}-{max_market}

Geef één zin waarom deze match {total_match}/100 scoort.
"""
            ai_comment = ask_gpt_text(prompt)

            results.append({
                "setup_id": setup_id,
                "name": name,
                "symbol": symbol,
                "match_score": total_match,
                "active": active,
                "macro_match": macro_match,
                "technical_match": tech_match,
                "market_match": market_match,
                "ai_comment": ai_comment,
                "best_of_day": False
            })

            # 4️⃣ OPSLAAN IN daily_setup_scores (FIXED: report_date)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores 
                        (setup_id, report_date, score, is_active, explanation)
                    VALUES (%s, CURRENT_DATE, %s, %s, %s)
                    ON CONFLICT (setup_id, report_date)
                    DO UPDATE SET 
                        score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation,
                        created_at = NOW();
                """, (setup_id, total_match, active, ai_comment))

        # 5️⃣ BEST OF DAY MARKEREN
        if best_setup:
            for r in results:
                if r["setup_id"] == best_setup["setup_id"]:
                    r["best_of_day"] = True

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s AND report_date = CURRENT_DATE;
                """, (best_setup["setup_id"],))

        conn.commit()
        logger.info("✅ Setup-Agent voltooid.")
        return {"active_setup": best_setup, "all_setups": results}

    except Exception:
        logger.error("❌ Setup-Agent crash:", exc_info=True)
        return {"active_setup": None, "all_setups": []}

    finally:
        conn.close()


# ===================================================================
# 🧠 EXPLANATION GENERATOR
# ===================================================================
def generate_setup_explanation(setup_id: int):
    logger.info(f"🧠 Setup-uitleg genereren voor {setup_id}...")

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
                WHERE id = %s;
            """, (setup_id,))
            row = cur.fetchone()

        if not row:
            return "Setup niet gevonden."

        (
            name, symbol, trend, timeframe,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market
        ) = row

        prompt = f"""
Je bent een professionele crypto analist.

Maak een korte uitleg voor deze setup:

Naam: {name}
Asset: {symbol}
Timeframe: {timeframe}
Trend: {trend}

Ranges:
Macro {min_macro}-{max_macro}
Technical {min_tech}-{max_tech}
Market {min_market}-{max_market}

Geef maximaal 3 zinnen.
"""

        explanation = ask_gpt_text(prompt)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET explanation = %s
                WHERE id = %s;
            """, (explanation, setup_id))

        conn.commit()
        return explanation

    except Exception:
        logger.error("❌ Fout bij setup-uitleg:", exc_info=True)
        return "Fout bij uitleg genereren."

    finally:
        conn.close()

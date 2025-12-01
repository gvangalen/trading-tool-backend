import logging
import traceback
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ===================================================================
# 🧠 Helper — score overlap berekenen
# ===================================================================
def score_overlap(value, min_v, max_v):
    """
    Match-berekening voor score ranges.
    100 = perfect midden
    0   = buiten range
    """
    if value < min_v or value > max_v:
        return 0

    mid = (min_v + max_v) / 2
    dist = abs(value - mid)
    max_dist = (max_v - min_v) / 2

    if max_dist == 0:
        return 100

    return round(100 - (dist / max_dist * 100))


# ===================================================================
# 🤖 HOOFDFUNCTIE — Find Active Setup
# ===================================================================
def run_setup_agent(asset="BTC"):
    logger.info("🤖 Setup-Agent gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in Setup-Agent.")
        return []

    try:
        with conn.cursor() as cur:

            # ---------------------------------------
            # 1️⃣ Scores ophalen uit daily_scores
            #    Jouw tabel gebruikt: report_date
            # ---------------------------------------
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                ORDER BY report_date DESC
                LIMIT 1;
            """)
            row = cur.fetchone()

            if not row:
                logger.error("❌ Geen daily_scores gevonden.")
                return []

            macro_score, technical_score, market_score = row

            # ---------------------------------------
            # 2️⃣ Setups ophalen
            #    created_at bestaat ✔
            # ---------------------------------------
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
                ORDER BY created_at DESC
            """, (asset,))

            setups = cur.fetchall()

        if not setups:
            logger.warning("⚠️ Geen setups gevonden.")
            return []

        results = []
        best_setup = None
        best_score_total = -999999

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
            tech_match  = score_overlap(technical_score, min_tech, max_tech)
            market_match = score_overlap(market_score, min_market, max_market)

            total_match = round((macro_match + tech_match + market_match) / 3)

            active = (macro_match > 0 and tech_match > 0 and market_match > 0)

            # Best match bepalen
            if total_match > best_score_total:
                best_score_total = total_match
                best_setup = {
                    "setup_id": setup_id,
                    "name": name,
                    "symbol": symbol,
                    "total_match": total_match,
                    "macro_match": macro_match,
                    "tech_match": tech_match,
                    "market_match": market_match,
                    "active": active,
                    "strategy_type": strategy_type
                }

            # AI oordeel over setup
            ai_prompt = f"""
Je bent een professionele crypto analist.

MARKT TODAY:
- Macro: {macro_score}
- Technical: {technical_score}
- Market: {market_score}

SETUP:
- Naam: {name}
- Ranges:
  Macro {min_macro}-{max_macro}
  Technical {min_tech}-{max_tech}
  Market {min_market}-{max_market}

Geef één korte zin over hoe goed deze setup past.
"""
            ai_expl = ask_gpt_text(ai_prompt)

            results.append({
                "setup_id": setup_id,
                "name": name,
                "symbol": symbol,
                "active": active,
                "match_score": total_match,
                "macro_match": macro_match,
                "technical_match": tech_match,
                "market_match": market_match,
                "ai_comment": ai_expl,
                "best_of_day": False,
            })

            # --------------------------------------------
            # Opslaan in daily_setup_scores
            #    ✔ Jouw tabel: (setup_id, date, score, is_active, explanation)
            # --------------------------------------------
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores 
                        (setup_id, date, score, is_active, explanation)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (setup_id, date)
                    DO UPDATE SET 
                        score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation;
                """, (
                    setup_id,
                    date.today(),
                    total_match,
                    active,
                    ai_expl
                ))

        # ===================================================================
        # 4️⃣ BEST OF DAY MARKEREN  
        # ===================================================================
        if best_setup:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s AND date = %s
                """, (best_setup["setup_id"], date.today()))

        for r in results:
            if best_setup and r["setup_id"] == best_setup["setup_id"]:
                r["best_of_day"] = True

        conn.commit()
        logger.info("✅ Setup-Agent voltooid.")

        return {
            "active_setup": best_setup,
            "all_setups": results
        }

    except Exception:
        logger.error("❌ Setup-Agent crash:", exc_info=True)
        return {
            "active_setup": None,
            "all_setups": []
        }

    finally:
        conn.close()



# ===================================================================
# 🧠 LOSSE UITLEG GENERATOR
# ===================================================================
def generate_setup_explanation(setup_id: int) -> str:
    logger.info(f"🧠 AI-uitleg genereren voor setup {setup_id}...")

    conn = get_db_connection()
    if not conn:
        return "Fout: geen databaseverbinding."

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, symbol, trend, timeframe, 
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score
                FROM setups
                WHERE id = %s
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
Je bent een professionele crypto-analist.

Genereer een korte uitleg (max 3 zinnen) voor deze trading setup:

Naam: {name}
Asset: {symbol}
Trend: {trend}
Timeframe: {timeframe}

Score ranges:
- Macro: {min_macro} — {max_macro}
- Technical: {min_tech} — {max_tech}
- Market: {min_market} — {max_market}
"""

        explanation = ask_gpt_text(prompt)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET explanation = %s
                WHERE id = %s
            """, (explanation, setup_id))

            conn.commit()

        return explanation

    except Exception:
        logger.error("❌ Fout bij uitleg genereren:", exc_info=True)
        return "Fout bij uitleg genereren."

    finally:
        conn.close()

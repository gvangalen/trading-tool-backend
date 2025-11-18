import logging
from datetime import date
from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)


# =========================================================
# 🤖 SETUP AGENT — Kiest de beste setup van vandaag
# =========================================================
def run_setup_agent(asset="BTC"):
    logger.info("🤖 Setup-Agent gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB verbinding in Setup-Agent.")
        return []

    try:
        with conn.cursor() as cur:

            # -------------------------------------------------
            # 1️⃣ SCORE OPHALEN (macro + technical + market)
            # -------------------------------------------------
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE date = CURRENT_DATE
            """)
            row = cur.fetchone()

            if not row:
                logger.error("❌ Geen daily_scores gevonden voor vandaag.")
                return []

            macro_score, technical_score, market_score = row

            # -------------------------------------------------
            # 2️⃣ ALLE SETUPS OPHALEN
            # -------------------------------------------------
            cur.execute("""
                SELECT 
                    id, name,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    explanation,
                    action,
                    dynamic_investment,
                    symbol,
                    created_at
                FROM setups
                WHERE symbol = %s
                ORDER BY created_at DESC
            """, (asset,))

            setups = cur.fetchall()

        results = []
        best_setup = None
        best_score = -999

        # -------------------------------------------------
        # 3️⃣ PER SETUP: CHECK RANGES
        # -------------------------------------------------
        for (
            setup_id, name,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market,
            explanation, action,
            dynamic_investment,
            symbol, created_at
        ) in setups:

            macro_ok = min_macro <= macro_score <= max_macro
            tech_ok = min_tech <= technical_score <= max_tech
            market_ok = min_market_score <= market_score <= max_market_score

            active = macro_ok and tech_ok and market_ok

            # Totale setupscore = gemiddelde van 3
            total_score = round((macro_score + technical_score + market_score) / 3)

            # Beste setup kiezen
            if total_score > best_score:
                best_score = total_score
                best_setup = setup_id

            # AI-uitleg
            ai_explanation = ask_ai(f"""
                Setup naam: {name}
                Macro score: {macro_score}
                Technical score: {technical_score}
                Market score: {market_score}

                Min/Max macro: {min_macro} - {max_macro}
                Min/Max technical: {min_tech} - {max_tech}
                Min/Max market: {min_market} - {max_market}

                Past deze setup bij de huidige marktsituatie?
                Leg duidelijk uit waarom wel of niet.
            """)

            results.append({
                "setup_id": setup_id,
                "name": name,
                "score": total_score,
                "active": active,
                "best_of_day": setup_id == best_setup,
                "explanation": ai_explanation,
            })

            # -------------------------------------------------
            # 4️⃣ Opslaan in daily_setup_scores
            # -------------------------------------------------
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores 
                        (setup_id, date, score, is_active, explanation)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (setup_id, date) DO UPDATE
                    SET score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation
                """, (
                    setup_id,
                    date.today(),
                    total_score,
                    active,
                    ai_explanation
                ))

        # -------------------------------------------------
        # 5️⃣ Markeer welke setup WINNAAR is (voor Strategy-Agent)
        # -------------------------------------------------
        if best_setup:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s
                    AND date = %s
                """, (best_setup, date.today()))

        conn.commit()
        logger.info("✅ Setup-Agent klaar.")
        return results

    except Exception as e:
        logger.error(f"❌ Setup-Agent error: {e}", exc_info=True)
        return []

    finally:
        conn.close()

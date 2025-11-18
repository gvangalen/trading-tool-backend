import logging
import traceback
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# 🤖 SETUP AGENT — kiest de best passende setup van vandaag
# =========================================================
def run_setup_agent(asset="BTC"):
    logger.info("🤖 Setup-Agent gestart...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in Setup-Agent.")
        return []

    try:
        with conn.cursor() as cur:

            # -------------------------------------------------
            # 1️⃣ Scores ophalen (macro / technical / market)
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
            # 2️⃣ Alle setups ophalen voor dit asset
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
        best_setup_id = None
        best_total_score = -999

        # -------------------------------------------------
        # 3️⃣ Elke setup checken tegen de drie categorie-scores
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
            market_ok = min_market <= market_score <= max_market

            active = macro_ok and tech_ok and market_ok

            # AI-setup score
            total_score = round((macro_score + technical_score + market_score) / 3)

            # Beste setup bepalen
            if total_score > best_total_score:
                best_total_score = total_score
                best_setup_id = setup_id

            # -------------------------------------------------
            # 4️⃣ AI-uitleg (universele GPT helper)
            # -------------------------------------------------
            prompt = f"""
Je bent een professionele crypto analist.

We beoordelen een setup:

=== SETUP ===
Naam: {name}

=== SCORES ===
Macro score: {macro_score}
Technical score: {technical_score}
Market score: {market_score}

=== SETUP RANGES ===
Macro: {min_macro} - {max_macro}
Technical: {min_tech} - {max_tech}
Market: {min_market} - {max_market}

Vraag:
Past deze setup bij de huidige marktsituatie?
Geef een korte, duidelijke uitleg in het Nederlands.
"""

            ai_explanation = ask_gpt_text(prompt)

            results.append({
                "setup_id": setup_id,
                "name": name,
                "score": total_score,
                "active": active,
                "best_of_day": False,
                "explanation": ai_explanation,
            })

            # -------------------------------------------------
            # 5️⃣ Opslaan in daily_setup_scores
            # -------------------------------------------------
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
                    total_score,
                    active,
                    ai_explanation
                ))

        # -------------------------------------------------
        # 6️⃣ Beste setup markeren
        # -------------------------------------------------
        if best_setup_id:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s AND date = %s
                """, (best_setup_id, date.today()))

            # Markeer ook lokaal in resultatenlijst
            for r in results:
                if r["setup_id"] == best_setup_id:
                    r["best_of_day"] = True

        conn.commit()
        logger.info("✅ Setup-Agent voltooid.")
        return results

    except Exception as e:
        logger.error("❌ Setup-Agent crash:", exc_info=True)
        return []

    finally:
        conn.close()

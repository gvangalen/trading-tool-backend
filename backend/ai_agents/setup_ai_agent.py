import logging
import traceback
from datetime import date

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# 🤖 1. SETUP AGENT — kiest beste setup van vandaag
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
        # 3️⃣ Elke setup scoren
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
            total_score = round((macro_score + technical_score + market_score) / 3)

            # Beste setup bepalen
            if total_score > best_total_score:
                best_total_score = total_score
                best_setup_id = setup_id

            # -------------------------------------------------
            # 4️⃣ AI-uitleg genereren
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

            # Markeer lokaal
            for r in results:
                if r["setup_id"] == best_setup_id:
                    r["best_of_day"] = True

        conn.commit()
        logger.info("✅ Setup-Agent voltooid.")
        return results

    except Exception:
        logger.error("❌ Setup-Agent crash:", exc_info=True)
        return []

    finally:
        conn.close()



# =========================================================
# 🧠 2. **EXTRA FUNCTIE**: Losse AI-uitleg per setup (API-knop)
# =========================================================
def generate_setup_explanation(setup_id: int) -> str:
    """
    Deze functie wordt gebruikt door de frontend-knop:
    “Genereer AI uitleg”.

    - Haalt 1 setup op
    - Bouwt een korte prompt
    - Maakt AI-uitleg met ask_gpt_text()
    - Slaat het op in setups.explanation
    """
    logger.info(f"🧠 AI-uitleg genereren voor setup {setup_id}...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding.")
        return "Fout: geen databaseverbinding."

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, symbol, trend, timeframe, min_macro_score, max_macro_score,
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

        # -------------------------------
        # Prompt bouwen
        # -------------------------------
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

Maak het duidelijk, beknopt en begrijpelijk.
"""

        explanation = ask_gpt_text(prompt)

        # -------------------------------
        # Opslaan in DB
        # -------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE setups
                SET explanation = %s
                WHERE id = %s
            """, (explanation, setup_id))
            conn.commit()

        logger.info(f"✅ AI-uitleg opgeslagen voor setup {setup_id}")
        return explanation

    except Exception:
        logger.error("❌ Fout bij AI-uitleg genereren:", exc_info=True)
        return "Fout bij uitleg genereren."

    finally:
        conn.close()

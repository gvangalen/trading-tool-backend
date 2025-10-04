import os
import logging
import json
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🔧 Mock of Live modus via omgeving
AI_MODE = os.getenv("AI_MODE", "live").lower()
logger.info(f"🧪 AI_MODE geladen: {AI_MODE}")

def generate_ai_explanation(setup_id: int) -> str:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, trend, indicators
                FROM setups
                WHERE id = %s
            """, (setup_id,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"[setup_explanation] ❌ Geen setup gevonden met id {setup_id}")
                return "Geen uitleg beschikbaar."

            name, trend, indicators = row

            # ✅ Indicators veilig converteren naar lijst
            if isinstance(indicators, str):
                try:
                    indicators = json.loads(indicators)  # Probeer als JSON
                except json.JSONDecodeError:
                    indicators = [s.strip() for s in indicators.split(",")]  # Anders: split op komma

            if not isinstance(indicators, list):
                indicators = [str(indicators)]

            if AI_MODE == "mock":
                explanation = f"De setup '{name}' volgt een {trend}-trend en gebruikt indicatoren zoals: {', '.join(indicators)}."
                logger.info(f"🧪 Mock-modus actief: gegenereerde uitleg voor setup '{name}'")

                cur.execute("""
                    UPDATE setups
                    SET explanation = %s
                    WHERE id = %s
                """, (explanation, setup_id))
                conn.commit()

                return explanation

            # ✅ OpenAI import hier
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            prompt = (
                f"Geef een korte uitleg over de trading setup '{name}' met deze kenmerken:\n"
                f"- Marktconditie: {trend}\n"
                f"- Indicatoren: {', '.join(indicators)}\n"
                f"Antwoord in 2-3 zinnen in het Nederlands. Gebruik begrijpelijke taal."
            )

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=150,
            )

            explanation = response.choices[0].message.content.strip()
            logger.info(f"✅ AI-uitleg gegenereerd voor setup {setup_id}")

            cur.execute("""
                UPDATE setups
                SET explanation = %s
                WHERE id = %s
            """, (explanation, setup_id))
            conn.commit()

            return explanation

    except Exception as e:
        logger.error(f"[setup_explanation] ❌ Fout bij uitleg genereren: {e}")
        return "Fout bij uitleg genereren."
    finally:
        conn.close()

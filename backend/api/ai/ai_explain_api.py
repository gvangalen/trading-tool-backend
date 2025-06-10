from fastapi import APIRouter, HTTPException, Request
import openai
import os
import logging

router = APIRouter(prefix="/ai/explain")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ OpenAI API key instellen
openai.api_key = os.getenv("OPENAI_API_KEY")
AI_MODE = os.getenv("AI_MODE", "live").lower()  # "live" of "mock"

# ✅ Fallback uitlegfunctie
def fallback_explanation(name, indicators, trend):
    return (
        f"De setup '{name}' is gericht op een {trend}-markttrend en gebruikt indicatoren zoals: "
        f"{indicators}. Deze combinatie kan helpen om kansen te signaleren in deze marktomstandigheden."
    )

# ✅ POST: Uitleg genereren voor een trading setup
@router.post("/setup")
async def explain_setup(request: Request):
    try:
        data = await request.json()
        name = data.get("name")
        indicators = data.get("indicators")
        trend = data.get("trend")

        if not name or not indicators or not trend:
            logger.warning("⚠️ AIEX01: Ongeldige input ontvangen.")
            raise HTTPException(status_code=400, detail="Ongeldige input: naam, indicatoren en trend zijn verplicht.")

        # ✅ Fallback modus (mock)
        if AI_MODE == "mock":
            logger.info("🤖 AIEX02: Fallback uitleg actief (AI_MODE=mock).")
            return {"explanation": fallback_explanation(name, indicators, trend)}

        # ✅ Prompt bouwen
        prompt = (
            f"Geef een beknopte en begrijpelijke uitleg over de trading setup '{name}' "
            f"met de volgende kenmerken:\n"
            f"- Marktconditie: {trend}\n"
            f"- Indicatoren: {indicators}\n"
            f"Antwoord in maximaal 3 zinnen in het Nederlands. Gebruik eenvoudige taal."
        )

        # ✅ AI-aanroep
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=150,
        )

        explanation = response['choices'][0]['message']['content'].strip()
        logger.info(f"✅ AIEX03: Uitleg succesvol gegenereerd voor setup '{name}'.")
        return {"explanation": explanation}

    except Exception as e:
        logger.warning(f"⚠️ AIEX04: Fout bij AI-aanroep: {e}. Fallback wordt gebruikt.")
        return {"explanation": fallback_explanation(name, indicators, trend)}

# ✅ routers/ai_explain_api.py

from fastapi import APIRouter, HTTPException, Request
import openai
import os
import logging

router = APIRouter(prefix="/ai/explain")
logger = logging.getLogger(__name__)

# ✅ OpenAI API key instellen
openai.api_key = os.getenv("OPENAI_API_KEY")
AI_MODE = os.getenv("AI_MODE", "live").lower()  # "live" of "mock"

# ✅ Fallback uitleg functie
def fallback_explanation(name, indicators, trend):
    return (
        f"De setup '{name}' is gericht op een {trend} markttrend en gebruikt indicatoren zoals: "
        f"{indicators}. Deze combinatie kan helpen om kansen te signaleren in die marktomstandigheden."
    )

# ✅ AI-explain endpoint
@router.post("/ai/explain_setup")
async def explain_setup(request: Request):
    try:
        data = await request.json()
        name = data.get("name")
        indicators = data.get("indicators")
        trend = data.get("trend")

        if not name or not indicators or not trend:
            logger.warning("⚠️ Ongeldige input ontvangen")
            raise HTTPException(status_code=400, detail="Ongeldige input")

        # ✅ Gebruik fallback als AI_MODE = mock
        if AI_MODE == "mock":
            logger.info("🤖 Fallback uitleg actief (AI_MODE=mock)")
            return {"explanation": fallback_explanation(name, indicators, trend)}

        # ✅ AI-prompt bouwen
        prompt = (
            f"Geef een beknopte en begrijpelijke uitleg over de trading setup '{name}' "
            f"met de volgende kenmerken:\n"
            f"- Marktconditie: {trend}\n"
            f"- Indicatoren: {indicators}\n"
            f"Antwoord in maximaal 3 zinnen in het Nederlands. Gebruik eenvoudige taal."
        )

        # ✅ OpenAI aanroepen
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=150,
        )

        explanation = response['choices'][0]['message']['content'].strip()
        return {"explanation": explanation}

    except Exception as e:
        logger.warning(f"⚠️ Fout bij OpenAI: {e}. Fallback wordt gebruikt.")
        return {"explanation": fallback_explanation(name, indicators, trend)}

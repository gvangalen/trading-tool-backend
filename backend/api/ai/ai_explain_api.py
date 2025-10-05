from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from openai import OpenAI
import os
import logging

# ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Configuratie ophalen uit omgeving
DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
AI_MODE = os.getenv("AI_MODE", "live").lower()

# ✅ OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ✅ API router
router = APIRouter()

# ✅ Input model
class SetupExplainRequest(BaseModel):
    name: str
    trend: str
    indicators: List[str]

# ✅ Fallback-functie als AI faalt
def fallback_explanation(name: Optional[str], indicators: Optional[List[str]], trend: Optional[str]) -> str:
    name = name or "deze setup"
    indicators_str = ", ".join(indicators or ["onbekend"])
    trend = trend or "neutrale"
    return (
        f"De setup '{name}' is gericht op een {trend}-markttrend en gebruikt indicatoren zoals: "
        f"{indicators_str}. Deze combinatie kan helpen om kansen te signaleren in deze marktomstandigheden."
    )

# ✅ POST endpoint voor uitleggeneratie
@router.post("/setup")
async def explain_setup(payload: SetupExplainRequest):
    name = payload.name
    trend = payload.trend
    indicators = payload.indicators

    if AI_MODE == "mock":
        logger.info("🤖 AIEX01: Fallback uitleg actief (AI_MODE=mock).")
        return {"explanation": fallback_explanation(name, indicators, trend)}

    try:
        indicators_str = ", ".join(indicators)
        prompt = (
            f"Geef een beknopte en begrijpelijke uitleg over de trading setup '{name}' "
            f"met de volgende kenmerken:\n"
            f"- Marktconditie: {trend}\n"
            f"- Indicatoren: {indicators_str}\n"
            f"Antwoord in maximaal 3 zinnen in het Nederlands. Gebruik eenvoudige taal."
        )

        logger.debug(f"📤 AIEX-PROMPT: {prompt}")

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,  # ✅ Correct gebruik van de variabele
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=150,
        )

        explanation = response.choices[0].message.content.strip()
        logger.info(f"✅ AIEX02: Uitleg succesvol gegenereerd voor setup '{name}'.")
        return {"explanation": explanation}

    except Exception as e:
        logger.warning(f"⚠️ AIEX03: Fout bij AI-aanroep: {e}. Fallback wordt gebruikt.")
        return {"explanation": fallback_explanation(name, indicators, trend)}

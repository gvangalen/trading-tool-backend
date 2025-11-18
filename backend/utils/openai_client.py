import os
import json
import logging
import time
from dotenv import load_dotenv
from openai import OpenAI

# ============================================================
# ⚙️ Setup
# ============================================================
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

client = OpenAI(api_key=api_key)

LOG_FILE = "/tmp/ai_agent_debug.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ============================================================
# 🧰 JSON Sanitize Helper
# ============================================================
def sanitize_json_output(raw_text: str) -> dict:
    """
    Probeert een GPT-antwoord veilig om te zetten naar JSON.
    - Verwijdert codeblokken of extra tekst
    - Geeft fallback met raw_text als JSON parsing mislukt
    """
    if not raw_text:
        return {}

    cleaned = raw_text.strip()

    # Verwijder eventuele ```json ... ``` blokken
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json", "").strip()

    # Probeer direct JSON te parsen
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Zoek JSON in een grotere tekst
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start != -1 and end != -1:
        try:
            return json.loads(cleaned[start:end + 1])
        except Exception:
            pass

    logger.warning("⚠️ GPT-output kon niet als JSON worden gelezen.")
    return {"raw_text": raw_text[:400]}  # fallback truncated raw text


# ============================================================
# 🧠 GPT JSON Helper (default)
# ============================================================
def ask_gpt(
    prompt: str,
    system_role: str = "Je bent een professionele crypto-analist. Antwoord in het Nederlands.",
    retries: int = 3,
    delay: float = 3.0
) -> dict:
    """
    Universele OpenAI-helper voor alle AI Agents die JSON-output verwachten.
    Probeert meerdere keren, parsed JSON en fallback naar raw text.
    """
    if not api_key:
        logger.error("❌ OPENAI_API_KEY ontbreekt in .env.")
        return {"error": "Geen API-key gevonden"}

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Attempt {attempt}] Prompt-lengte: {len(prompt)}")

            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.6,
            )

            content = response.choices[0].message.content.strip()
            if not content:
                continue

            parsed = sanitize_json_output(content)
            logger.info(f"✅ [AI JSON Response] {str(parsed)[:120]}")
            return parsed

        except Exception as e:
            logger.warning(f"⚠️ Fout bij OpenAI poging {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI-pogingen mislukt.")
    return {"error": "Alle GPT-pogingen mislukt."}


# ============================================================
# 🧠 GPT TEXT Helper (ruwe tekst output)
# ============================================================
def ask_gpt_text(
    prompt: str,
    system_role: str = "Je bent een professionele crypto-analist. Antwoord in het Nederlands.",
    retries: int = 3,
    delay: float = 3.0
) -> str:
    """
    Retourneert ruwe tekst van GPT zonder JSON parsing.
    Ideaal voor:
    - setup-agent uitleg
    - strategy-agent uitleg
    - AI toelichting in rapporten
    - coaching / reflecties in natuurlijke taal
    """
    if not api_key:
        logger.error("❌ OPENAI_API_KEY ontbreekt in .env.")
        return "AI-error: geen API-key gevonden."

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Text Attempt {attempt}] Prompt-lengte: {len(prompt)}")

            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.6,
            )

            content = response.choices[0].message.content.strip()
            logger.info(f"📝 [AI Text Response] {content[:120]}")
            return content

        except Exception as e:
            logger.warning(f"⚠️ Fout bij OpenAI poging {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle tekstpogingen mislukt.")
    return "AI-error: geen geldig antwoord ontvangen."

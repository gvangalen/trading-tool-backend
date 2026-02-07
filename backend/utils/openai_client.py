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
model = os.getenv("OPENAI_MODEL", "gpt-5.2")

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
    Probeert AI-output veilig om te zetten naar JSON.
    """
    if not raw_text:
        return {}

    cleaned = raw_text.strip()

    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").replace("json", "").strip()

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start != -1 and end != -1:
        try:
            return json.loads(cleaned[start:end + 1])
        except Exception:
            pass

    logger.warning("⚠️ AI-output kon niet als JSON worden gelezen.")
    return {"raw_text": raw_text[:400]}

# ============================================================
# 🧠 GPT JSON Helper
# ============================================================
def ask_gpt(
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> dict:
    """
    Voor agents die JSON-output verwachten.
    """
    if not api_key:
        logger.error("❌ OPENAI_API_KEY ontbreekt.")
        return {"error": "Geen API-key"}

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI JSON Attempt {attempt}] Prompt-lengte={len(prompt)}")

            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()
            parsed = sanitize_json_output(content)
            logger.info(f"✅ [AI JSON OK] {str(parsed)[:120]}")
            return parsed

        except Exception as e:
            logger.warning(f"⚠️ AI JSON fout (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI JSON pogingen mislukt.")
    return {"error": "AI JSON mislukt"}

# ============================================================
# 🧠 GPT TEXT Helper
# ============================================================
def ask_gpt_text(
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> str:
    """
    Voor agents die vrije tekst schrijven (reports, uitleg).
    """
    if not api_key:
        logger.error("❌ OPENAI_API_KEY ontbreekt.")
        return "AI-error: geen API-key."

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Text Attempt {attempt}] Prompt-lengte={len(prompt)}")

            response = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()
            logger.info(f"📝 [AI Text OK] {content[:120]}")
            return content

        except Exception as e:
            logger.warning(f"⚠️ AI Text fout (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI tekstpogingen mislukt.")
    return "AI-error: geen geldig antwoord."

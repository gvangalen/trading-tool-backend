import os
import json
import logging
import time
import re
from dotenv import load_dotenv
from openai import OpenAI

# ============================================================
# ⚙️ Setup
# ============================================================
load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
model = os.getenv("OPENAI_MODEL", "gpt-5.2")

if not api_key:
    raise RuntimeError("OPENAI_API_KEY ontbreekt.")

client = OpenAI(api_key=api_key)

LOG_FILE = "/tmp/ai_agent_debug.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

# ============================================================
# 🔥 AI DEFAULTS
# ============================================================
TEXT_TEMP = float(os.getenv("OPENAI_TEXT_TEMP", "0.4"))
JSON_TEMP = float(os.getenv("OPENAI_JSON_TEMP", "0.2"))

TEXT_MAX_TOKENS = int(os.getenv("OPENAI_TEXT_MAX_TOKENS", "900"))
JSON_MAX_TOKENS = int(os.getenv("OPENAI_JSON_MAX_TOKENS", "600"))

TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "45"))

# ============================================================
# 🧰 ROBUST JSON SANITIZER (failsafe)
# ============================================================
def sanitize_json_output(raw_text: str) -> dict:
    """
    Probeeert AI output te converteren naar valide JSON.
    Alleen gebruikt als fallback.
    """

    if not raw_text:
        return {}

    text = raw_text.strip()

    # remove markdown fences
    text = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()

    # try direct parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # extract first JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        candidate = match.group()

        candidate = candidate.replace("\n", " ")
        candidate = candidate.replace("True", "true")
        candidate = candidate.replace("False", "false")

        try:
            return json.loads(candidate)
        except Exception:
            pass

    logger.warning("⚠️ AI-output kon niet als JSON worden gelezen.")
    logger.warning(f"RAW OUTPUT:\n{text[:800]}")
    return {}

# ============================================================
# 🧠 STRICT JSON (SCHEMA ENFORCED)  ⭐⭐⭐
# ============================================================
def ask_gpt_json(
    prompt: str,
    system_role: str,
    schema: dict,
    retries: int = 3,
    delay: float = 2.0,
) -> dict:
    """
    GARANDEERT geldig JSON volgens schema.
    Gebruik voor:
    ✔ master agent
    ✔ scoring
    ✔ trading decisions
    ✔ alles wat dashboard breekt als fout
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI JSON SCHEMA Attempt {attempt}]")

            response = client.responses.create(
                model=model,
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,

                # ⭐ JSON STRUCTUUR AFDWINGEN
                response_format={
                    "type": "json_schema",
                    "json_schema": schema,
                },

                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()
            parsed = json.loads(content)

            logger.info("✅ JSON schema output OK")
            return parsed

        except Exception as e:
            logger.warning(f"⚠️ JSON schema fout attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ JSON schema mislukt.")
    return {}

# ============================================================
# 🧠 LEGACY JSON (backwards compatible)
# ============================================================
def ask_gpt(
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> dict:
    """
    Voor agents waar structuur minder kritisch is.
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI JSON Attempt {attempt}]")

            response = client.responses.create(
                model=model,
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                response_format={"type": "json_object"},

                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()
            parsed = sanitize_json_output(content)

            if parsed:
                logger.info("✅ JSON parsed")
                return parsed

        except Exception as e:
            logger.warning(f"⚠️ JSON fout attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ JSON parsing mislukt.")
    return {}

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
    Voor rapporten, uitleg, analyses.
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Text Attempt {attempt}]")

            response = client.responses.create(
                model=model,
                temperature=TEXT_TEMP,
                top_p=0.9,
                max_output_tokens=TEXT_MAX_TOKENS,
                timeout=TIMEOUT,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()

            if content:
                logger.info("📝 Text OK")
                return content

        except Exception as e:
            logger.warning(f"⚠️ Text fout attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ AI tekst mislukt.")
    return "AI-error: geen geldig antwoord."

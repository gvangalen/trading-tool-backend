import os
import json
import logging
import time
import re
from typing import Any, Dict

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

LOG_FILE = os.getenv("OPENAI_LOG_FILE", "/tmp/ai_agent_debug.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger(__name__)

# ============================================================
# 🔥 AI DEFAULTS
# ============================================================
TEXT_TEMP = float(os.getenv("OPENAI_TEXT_TEMP", "0.4"))
JSON_TEMP = float(os.getenv("OPENAI_JSON_TEMP", "0.2"))

TEXT_MAX_TOKENS = int(os.getenv("OPENAI_TEXT_MAX_TOKENS", "900"))
JSON_MAX_TOKENS = int(os.getenv("OPENAI_JSON_MAX_TOKENS", "700"))

TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "45"))

# ============================================================
# 🧰 ROBUST JSON SANITIZER (FAILSAFE)
# ============================================================
def sanitize_json_output(raw_text: str) -> dict:
    """
    Probeeert AI output te converteren naar valide JSON.

    Beschermt tegen:
    - markdown fences
    - tekst vóór/na JSON
    - Python booleans
    - multi-line JSON
    """

    if not raw_text:
        return {}

    text = raw_text.strip()

    # verwijder markdown fences
    text = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()

    # directe parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # eerste {...} blok extraheren
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        candidate = match.group()

        candidate = candidate.replace("True", "true")
        candidate = candidate.replace("False", "false")
        candidate = candidate.replace("\n", " ")

        try:
            return json.loads(candidate)
        except Exception:
            pass

    logger.warning("⚠️ JSON parse fallback mislukt.")
    logger.warning(text[:500])
    return {}

# ============================================================
# ✅ GPT JSON CALL (STABLE & SDK SAFE)
# ============================================================
def ask_gpt_json(
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> Dict[str, Any]:
    """
    Stabiele JSON call.

    ✔ Werkt met huidige SDK
    ✔ Geen response_format nodig
    ✔ Geen crashes
    ✔ Fallback parsing
    ✔ Retries bij failure
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 JSON Attempt {attempt}")

            response = client.responses.create(
                model=model,
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = (response.output_text or "").strip()

            # 1️⃣ directe parse
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict):
                    logger.info("✅ JSON OK")
                    return parsed
            except Exception:
                pass

            # 2️⃣ fallback sanitize
            parsed = sanitize_json_output(content)
            if parsed:
                logger.info("✅ JSON OK (sanitized)")
                return parsed

            logger.warning("⚠️ JSON leeg → retry")

        except Exception as e:
            logger.warning(f"⚠️ JSON fout: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ JSON call mislukt")
    return {}

# ============================================================
# 🧠 GPT TEXT CALL
# ============================================================
def ask_gpt_text(
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> str:
    """
    Stabiele tekst-call voor rapporten en uitleg.
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 Text Attempt {attempt}")

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

            content = (response.output_text or "").strip()
            logger.info("📝 Text OK")
            return content

        except Exception as e:
            logger.warning(f"⚠️ Text fout: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Text call mislukt")
    return "AI-error"

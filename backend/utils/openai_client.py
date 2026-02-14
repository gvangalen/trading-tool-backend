import os
import json
import logging
import time
import re
from typing import Any, Dict, Optional

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
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
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
# 🧰 Robust JSON sanitize (fallback only)
# ============================================================
def sanitize_json_output(raw_text: str) -> dict:
    """
    Fallback parser. Normaal heb je dit bijna nooit nodig als schema enforced is,
    maar het voorkomt dat je ooit crasht.
    """
    if not raw_text:
        return {}

    text = raw_text.strip()

    # remove markdown fences
    text = re.sub(r"```json|```", "", text, flags=re.IGNORECASE).strip()

    # direct parse
    try:
        return json.loads(text)
    except Exception:
        pass

    # extract first {...}
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        candidate = match.group()

        # common fixes
        candidate = candidate.replace("True", "true").replace("False", "false")
        candidate = candidate.replace("\n", " ")

        try:
            return json.loads(candidate)
        except Exception:
            pass

    logger.warning("⚠️ AI-output kon niet als JSON worden gelezen (fallback sanitize faalde).")
    logger.warning(f"RAW OUTPUT:\n{text[:800]}")
    return {}

# ============================================================
# ✅ Schema-enforced JSON call (DEFINITIEF)
# ============================================================
def ask_gpt_json(
    prompt: str,
    system_role: str,
    schema: Dict[str, Any],
    retries: int = 3,
    delay: float = 2.0,
) -> Dict[str, Any]:
    """
    HARD JSON: schema enforced via Responses API.
    - Geen markdown
    - Geen extra tekst
    - Altijd geldig JSON object (als model het kan)
    - Fallback sanitize als laatste redmiddel
    """

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI JSON Attempt {attempt}] Prompt-lengte={len(prompt)}")

            response = client.responses.create(
                model=model,
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                # ✅ schema enforced output (Responses API)
                text_format={
                    "type": "json_schema",
                    "name": schema.get("name", "json_output"),
                    "schema": schema.get("schema", {}),
                    "strict": True,
                },
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            # Responses geeft plain output_text terug
            content = (response.output_text or "").strip()

            # 1) Try strict JSON loads first (zou moeten slagen)
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict):
                    logger.info("✅ [AI JSON OK | strict]")
                    return parsed
            except Exception:
                pass

            # 2) Fallback sanitizer (laatste redmiddel)
            parsed = sanitize_json_output(content)
            if isinstance(parsed, dict) and parsed:
                logger.info("✅ [AI JSON OK | sanitized fallback]")
                return parsed

            logger.warning("⚠️ JSON leeg/ongeldig → retry mogelijk")

        except Exception as e:
            logger.warning(f"⚠️ AI JSON fout (attempt {attempt}): {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI JSON pogingen mislukt.")
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

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Text Attempt {attempt}] Prompt-lengte={len(prompt)}")

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
            logger.info("📝 [AI Text OK]")
            return content

        except Exception as e:
            logger.warning(f"⚠️ AI Text fout (attempt {attempt}): {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI tekstpogingen mislukt.")
    return "AI-error: geen geldig antwoord."

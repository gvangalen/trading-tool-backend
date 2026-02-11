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

# 🔥 SPLIT DEFAULTS (ZEER BELANGRIJK)
TEXT_TEMP = float(os.getenv("OPENAI_TEXT_TEMP", "0.4"))
JSON_TEMP = float(os.getenv("OPENAI_JSON_TEMP", "0.2"))

TEXT_MAX_TOKENS = int(os.getenv("OPENAI_TEXT_MAX_TOKENS", "900"))
JSON_MAX_TOKENS = int(os.getenv("OPENAI_JSON_MAX_TOKENS", "500"))

# ============================================================
# 🧰 JSON Sanitize Helper
# ============================================================
def sanitize_json_output(raw_text: str) -> dict:
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

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI JSON Attempt {attempt}] Prompt-lengte={len(prompt)}")

            response = client.responses.create(
                model=model,

                # 🔥 JSON = LOW CREATIVITY
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=45,

                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()
            parsed = sanitize_json_output(content)

            logger.info("✅ [AI JSON OK]")
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

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 [AI Text Attempt {attempt}] Prompt-lengte={len(prompt)}")

            response = client.responses.create(
                model=model,

                # 🔥 TEXT = institutional writing
                temperature=TEXT_TEMP,
                top_p=0.9,
                max_output_tokens=TEXT_MAX_TOKENS,
                timeout=45,

                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": prompt},
                ],
            )

            content = response.output_text.strip()

            logger.info("📝 [AI Text OK]")
            return content

        except Exception as e:
            logger.warning(f"⚠️ AI Text fout (attempt {attempt}): {e}")

            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Alle AI tekstpogingen mislukt.")
    return "AI-error: geen geldig antwoord."

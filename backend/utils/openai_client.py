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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

api_key = os.getenv("OPENAI_API_KEY")

# 🔧 FIX: goedkopere default
model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

if not api_key:
    raise RuntimeError("OPENAI_API_KEY ontbreekt.")

client = OpenAI(api_key=api_key)

LOG_FILE = os.getenv("OPENAI_LOG_FILE", "/tmp/ai_agent_debug.log")

if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )

logger.info(f"🤖 OpenAI model: {model}")

# ============================================================
# 🔥 AI DEFAULTS
# ============================================================

TEXT_TEMP = float(os.getenv("OPENAI_TEXT_TEMP", "0.4"))
JSON_TEMP = float(os.getenv("OPENAI_JSON_TEMP", "0.2"))

# 🔧 realistischer limits
TEXT_MAX_TOKENS = int(os.getenv("OPENAI_TEXT_MAX_TOKENS", "800"))
JSON_MAX_TOKENS = int(os.getenv("OPENAI_JSON_MAX_TOKENS", "600"))

TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "45"))

# ============================================================
# 🧰 JSON parsing helpers
# ============================================================

_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _strip_fences(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"^\s*```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def sanitize_json_output(raw_text: str) -> Dict[str, Any]:

    if not raw_text:
        return {}

    text = _strip_fences(raw_text)

    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    m = _JSON_BLOCK_RE.search(text)

    if not m:
        return {}

    candidate = m.group(0)

    candidate = candidate.replace("True", "true").replace("False", "false")

    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _validate_schema_minimal(data: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> bool:

    if not schema:
        return True

    s = schema.get("schema") if isinstance(schema, dict) and "schema" in schema else schema

    if not isinstance(s, dict):
        return True

    required = s.get("required", [])

    for k in required:
        if k not in data:
            return False

    props = s.get("properties", {})

    for k, spec in props.items():

        if k not in data:
            continue

        if isinstance(spec, dict) and spec.get("type") == "number":

            v = data.get(k)

            if isinstance(v, (int, float)):
                continue

            if isinstance(v, str):

                try:
                    float(v.replace(",", "."))
                    continue
                except:
                    return False

    return True


# ============================================================
# 🧠 JSON prompt helpers
# ============================================================

def _make_json_guard_prompt(user_prompt: str, schema: Optional[Dict[str, Any]] = None) -> str:

    schema_hint = ""

    if schema:
        s = schema.get("schema") if isinstance(schema, dict) else schema

        if isinstance(s, dict):
            req = s.get("required", [])

            if isinstance(req, list) and req:
                schema_hint = "\nREQUIRED KEYS: " + ", ".join(req)

    return (
        "CRITICAL:\n"
        "Return ONLY valid JSON.\n"
        "No markdown.\n"
        "No explanations.\n"
        "No text outside JSON.\n"
        f"{schema_hint}\n\n"
        f"{user_prompt}"
    )


def _repair_prompt(bad_output: str, base_prompt: str) -> str:

    bad = (bad_output or "")[:2000]

    return (
        "CRITICAL:\n"
        "Return ONLY valid JSON.\n"
        "No markdown.\n"
        "No explanations.\n\n"
        "BASE INSTRUCTIONS:\n"
        f"{base_prompt}\n\n"
        "YOUR PREVIOUS OUTPUT:\n"
        f"{bad}\n"
    )


# ============================================================
# ✅ GPT JSON CALL
# ============================================================

def ask_gpt_json(
    *,
    prompt: str,
    system_role: str,
    schema: Optional[Dict[str, Any]] = None,
    retries: int = 2,   # 🔧 lager
    delay: float = 2.0,
) -> Dict[str, Any]:

    base_prompt = _make_json_guard_prompt(prompt, schema=schema)

    last_raw = ""

    for attempt in range(1, retries + 1):

        try:

            logger.info(f"🧠 JSON attempt {attempt}")

            response = client.responses.create(
                model=model,
                temperature=JSON_TEMP,
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": base_prompt},
                ],
            )

            content = (response.output_text or "").strip()

            last_raw = content

            parsed = sanitize_json_output(content)

            if parsed and _validate_schema_minimal(parsed, schema):
                return parsed

            # 🔧 repair 1x
            repair_prompt = _repair_prompt(content, base_prompt)

            response2 = client.responses.create(
                model=model,
                temperature=0,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": repair_prompt},
                ],
            )

            parsed2 = sanitize_json_output(response2.output_text)

            if parsed2:
                return parsed2

        except Exception as e:

            logger.warning(f"⚠️ JSON error attempt {attempt}: {e}")

            if attempt < retries:
                time.sleep(delay)

    logger.error("❌ JSON call failed")

    return {}


# ============================================================
# Backwards compatible alias
# ============================================================

def ask_gpt(prompt: str, system_role: str) -> Dict[str, Any]:

    return ask_gpt_json(prompt=prompt, system_role=system_role)


# ============================================================
# 🧠 GPT TEXT CALL
# ============================================================

def ask_gpt_text(
    *,
    prompt: str,
    system_role: str,
    retries: int = 2,
    delay: float = 2.0,
) -> str:

    last = ""

    for attempt in range(1, retries + 1):

        try:

            logger.info(f"🧠 Text attempt {attempt}")

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

            last = content

            return content

        except Exception as e:

            logger.warning(f"⚠️ Text error attempt {attempt}: {e}")

            if attempt < retries:
                time.sleep(delay)

    logger.error("❌ Text call failed")

    return last or "AI-error"

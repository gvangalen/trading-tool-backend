import os
import json
import logging
import time
import re
from typing import Any, Dict, Optional, List

from dotenv import load_dotenv
from openai import OpenAI

# ============================================================
# ⚙️ Setup
# ============================================================
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

api_key = os.getenv("OPENAI_API_KEY")
model = os.getenv("OPENAI_MODEL", "gpt-5.2")

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

# ============================================================
# 🔥 AI DEFAULTS
# ============================================================
TEXT_TEMP = float(os.getenv("OPENAI_TEXT_TEMP", "0.4"))
JSON_TEMP = float(os.getenv("OPENAI_JSON_TEMP", "0.2"))

TEXT_MAX_TOKENS = int(os.getenv("OPENAI_TEXT_MAX_TOKENS", "900"))
JSON_MAX_TOKENS = int(os.getenv("OPENAI_JSON_MAX_TOKENS", "700"))

TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "45"))

# ============================================================
# 🧰 JSON parsing helpers (robust)
# ============================================================
_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _strip_fences(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    # remove ```json ... ``` or ``` ... ```
    s = re.sub(r"^\s*```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def sanitize_json_output(raw_text: str) -> Dict[str, Any]:
    """
    Failsafe parser.
    - verwijdert markdown fences
    - pakt eerste JSON-object uit gemixte tekst
    - fixt True/False -> true/false
    """
    if not raw_text:
        return {}

    text = _strip_fences(raw_text)

    # 1) direct parse
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    # 2) find first {...}
    m = _JSON_BLOCK_RE.search(text)
    if not m:
        return {}

    candidate = m.group(0)

    # common fixes
    candidate = candidate.replace("True", "true").replace("False", "false")

    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _validate_schema_minimal(data: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> bool:
    """
    Minimal validation:
    - required keys aanwezig
    - numeric fields zijn numeric (als schema dat aangeeft)
    """
    if not schema:
        return True

    # accept both {"name":..., "schema":{...}} or raw schema
    s = schema.get("schema") if isinstance(schema, dict) and "schema" in schema else schema
    if not isinstance(s, dict):
        return True

    required = s.get("required", [])
    if isinstance(required, list):
        for k in required:
            if k not in data:
                return False

    props = s.get("properties", {})
    if isinstance(props, dict):
        for k, spec in props.items():
            if k not in data:
                continue
            if isinstance(spec, dict) and spec.get("type") == "number":
                if not _is_number(data.get(k)):
                    # allow numeric strings? -> not here; keep strict.
                    return False

    return True


def _make_json_guard_prompt(user_prompt: str, schema: Optional[Dict[str, Any]] = None) -> str:
    """
    Injecteert harde JSON regels in de USER prompt (SDK-safe).
    """
    schema_hint = ""
    if schema:
        # keep short; do not dump huge schema
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
        "Use numeric values for numeric fields.\n"
        f"{schema_hint}\n\n"
        f"{user_prompt}".strip()
    )


def _repair_prompt(bad_output: str, base_prompt: str, schema: Optional[Dict[str, Any]] = None) -> str:
    """
    Tweede kans prompt: “reformat to valid JSON only”.
    """
    bad = (bad_output or "")[:2000]
    return (
        "CRITICAL:\n"
        "You returned invalid or wrong-format output.\n"
        "Return ONLY valid JSON that matches the required keys.\n"
        "No markdown.\n"
        "No explanations.\n\n"
        "BASE INSTRUCTIONS:\n"
        f"{base_prompt}\n\n"
        "YOUR PREVIOUS OUTPUT (for repair):\n"
        f"{bad}\n"
    )


# ============================================================
# ✅ GPT JSON CALL (SDK-safe, schema-validated, never crashes)
# ============================================================
def ask_gpt_json(
    *,
    prompt: str,
    system_role: str,
    schema: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    delay: float = 2.0,
) -> Dict[str, Any]:
    """
    SDK-safe JSON helper:
    - gebruikt GEEN response_format/text_format (want jouw SDK faalt daarop)
    - dwingt JSON af via prompt
    - parse + schema validate
    - repair loop bij fout output
    """
    base_prompt = _make_json_guard_prompt(prompt, schema=schema)

    last_raw = ""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 JSON Attempt {attempt} | prompt_len={len(base_prompt)}")

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
                logger.info("✅ JSON OK")
                return parsed

            # Repair attempt inside same try
            repair = _repair_prompt(content, base_prompt, schema=schema)

            response2 = client.responses.create(
                model=model,
                temperature=0.0,  # ultra deterministic repair
                top_p=0.8,
                max_output_tokens=JSON_MAX_TOKENS,
                timeout=TIMEOUT,
                input=[
                    {"role": "system", "content": system_role},
                    {"role": "user", "content": repair},
                ],
            )

            content2 = (response2.output_text or "").strip()
            parsed2 = sanitize_json_output(content2)

            if parsed2 and _validate_schema_minimal(parsed2, schema):
                logger.info("✅ JSON OK (repair)")
                return parsed2

            logger.warning("⚠️ JSON output invalid/wrong-format → retry")

        except Exception as e:
            logger.warning(f"⚠️ JSON fout (attempt {attempt}): {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ JSON call mislukt (return {})")
    if last_raw:
        logger.error(f"Last raw (head): {last_raw[:600]}")
    return {}


# Backwards compatible alias (sommige files gebruiken ask_gpt)
def ask_gpt(prompt: str, system_role: str, retries: int = 3, delay: float = 2.0) -> Dict[str, Any]:
    return ask_gpt_json(prompt=prompt, system_role=system_role, retries=retries, delay=delay)


# ============================================================
# 🧠 GPT TEXT CALL (stable)
# ============================================================
def ask_gpt_text(
    *,
    prompt: str,
    system_role: str,
    retries: int = 3,
    delay: float = 2.0,
) -> str:
    last = ""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 Text Attempt {attempt} | prompt_len={len(prompt)}")

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
            logger.info("📝 Text OK")
            return content

        except Exception as e:
            logger.warning(f"⚠️ Text fout (attempt {attempt}): {e}", exc_info=True)
            if attempt < retries:
                time.sleep(delay * attempt)

    logger.error("❌ Text call mislukt")
    return last or "AI-error"

import os
import logging
import json
from dotenv import load_dotenv
from openai import OpenAI

from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.scoring_utils import get_scores_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup

# === ✅ Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === ✅ OpenAI client initialiseren (nog niet gebruikt in deze testversie)
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

# === ✅ Helper: veilig casten naar dict ===
def ensure_dict(obj, fallback=None, context=""):
    if isinstance(obj, dict):
        return obj
    try:
        if isinstance(obj, str):
            obj = obj.strip()
            if obj.startswith("{"):
                return json.loads(obj)
            else:
                logger.warning(f"⚠️ {context} was string zonder JSON: '{obj}'")
                return fallback or {"error": obj}
    except Exception as e:
        logger.warning(f"❌ Kon geen dict maken van {context}: {e}")
    logger.warning(f"⚠️ {context} is geen dict: {obj}")
    return fallback or {}

# === ✅ Testversie rapportgenerator (nog meer debug) ===
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    logger.info(f"📥 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    logger.info(f"📄 SETUP RAW TYPE = {type(setup_raw)} — VALUE = {setup_raw}")

    scores_raw = get_scores_for_symbol(symbol)
    logger.info(f"📊 SCORES RAW TYPE = {type(scores_raw)} — VALUE = {scores_raw}")

    strategy_raw = generate_strategy_from_setup(setup_raw)
    logger.info(f"🧠 STRATEGY RAW TYPE = {type(strategy_raw)} — VALUE = {strategy_raw}")

    return {
        "status": "ok",
        "symbol": symbol,
        "setup_raw_type": str(type(setup_raw)),
        "scores_raw_type": str(type(scores_raw)),
        "strategy_raw_type": str(type(strategy_raw)),
        "debug_note": "Deep type/value check voor raw data",
    }

    

# === ✅ Test handmatig draaien vanaf CLI
if __name__ == "__main__":
    report = generate_daily_report_sections("BTC")
    print(json.dumps(report, indent=2, ensure_ascii=False))

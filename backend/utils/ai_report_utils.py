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

# === ✅ Testversie rapportgenerator (zonder AI) ===
def generate_daily_report_sections(symbol: str = "BTC") -> dict:
    logger.info(f"📥 Start rapportgeneratie voor: {symbol}")

    setup_raw = get_latest_setup_for_symbol(symbol)
    setup = ensure_dict(setup_raw, context="setup")

    scores_raw = get_scores_for_symbol(symbol)
    scores = ensure_dict(scores_raw, context="scores")

    strategy_raw = generate_strategy_from_setup(setup)
    strategy = ensure_dict(strategy_raw, fallback={}, context="strategy")

    # 🧪 Debug logs
    logger.info("🧪 Volledige SETUP:")
    logger.info(setup_raw)
    logger.info("🧪 Dict SETUP:")
    logger.info(setup)

    logger.info("🧪 Volledige SCORES:")
    logger.info(scores_raw)
    logger.info("🧪 Dict SCORES:")
    logger.info(scores)

    logger.info("🧪 Volledige STRATEGY:")
    logger.info(strategy_raw)
    logger.info("🧪 Dict STRATEGY:")
    logger.info(strategy)

    return {
        "status": "ok",
        "symbol": symbol,
        "setup_type": type(setup).__name__,
        "scores_type": type(scores).__name__,
        "strategy_type": type(strategy).__name__,
        "debug_note": "AI output tijdelijk uitgeschakeld voor foutopsporing"
    }

# === ✅ Test handmatig draaien vanaf CLI
if __name__ == "__main__":
    report = generate_daily_report_sections("BTC")
    print(json.dumps(report, indent=2, ensure_ascii=False))

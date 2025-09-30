# ✅ backend/utils/ai_report_utils.py

from backend.utils.ai_score_utils import get_scores_for_symbol
from backend.utils.setup_utils import get_latest_setup_for_symbol
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
import datetime

def generate_daily_report_sections(symbol="BTC"):
    setup = get_latest_setup_for_symbol(symbol)
    strategy = generate_strategy_from_setup(setup)
    scores = get_scores_for_symbol(symbol)

    return {
        "btc_summary": "BTC is stabiel vandaag met lichte stijgingen...",
        "macro_summary": "Macro-economische condities blijven gemengd...",
        "setup_checklist": "Setup voldoet aan A+ criteria...",
        "priorities": "Focus op breakout boven 200MA...",
        "wyckoff_analysis": "Re-accumulatie fase met mogelijk spring...",
        "recommendations": "Koop bij retrace naar 21EMA + volume toename...",
        "conclusion": "Markt neigt bullish, maar afwachten breakout...",
        "outlook": "Verwacht consolidatie gevolgd door uitbraak...",
        "macro_score": scores.get("macro_score", 0),
        "technical_score": scores.get("technical_score", 0),
        "setup_score": scores.get("setup_score", 0),
        "sentiment_score": scores.get("sentiment_score", 0)
    }

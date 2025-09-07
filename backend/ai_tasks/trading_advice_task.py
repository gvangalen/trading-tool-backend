import os
import json
import logging
import traceback
from celery import shared_task
from backend.utils.db import get_db_connection
from backend.utils.setup_validator import validate_setups
from backend.utils.ai_strategy_utils import generate_strategy_advice

# ✅ Logging instellen
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# === 📊 Advies genereren ===
@shared_task(name="ai_tasks.generate_trading_advice")
def generate_trading_advice():
    logger.info("📊 Start tradingadvies generatie")
    try:
        # ✅ 1. Valideer setups
        setups = validate_setups()

        # ✅ 2. Haal scores op per categorie
        macro_score = calculate_avg_score(setups, "macro")
        technical_score = calculate_avg_score(setups, "technical")

        # ✅ 3. Haal marktdata uit database (alleen BTC)
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT price, change_24h FROM market_data WHERE symbol = 'BTC' ORDER BY timestamp DESC LIMIT 1")
            row = cur.fetchone()
        conn.close()

        if not row:
            logger.warning("⚠️ Geen marktdata beschikbaar voor BTC")
            return

        market_data = {
            "symbol": "BTC",
            "price": float(row[0]),
            "change_24h": float(row[1]),
        }

        # ✅ 4. Genereer advies via AI
        advice = generate_strategy_advice(setups, macro_score, technical_score, market_data)

        # ✅ 5. Sla advies op als JSON
        with open("trading_advice.json", "w") as f:
            json.dump(advice, f, indent=2)

        logger.info("✅ Tradingadvies succesvol gegenereerd")

    except Exception as e:
        logger.error(f"❌ Fout in generate_trading_advice: {e}")
        logger.error(traceback.format_exc())


# === 🧮 Gemiddelde score per categorie (macro/technical/etc.) ===
def calculate_avg_score(setups, category):
    scores = []
    for setup in setups:
        breakdown = setup.get("score_breakdown", {}).get(category, {})
        if breakdown.get("total", 0) > 0:
            scores.append(breakdown.get("score", 0))
    return round(sum(scores) / len(scores), 2) if scores else 0

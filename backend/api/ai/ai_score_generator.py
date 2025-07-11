# ✅ backend/utils/calculate_combined_score.py
import sys
import os
import logging
from typing import Dict, Union

# ✅ Zorg dat backend imports altijd werken, ook als script los draait
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from backend.utils.db import get_db_connection  # Let op juiste pad

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def calculate_combined_score(symbol: str = "BTC") -> Dict[str, Union[str, float, int]]:
    """
    Haalt macro-, technische- en sentiment-score op uit setup_scores tabel.
    Berekent een gecombineerde AI-score (gemiddelde van 3).
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ COMB01: Geen databaseverbinding.")
        return {"symbol": symbol, "error": "Geen databaseverbinding", "total_score": 0}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, sentiment_score
                FROM setup_scores
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ COMB02: Geen scoregegevens gevonden voor {symbol}")
            return {"symbol": symbol, "error": "Geen scoregegevens", "total_score": 0}

        try:
            macro = float(row[0]) if row[0] is not None else 0
            technical = float(row[1]) if row[1] is not None else 0
            sentiment = float(row[2]) if row[2] is not None else 0
        except (ValueError, TypeError):
            logger.warning(f"⚠️ COMB03: Ongeldige waarden (niet-numeriek) voor {symbol}")
            return {"symbol": symbol, "error": "Niet-numerieke waarden", "total_score": 0}

        total = round((macro + technical + sentiment) / 3, 2)
        logger.info(f"✅ COMB04: Totale score voor {symbol} = {total}")

        return {
            "symbol": symbol,
            "macro_score": macro,
            "technical_score": technical,
            "sentiment_score": sentiment,
            "total_score": total
        }

    except Exception as e:
        logger.error(f"❌ COMB05: Fout bij scoreberekening voor {symbol}: {e}")
        return {"symbol": symbol, "error": str(e), "total_score": 0}

    finally:
        conn.close()


# ✅ Standalone testmogelijkheid
if __name__ == "__main__":
    result = calculate_combined_score("BTC")
    print(result)

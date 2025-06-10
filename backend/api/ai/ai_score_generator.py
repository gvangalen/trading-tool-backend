from utils.db import get_db_connection
import logging

logger = logging.getLogger(__name__)

def calculate_combined_score(symbol: str = "BTC") -> dict:
    """
    Haalt macro-, technische- en sentiment-score op uit setup_scores tabel.
    Berekent ook een totale AI-score.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ COMB01: Geen databaseverbinding.")
        return {"error": "Geen databaseverbinding"}

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
            logger.warning(f"⚠️ COMB02: Geen score gevonden voor {symbol}")
            return {"error": f"Geen score gevonden voor {symbol}"}

        macro, technical, sentiment = row
        total = round((macro + technical + sentiment) / 3, 2)

        logger.info(f"✅ COMB03: Score geladen voor {symbol}: totaal={total}")
        return {
            "symbol": symbol,
            "macro_score": macro,
            "technical_score": technical,
            "sentiment_score": sentiment,
            "total_score": total
        }

    except Exception as e:
        logger.error(f"❌ COMB04: Fout bij scoreberekening: {e}")
        return {"error": str(e)}

    finally:
        conn.close()

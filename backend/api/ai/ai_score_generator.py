from utils.db import get_db_connection

def calculate_combined_score(symbol: str = "BTC") -> dict:
    """
    Haalt macro-, technische- en sentiment-score op uit setup_scores tabel.
    Berekent ook een totaal AI-score.
    """
    conn = get_db_connection()
    if not conn:
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
            return {"error": f"Geen score gevonden voor {symbol}"}

        macro, technical, sentiment = row
        total = macro + technical + sentiment

        return {
            "symbol": symbol,
            "macro_score": macro,
            "technical_score": technical,
            "sentiment_score": sentiment,
            "total_score": total
        }

    except Exception as e:
        return {"error": str(e)}

    finally:
        conn.close()

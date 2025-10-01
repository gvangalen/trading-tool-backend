from backend.utils.db import get_db_connection

def get_latest_setup_for_symbol(symbol: str = "BTC"):
    """
    Haalt de meest recente setup op voor het opgegeven symbool.
    """
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, trend, score, symbol, created_at
                FROM setups
                WHERE symbol = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "trend": row[3],
                    "score": row[4],
                    "symbol": row[5],
                    "created_at": row[6]
                }
            else:
                return None
    finally:
        conn.close()

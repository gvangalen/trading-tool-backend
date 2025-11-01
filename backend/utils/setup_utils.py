from backend.utils.db import get_db_connection

def get_all_setups(symbol: str = "BTC"):
    """
    Haalt alle setups op voor het opgegeven symbool.
    """
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, min_score, max_score, explanation, action, dynamic_investment
                FROM setups
                WHERE symbol = %s
            """, (symbol,))
            rows = cur.fetchall()

            setups = []
            for row in rows:
                setups.append({
                    "id": row[0],
                    "name": row[1],
                    "min_score": row[2],
                    "max_score": row[3],
                    "explanation": row[4],
                    "action": row[5],
                    "dynamic_investment": row[6],
                })

            return setups

    finally:
        conn.close()

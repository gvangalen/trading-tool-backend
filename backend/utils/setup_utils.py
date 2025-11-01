from backend.utils.db import get_db_connection
from datetime import date

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

def get_latest_setup_for_symbol(symbol: str = "BTC"):
    """
    Haalt de meest recente setup op voor het opgegeven symbool,
    inclusief de score van vandaag uit daily_setup_scores (indien beschikbaar).
    """
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.name, s.description, s.trend, ds.score, s.symbol, s.created_at
                FROM setups s
                LEFT JOIN daily_setup_scores ds ON s.id = ds.setup_id AND ds.date = %s
                WHERE s.symbol = %s
                ORDER BY s.created_at DESC
                LIMIT 1
            """, (date.today(), symbol))
            row = cur.fetchone()

            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "trend": row[3],
                    "score": row[4],  # ✅ uit daily_setup_scores
                    "symbol": row[5],
                    "created_at": row[6]
                }
            else:
                return None
    finally:
        conn.close()

from backend.utils.db import get_db_connection
from datetime import date
import logging

logger = logging.getLogger(__name__)

# =========================================================
# ✅ Alle setups ophalen
# =========================================================
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
                ORDER BY created_at DESC
            """, (symbol,))
            rows = cur.fetchall()

            setups = [
                {
                    "id": r[0],
                    "name": r[1],
                    "min_score": r[2],
                    "max_score": r[3],
                    "explanation": r[4],
                    "action": r[5],
                    "dynamic_investment": r[6],
                }
                for r in rows
            ]
            logger.info(f"📦 {len(setups)} setups opgehaald voor {symbol}")
            return setups

    except Exception as e:
        logger.error(f"❌ Fout in get_all_setups: {e}", exc_info=True)
        return []
    finally:
        conn.close()


# =========================================================
# ✅ Meest recente setup + daily score ophalen
# =========================================================
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
                LEFT JOIN daily_setup_scores ds 
                       ON s.id = ds.setup_id AND ds.date = %s
                WHERE s.symbol = %s
                ORDER BY s.created_at DESC
                LIMIT 1
            """, (date.today(), symbol))
            row = cur.fetchone()

            if not row:
                logger.info(f"⚠️ Geen setup gevonden voor {symbol}")
                return None

            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "trend": row[3],
                "score": row[4],
                "symbol": row[5],
                "created_at": row[6],
            }

    except Exception as e:
        logger.error(f"❌ Fout in get_latest_setup_for_symbol: {e}", exc_info=True)
        return None
    finally:
        conn.close()

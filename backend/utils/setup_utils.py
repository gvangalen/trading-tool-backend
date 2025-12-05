import logging
from datetime import date
from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)


# =========================================================
# 🔥 1. Alle setups ophalen (USER-SPECIFIEK)
# =========================================================
def get_all_setups(symbol: str = "BTC", user_id: int = None):
    """
    Haalt ALLE setups op voor een gebruiker + symbool.
    Zonder user_id → geen resultaten (veiligheidsredenen).
    """

    if user_id is None:
        logger.error("❌ get_all_setups() zonder user_id aangeroepen — blokkeren!")
        return []

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij get_all_setups()")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id,
                    name,
                    min_macro_score,
                    max_macro_score,
                    min_technical_score,
                    max_technical_score,
                    min_market_score,
                    max_market_score,
                    explanation,
                    action,
                    dynamic_investment,
                    symbol,
                    created_at
                FROM setups
                WHERE symbol = %s
                AND user_id = %s
                ORDER BY created_at DESC
            """, (symbol, user_id))

            rows = cur.fetchall()

        setups = []
        for r in rows:
            setups.append({
                "id": r[0],
                "name": r[1],
                "min_macro_score": r[2],
                "max_macro_score": r[3],
                "min_technical_score": r[4],
                "max_technical_score": r[5],
                "min_market_score": r[6],
                "max_market_score": r[7],
                "explanation": r[8],
                "action": r[9],
                "dynamic_investment": r[10],
                "symbol": r[11],
                "created_at": r[12],
            })

        logger.info(f"📦 {len(setups)} setups opgehaald voor {symbol} (user {user_id})")
        return setups

    except Exception as e:
        logger.error(f"❌ Fout in get_all_setups(): {e}", exc_info=True)
        return []

    finally:
        conn.close()


# =========================================================
# 🔥 2. Laatste setup + daily score ophalen (USER-SPECIFIEK)
# =========================================================
def get_latest_setup_for_symbol(symbol: str = "BTC", user_id: int = None):
    """
    Haalt de meest recente setup voor een gebruiker op + de daily setup score.
    """

    if user_id is None:
        logger.error("❌ get_latest_setup_for_symbol() zonder user_id — blokkeren.")
        return None

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding bij get_latest_setup_for_symbol()")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    s.id,
                    s.name,
                    s.explanation,
                    s.action,
                    ds.score,
                    s.symbol,
                    s.created_at
                FROM setups s
                LEFT JOIN daily_setup_scores ds
                    ON s.id = ds.setup_id
                   AND ds.report_date = %s
                   AND ds.user_id = %s
                WHERE s.symbol = %s
                AND s.user_id = %s
                ORDER BY s.created_at DESC
                LIMIT 1
            """, (date.today(), user_id, symbol, user_id))

            row = cur.fetchone()

        if not row:
            logger.info(f"⚠️ Geen setup gevonden voor {symbol} (user {user_id})")
            return None

        return {
            "id": row[0],
            "name": row[1],
            "explanation": row[2],
            "action": row[3],
            "score": row[4],
            "symbol": row[5],
            "created_at": row[6],
        }

    except Exception as e:
        logger.error(f"❌ Fout in get_latest_setup_for_symbol(): {e}", exc_info=True)
        return None

    finally:
        conn.close()

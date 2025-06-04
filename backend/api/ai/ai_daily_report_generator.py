import logging
from utils.db import get_db_connection
from utils.ai_strategy_utils import generate_strategy_from_setup

logger = logging.getLogger(__name__)

def generate_daily_report(asset: str = "BTC") -> dict:
    """
    Genereert een AI-tradingrapport voor een specifieke asset (default: BTC)
    """
    conn = get_db_connection()
    if not conn:
        return {"error": "DB-verbinding mislukt."}

    try:
        with conn.cursor() as cur:
            # Haal meest recente setup op
            cur.execute("""
                SELECT data FROM setups
                WHERE data->>'symbol' = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (asset,))
            row = cur.fetchone()
            if not row:
                return {"error": f"Geen setup gevonden voor {asset}"}
            setup = row[0]

        # Genereer strategie
        strategy = generate_strategy_from_setup(setup)
        if not strategy:
            return {"error": "Strategie-generatie mislukt"}

        # Voeg extra metadata toe aan rapport
        report = {
            "asset": asset,
            "setup_name": setup.get("name"),
            "strategy": strategy,
            "timestamp": setup.get("timestamp"),
        }
        return report

    except Exception as e:
        logger.error(f"❌ Fout bij rapportgeneratie: {e}")
        return {"error": str(e)}

    finally:
        conn.close()

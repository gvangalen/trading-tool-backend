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
        logger.error("❌ RAP10: Databaseverbinding mislukt.")
        return {"error": "Databaseverbinding mislukt."}

    try:
        with conn.cursor() as cur:
            # Haal de laatste setup op voor deze asset
            cur.execute("""
                SELECT data
                FROM setups
                WHERE data->>'symbol' = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (asset,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ RAP11: Geen setup gevonden voor asset '{asset}'")
            return {"error": f"Geen setup gevonden voor asset '{asset}'"}

        setup = row[0]  # JSONB veld (dict)

        # Genereer strategie op basis van setup
        strategy = generate_strategy_from_setup(setup)
        if not strategy:
            logger.warning("⚠️ RAP12: Strategie-generatie mislukt voor opgehaalde setup.")
            return {"error": "Strategie-generatie mislukt."}

        # Samengesteld rapport
        report = {
            "asset": asset,
            "setup_name": setup.get("name", "Onbekende setup"),
            "strategy": strategy,
            "timestamp": setup.get("timestamp") or None,
        }

        logger.info(f"✅ RAP13: Rapport succesvol gegenereerd voor {asset}")
        return report

    except Exception as e:
        logger.error(f"❌ RAP14: Fout bij genereren rapport: {e}")
        return {"error": str(e)}

    finally:
        conn.close()

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
            cur.execute("""
                SELECT data
                FROM setups
                WHERE data->>'symbol' = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (asset,))
            row = cur.fetchone()

        if not row or not isinstance(row[0], dict):
            logger.warning(f"⚠️ RAP11: Geen geldige setup gevonden voor asset '{asset}'")
            return {"error": f"Geen geldige setup gevonden voor asset '{asset}'"}

        setup = row[0]

        # ✅ Strategie genereren
        strategy = generate_strategy_from_setup(setup)
        if not strategy or not isinstance(strategy, dict):
            logger.warning("⚠️ RAP12: Strategie-generatie mislukt voor opgehaalde setup.")
            return {"error": "Strategie-generatie mislukt."}

        # ✅ Rapportstructuur samenstellen
        report = {
            "asset": asset,
            "setup_name": setup.get("name", "Onbekende setup"),
            "timestamp": setup.get("timestamp", None),
            "strategy": strategy,
            # Toekomstige uitbreidingen:
            # "scores": { ... },
            # "ai_explanation": "...",
            # "trend_analysis": "...",
        }

        logger.info(f"✅ RAP13: Dagrapport succesvol gegenereerd voor {asset}")
        return report

    except Exception as e:
        logger.error(f"❌ RAP14: Fout bij genereren dagrapport: {e}")
        return {"error": str(e)}

    finally:
        conn.close()

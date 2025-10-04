from fastapi import APIRouter, HTTPException, Query
from backend.utils.db import get_db_connection
from backend.utils.ai_strategy_utils import generate_strategy_from_setup
import logging
import json

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/report/generate")
def generate_daily_report(asset: str = Query("BTC", description="Asset waarvoor het rapport gegenereerd moet worden")):
    """
    ✅ Genereert een tradingrapport met AI-strategie op basis van meest recente setup.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("❌ RAP10: Databaseverbinding mislukt.")
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

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

        if not row or not row[0]:
            logger.warning(f"⚠️ RAP11: Geen setup gevonden voor asset '{asset}'")
            raise HTTPException(status_code=404, detail=f"Geen setup gevonden voor '{asset}'")

        # ✅ JSON-str naar dict converteren indien nodig
        setup = row[0]
        if isinstance(setup, str):
            try:
                setup = json.loads(setup)
            except json.JSONDecodeError:
                logger.error(f"❌ RAP15: Setup kon niet als JSON worden ingelezen: {setup}")
                raise HTTPException(status_code=500, detail="Setup is geen geldig JSON-object.")

        if not isinstance(setup, dict):
            logger.warning("⚠️ RAP16: Setup is geen dictionary na parsing.")
            raise HTTPException(status_code=500, detail="Setupformaat ongeldig.")

        strategy = generate_strategy_from_setup(setup)
        if not strategy or not isinstance(strategy, dict):
            logger.warning("⚠️ RAP12: Strategie-generatie mislukt voor opgehaalde setup.")
            raise HTTPException(status_code=500, detail="Strategie-generatie mislukt.")

        report = {
            "asset": asset,
            "setup_name": setup.get("name", "Onbekende setup"),
            "timestamp": setup.get("timestamp"),
            "strategy": strategy,
        }

        logger.info(f"✅ RAP13: Dagrapport succesvol gegenereerd voor {asset}")
        return report

    except Exception as e:
        logger.error(f"❌ RAP14: Fout bij genereren dagrapport: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

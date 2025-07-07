from fastapi import APIRouter, HTTPException, Request
from backend.utils.db import get_db_connection
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ GET: Onboarding-status per gebruiker ophalen
@router.get("/onboarding/status/{user_id}")
async def get_onboarding_status(user_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT setup_done, technical_done, macro_done, dashboard_done
                FROM onboarding_status
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Onboarding-status niet gevonden.")

        return {
            "setup_done": row[0],
            "technical_done": row[1],
            "macro_done": row[2],
            "dashboard_done": row[3],
        }

    except Exception as e:
        logger.error(f"❌ ONB01: Fout bij ophalen onboarding status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ PUT: Specifieke onboarding-stap updaten
@router.put("/onboarding/status/{user_id}")
async def update_onboarding_status(user_id: int, update_data: dict):
    step = update_data.get("step")
    done = update_data.get("done")

    if step not in ["setup_done", "technical_done", "macro_done", "dashboard_done"]:
        raise HTTPException(status_code=400, detail="❌ Ongeldige onboarding-stap.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt.")

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE onboarding_status
                SET {step} = %s
                WHERE user_id = %s
            """, (done, user_id))
            conn.commit()

        logger.info(f"✅ ONB02: Stap '{step}' geüpdatet voor gebruiker {user_id}")
        return {"message": f"Stap '{step}' succesvol bijgewerkt."}

    except Exception as e:
        logger.error(f"❌ ONB02: Fout bij bijwerken onboarding status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

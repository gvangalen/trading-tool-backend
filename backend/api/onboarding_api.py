# ✅ onboarding_status_api.py — FastAPI versie

from fastapi import APIRouter, HTTPException
from db import get_db_connection
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# ✅ Ophalen van onboarding status
@router.get("/api/onboarding_status/{user_id}")
async def get_onboarding_status(user_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT setup_done, technical_done, macro_done, dashboard_done
                FROM onboarding_status
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            return {}, 404

        return {
            "setup_done": row[0],
            "technical_done": row[1],
            "macro_done": row[2],
            "dashboard_done": row[3],
        }

    except Exception as e:
        logger.error(f"❌ Error fetching onboarding status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# ✅ Updaten van een stap in de onboarding
@router.put("/api/onboarding_status/{user_id}")
async def update_onboarding_status(user_id: int, update_data: dict):
    step = update_data.get("step")
    done = update_data.get("done")

    if step not in ["setup_done", "technical_done", "macro_done", "dashboard_done"]:
        raise HTTPException(status_code=400, detail="Invalid onboarding step.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE onboarding_status
                SET {step} = %s
                WHERE user_id = %s
            """, (done, user_id))
            conn.commit()

        logger.info(f"✅ Onboarding step '{step}' updated for user {user_id}")
        return {"message": f"Step '{step}' successfully updated."}

    except Exception as e:
        logger.error(f"❌ Error updating onboarding status: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

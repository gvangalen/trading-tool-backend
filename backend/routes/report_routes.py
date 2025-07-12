print("🟢 report_routes wordt geladen ✅")
from fastapi import APIRouter
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/daily_report/summary")
def get_daily_report_summary():
    """
    Geeft een samenvatting van het dagelijkse rapport terug.
    Placeholder voor AI-gegenereerde inzichten.
    """
    logger.info("📄 API-call: /api/daily_report/summary")

    return {
        "status": "ok",
        "summary": "Samenvatting nog niet beschikbaar. AI gegenereerde content volgt binnenkort."
    }

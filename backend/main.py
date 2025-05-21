# ✅ main.py — FastAPI-versie van de oude app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import os

# ✅ Routers importeren (gebaseerd op nieuwe structuur)
from api.market_data_api import router as market_data_router
from api.macro_data_api import router as macro_data_router
from api.technical_data_api import router as technical_data_router
from api.setups_api import router as setups_router
from api.dashboard_api import router as dashboard_router
from api.report_api import router as report_router
from api.ai.ai_explain_api import router as ai_explain_router
from api.ai.ai_strategy_api import router as ai_strategy_router
from api.onboarding_api import router as onboarding_router  

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ FastAPI app initialiseren
app = FastAPI(title="Market Dashboard API", version="1.0")

# ✅ CORS instellen
origins = [
    "https://market-dashboard-frontend.s3-website.eu-north-1.amazonaws.com",
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Routers koppelen
app.include_router(market_data_router, prefix="/api")
app.include_router(macro_data_router, prefix="/api")
app.include_router(technical_data_router, prefix="/api")
app.include_router(setups_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(report_router, prefix="/api")
app.include_router(ai_explain_router, prefix="/api")
app.include_router(ai_strategy_router, prefix="/api")
app.include_router(onboarding_router, prefix="/api")

# ✅ Health check
@app.get("/api/health")
def health_check():
    logger.info("📡 Health check aangeroepen.")
    return {"status": "ok", "message": "API is running"}

# ✅ CORS test
@app.get("/api/test-cors")
def test_cors():
    logger.info("🧪 CORS test endpoint aangeroepen.")
    return {"success": True, "message": "CORS werkt correct vanaf frontend."}

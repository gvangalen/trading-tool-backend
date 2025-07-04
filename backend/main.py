import sys, os
import logging
import importlib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ✅ Zorg dat relative imports werken
sys.path.insert(0, os.path.abspath("."))

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ FastAPI app initialiseren
app = FastAPI(title="Market Dashboard API", version="1.0")

# ✅ Toegestane origins (pas aan indien nodig)
origins = [
    "http://localhost:3000",
    "http://143.47.186.148",
    "http://143.47.186.148:3000",
]

# ✅ CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Veilige router-import functie
def safe_include(import_path, name=""):
    try:
        module = importlib.import_module(import_path)
        app.include_router(module.router, prefix="/api")
        logger.info(f"✅ Router geladen: {name or import_path}")
    except Exception as e:
        logger.warning(f"❌ Kon router '{name}' niet laden ({import_path}): {e}")

# ✅ Routers includen
safe_include("backend.api.market_data_api", "market_data_api")
safe_include("backend.api.macro_data_api", "macro_data_api")
safe_include("backend.api.technical_data_api", "technical_data_api")
safe_include("backend.api.setups_api", "setups_api")
safe_include("backend.api.dashboard_api", "dashboard_api")
safe_include("backend.api.report_api", "report_api")
safe_include("backend.api.ai.ai_explain_api", "ai_explain_api")
safe_include("backend.api.ai.ai_strategy_api", "ai_strategy_api")
safe_include("backend.api.ai.ai_trading_api", "ai_trading_api")
safe_include("backend.api.ai.validate_setups_api", "validate_setups_api")  # ✅ Toegevoegd
safe_include("backend.api.onboarding_api", "onboarding_api")
safe_include("backend.api.sidebar_api", "sidebar_api")

# ✅ Health endpoint
@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "API is running"}

# ✅ CORS test
@app.get("/api/test-cors")
def test_cors():
    return {"success": True, "message": "CORS werkt correct vanaf frontend."}

# ✅ Startserver
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5002)

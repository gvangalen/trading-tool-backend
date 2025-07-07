import sys, os
import logging
import importlib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ✅ Voeg rootpad toe zodat relative imports werken
sys.path.insert(0, os.path.abspath("."))

# ✅ Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ App
app = FastAPI(title="Market Dashboard API", version="1.0")

# ✅ CORS
origins = [
    "http://localhost:3000",
    "http://143.47.186.148",
    "http://143.47.186.148:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Router-loader
def safe_include(import_path, name=""):
    try:
        module = importlib.import_module(import_path)
        app.include_router(module.router, prefix="/api")
        logger.info(f"✅ Router geladen: {name or import_path}")
    except Exception as e:
        logger.warning(f"❌ Kon router '{name or import_path}' niet laden: {e}")

# ✅ Juiste prefix: backend.api.*
router_paths = [
    ("backend.api.market_data_api", "market_data_api"),
    ("backend.api.macro_data_api", "macro_data_api"),
    ("backend.api.technical_data_api", "technical_data_api"),
    ("backend.api.setups_api", "setups_api"),
    ("backend.api.dashboard_api", "dashboard_api"),
    ("backend.api.report_api", "report_api"),
    ("backend.api.sidebar_api", "sidebar_api"),
    ("backend.api.onboarding_api", "onboarding_api"),
    ("backend.api.score_api", "score_api"),
    ("backend.api.strategy_api", "strategy_api"),
    ("backend.api.ai.ai_explain_api", "ai_explain_api"),
    ("backend.api.ai.ai_strategy_api", "ai_strategy_api"),
    ("backend.api.ai.ai_trading_api", "ai_trading_api"),
    ("backend.api.ai.validate_setups_api", "validate_setups_api"),
    ("backend.api.ai.ai_score_generator", "ai_score_generator"),
    ("backend.api.ai.ai_setup_validator", "ai_setup_validator"),
    ("backend.api.ai.ai_daily_report_generator", "ai_daily_report_generator"),
]

for path, name in router_paths:
    safe_include(path, name)

# ✅ Healthcheck
@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "API is running"}

# ✅ CORS test
@app.get("/api/test-cors")
def test_cors():
    return {"success": True, "message": "CORS werkt correct vanaf frontend."}

# ✅ Start server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("start_backend:app", host="0.0.0.0", port=5002, reload=True)

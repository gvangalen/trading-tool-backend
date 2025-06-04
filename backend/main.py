from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ FastAPI app initialiseren
app = FastAPI(title="Market Dashboard API", version="1.0")

# ✅ Alleen toegang vanaf frontend + lokaal
origins = [
    "http://localhost:3000",          # lokaal
    "http://143.47.186.148",          # Oracle server zonder poort
    "http://143.47.186.148:80",       # HTTP standaardpoort
    "http://143.47.186.148:3000",     # Frontend draait op poort 3000
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Routers includen zonder extra prefix (verwacht dat routers zelf prefix="/api" hebben)
def safe_include(import_path, router_name):
    try:
        module = __import__(import_path, fromlist=["router"])
        app.include_router(module.router)
        logger.info(f"✅ Router geladen: {router_name}")
    except Exception as e:
        logger.warning(f"❌ Kon router '{router_name}' niet laden ({import_path}): {e}")

# ✅ Laad alle routers
safe_include("api.market_data_api", "market_data_api")
safe_include("api.macro_data_api", "macro_data_api")
safe_include("api.technical_data_api", "technical_data_api")
safe_include("api.setups_api", "setups_api")
safe_include("api.dashboard_api", "dashboard_api")
safe_include("api.report_api", "report_api")
safe_include("api.ai.ai_explain_api", "ai_explain_api")
safe_include("api.ai.ai_strategy_api", "ai_strategy_api")
safe_include("api.onboarding_api", "onboarding_api")

# ✅ Healthcheck
@app.get("/api/health")
def health_check():
    logger.info("📡 Health check aangeroepen.")
    return {"status": "ok", "message": "API is running"}

# ✅ CORS-test
@app.get("/api/test-cors")
def test_cors():
    logger.info("🧪 CORS test endpoint aangeroepen.")
    return {"success": True, "message": "CORS werkt correct vanaf frontend."}

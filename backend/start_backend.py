import sys, os
import logging
import traceback
import importlib
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ✅ .env laden
load_dotenv()

# ✅ Padinstellingen voor module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ✅ Logging instellen
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("start_backend")

# ✅ FastAPI-instantie
app = FastAPI(title="Market Dashboard API", version="1.0")

# ✅ CORS-configuratie
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

# ✅ Helper voor veilig importeren van routers
def safe_include(import_path, name=""):
    try:
        module = importlib.import_module(import_path)
        app.include_router(module.router, prefix="/api")
        logger.info(f"✅ Router geladen: {name or import_path}")
        print(f"🧪 Router geladen: {name or import_path}")
    except Exception as e:
        logger.warning(f"❌ Kon router '{name or import_path}' niet laden: {e}")
        print(f"❌ Router FOUT: {name or import_path} — {e}")
        traceback.print_exc()

# ✅ Standaard API-routers
safe_include("backend.api.market_data_api", "market_data_api")
safe_include("backend.api.macro_data_api", "macro_data_api")
safe_include("backend.api.technical_data_api", "technical_data_api")
safe_include("backend.api.setups_api", "setups_api")
safe_include("backend.api.dashboard_api", "dashboard_api")
safe_include("backend.api.report_api", "report_api")
safe_include("backend.api.sidebar_api", "sidebar_api")
safe_include("backend.api.onboarding_api", "onboarding_api")
safe_include("backend.api.score_api", "score_api")
safe_include("backend.api.strategy_api", "strategy_api")

# ✅ AI API-routers
safe_include("backend.api.ai.ai_explain_api", "ai_explain_api")
safe_include("backend.api.ai.ai_strategy_api", "ai_strategy_api")
safe_include("backend.api.ai.ai_trading_api", "ai_trading_api")
safe_include("backend.api.ai.validate_setups_api", "validate_setups_api")
safe_include("backend.api.ai.ai_daily_report_generator", "ai_daily_report_generator")
safe_include("backend.api.ai.ai_status_api", "ai_status_api")

# ✅ Extra routers
safe_include("backend.routes.trades_routes", "trades_routes")
safe_include("backend.routes.report_routes", "report_routes")

# ✅ Debug: print alle routes
print("\n🚦 Alle geregistreerde routes en HTTP-methodes:")
for route in app.routes:
    print(f"{route.path} - methods: {route.methods}")
print()

# ✅ Healthcheck endpoint
@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "API draait ✅"}

# ✅ Lokale run via Python direct: start op poort 5002
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("start_backend:app", host="0.0.0.0", port=5002, reload=True)

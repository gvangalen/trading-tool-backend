import sys
import os
import logging
import importlib
import traceback
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.routing import APIRoute
from dotenv import load_dotenv

# ------------------------------------------------------------
# 📌 .env laden
# ------------------------------------------------------------
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

# ------------------------------------------------------------
# 📌 Root path toevoegen
# ------------------------------------------------------------
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ------------------------------------------------------------
# 📌 Logging
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# 🚀 FastAPI app
# ------------------------------------------------------------
app = FastAPI(title="Market Dashboard API", version="1.0")

# ------------------------------------------------------------
# 🌍 CORS — volledig correct voor COOKIE-BASED AUTH
# ------------------------------------------------------------
# ❗ allow_origins mag NIET "*" zijn bij cookies.
# ❗ allow_origin_regex mag NIET meer gebruikt worden (breekt cookies)
# ❗ credentials=True verplicht dat origins EXACT overeenkomen.

allow_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",

    # FRONTEND PROD (poort 80)
    "http://143.47.186.148",
    "https://143.47.186.148",

    # eventueel: als frontend op andere poorten draait
    "http://143.47.186.148:3000",
    "https://143.47.186.148:3000",

    # backend zelf (nodig voor cookies bij local testing)
    "http://143.47.186.148:5002",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,        # ❗ GEEN "*" en GEEN regex
    allow_credentials=True,            # ⭐ cookies toestaan
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# 📂 Static files
# ------------------------------------------------------------
app.mount("/static", StaticFiles(directory="backend/static"), name="static")


# ==================================================================
# 🔧 Helper: veilig routers includen
# ==================================================================
def safe_include(import_path, name=""):
    """
    Laadt een router dynamisch en prefix't deze automatisch met /api.
    Safe loading: breekt de API niet als 1 module failt.
    """
    try:
        module = importlib.import_module(import_path)
        app.include_router(module.router, prefix="/api")
        logger.info(f"✅ Router geladen: {name or import_path}")
    except Exception as e:
        logger.error(f"❌ Router FOUT bij laden van {name or import_path}: {e}")
        traceback.print_exc()


# ==================================================================
# 🔐 AUTH MOET ALTIJD EERST
# ==================================================================
safe_include("backend.api.auth_api", "auth_api")

# ==================================================================
# 🎯 ONBOARDING (na auth)
# ==================================================================
safe_include("backend.api.onboarding_api", "onboarding_api")

# ==================================================================
# 📦 Andere API's
# ==================================================================
safe_include("backend.api.market_data_api", "market_data_api")
safe_include("backend.api.macro_data_api", "macro_data_api")
safe_include("backend.api.technical_data_api", "technical_data_api")
safe_include("backend.api.setups_api", "setups_api")
safe_include("backend.api.strategy_api", "strategy_api")
safe_include("backend.api.score_api", "score_api")
safe_include("backend.api.dashboard_api", "dashboard_api")
safe_include("backend.api.sidebar_api", "sidebar_api")
safe_include("backend.api.agents_api", "agents_api")

# ==================================================================
# 🗂 Legacy routes
# ==================================================================
safe_include("backend.routes.trades_routes", "trades_routes")
safe_include("backend.routes.report_routes", "report_routes")

# ==================================================================
# 👨‍⚕️ Health check
# ==================================================================
@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "API is running"}

# ==================================================================
# 🧭 Debug: lijst ALLE routes
# ==================================================================
print("\n🚦 Alle geregistreerde routes en HTTP-methodes:")
for route in app.routes:
    if isinstance(route, APIRoute):
        print(f"{route.path} - methods: {route.methods}")
    else:
        print(f"{route.path} - <non-API route>")
print()

print("🔍 ASSETS_JSON uit .env:", os.getenv("ASSETS_JSON"))

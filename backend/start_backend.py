import sys, os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ✅ Zorg dat relative imports werken
sys.path.insert(0, os.path.abspath("."))

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Alle routers importeren (zonder /api prefix)
from api.market_data_api import router as market_data_router
from api.macro_data_api import router as macro_data_router
from api.technical_data_api import router as technical_data_router
from api.setups_api import router as setups_router
from api.dashboard_api import router as dashboard_router
from api.report_api import router as report_router
from api.ai.ai_explain_api import router as ai_explain_router
from api.ai.ai_strategy_api import router as ai_strategy_router
from api.ai.ai_trading_api import router as ai_trading_router
from api.onboarding_api import router as onboarding_router

# ✅ App aanmaken
app = FastAPI()

# ✅ CORS toestaan (voor frontend verbindingen)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Routers koppelen — zonder /api prefix
app.include_router(market_data_router)
app.include_router(macro_data_router)
app.include_router(technical_data_router)
app.include_router(setups_router)
app.include_router(dashboard_router)
app.include_router(report_router)
app.include_router(ai_explain_router)
app.include_router(ai_strategy_router)
app.include_router(ai_trading_router)
app.include_router(onboarding_router)

# ✅ Healthcheck route
@app.get("/health")
def health():
    return {"status": "ok"}

# ✅ Server starten
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5002)

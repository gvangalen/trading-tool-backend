# start_backend.py
import sys, os
sys.path.insert(0, os.path.abspath("."))  # Voegt ./backend toe aan sys.path

# ✅ Import alle routers
from api.market_data_api import router as market_data_router
from api.macro_data_api import router as macro_data_router
from api.technical_data_api import router as technical_data_router
from api.setups_api import router as setups_router
from api.dashboard_api import router as dashboard_router
from api.report_api import router as report_router
from api.ai.ai_explain_api import router as ai_explain_router
from api.ai.ai_strategy_api import router as ai_strategy_router
from api.onboarding_api import router as onboarding_router

# ✅ Import FastAPI app
from main import app

# ✅ Voeg routers toe aan de app
app.include_router(market_data_router, prefix="/api")
app.include_router(macro_data_router, prefix="/api")
app.include_router(technical_data_router, prefix="/api")
app.include_router(setups_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(report_router, prefix="/api")
app.include_router(ai_explain_router, prefix="/api")
app.include_router(ai_strategy_router, prefix="/api")
app.include_router(onboarding_router, prefix="/api")

# ✅ Start de API
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5002)

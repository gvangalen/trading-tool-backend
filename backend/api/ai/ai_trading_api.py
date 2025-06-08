# ✅ ai_trading_api.py

import logging
from fastapi import APIRouter, HTTPException
from utils.db import get_db_connection
import psycopg2.extras

router = APIRouter(prefix="/ai/trading")
logger = logging.getLogger(__name__)

# ✅ Tradingadvies ophalen per asset
@router.get("/trading_advice")
async def get_trading_advice(symbol: str = "BTC"):
    symbol = symbol.upper()
    conn = get_db_connection()
    if not conn:
        logger.error("❌ DB01: Geen databaseverbinding.")
        return {
            "symbol": symbol,
            "advice": "⚠️ Geen verbinding met database.",
            "score": 0,
            "setup": None,
            "targets": [],
            "risk_profile": None
        }

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT asset, advice, explanation, risk_profile, created_at
                FROM trading_advice
                WHERE asset = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()

            if not row:
                logger.warning(f"⚠️ Geen advies gevonden voor {symbol}")
                return {
                    "symbol": symbol,
                    "advice": f"⚠️ Geen advies beschikbaar voor {symbol}",
                    "score": 0,
                    "setup": None,
                    "targets": [],
                    "risk_profile": None
                }

            return {
                "symbol": row["asset"],
                "advice": row["advice"],
                "explanation": row["explanation"],
                "risk_profile": row["risk_profile"],
                "timestamp": row["created_at"].isoformat(),
                "score": 100,               # 👉 optioneel, kan dynamisch
                "setup": "A-Plus Setup",    # 👉 optioneel, kan via extra join of kolom
                "targets": [                # 👉 optioneel, hardcoded of los ophalen
                    {"price": 69000, "type": "TP1"},
                    {"price": 72000, "type": "TP2"}
                ]
            }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen tradingadvies voor {symbol}: {e}")
        return {
            "symbol": symbol,
            "advice": "❌ Interne fout bij ophalen advies.",
            "score": 0,
            "setup": None,
            "targets": [],
            "risk_profile": None
        }
    finally:
        conn.close()

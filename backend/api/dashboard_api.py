# dashboard_api.py
from fastapi import APIRouter, HTTPException
from db import get_db_connection
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/api/dashboard_data")
async def get_dashboard_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Databaseverbinding mislukt")

    try:
        with conn.cursor() as cur:
            # ✅ Market data ophalen
            cur.execute("""
                SELECT symbol, price, volume, change_24h, timestamp
                FROM market_data
                WHERE symbol IN ('BTC', 'SOL')
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            market_rows = cur.fetchall()
            market_data = []
            seen_market = set()
            for row in market_rows:
                symbol = row[0]
                if symbol not in seen_market:
                    seen_market.add(symbol)
                    market_data.append({
                        "symbol": symbol,
                        "price": float(row[1]),
                        "volume": float(row[2]),
                        "change_24h": float(row[3]),
                        "timestamp": row[4].isoformat() if row[4] else None
                    })

            # ✅ Technical data ophalen
            cur.execute("""
                SELECT symbol, rsi, volume, ma_200, timestamp
                FROM technical_data
                WHERE symbol IN ('BTC', 'SOL')
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            tech_rows = cur.fetchall()
            technical_data = []
            seen_tech = set()
            for row in tech_rows:
                symbol = row[0]
                if symbol not in seen_tech:
                    seen_tech.add(symbol)
                    technical_data.append({
                        "symbol": symbol,
                        "rsi": float(row[1]) if row[1] is not None else None,
                        "volume": float(row[2]) if row[2] is not None else None,
                        "ma_200": float(row[3]) if row[3] is not None else None,
                        "timestamp": row[4].isoformat() if row[4] else None
                    })

            # ✅ Macro data ophalen
            cur.execute("""
                SELECT name, value, trend, interpretation, action, timestamp
                FROM macro_data
                ORDER BY timestamp DESC
                LIMIT 100
            """)
            macro_rows = cur.fetchall()
            macro_data = []
            seen_macro = set()
            for row in macro_rows:
                name = row[0]
                if name not in seen_macro:
                    seen_macro.add(name)
                    macro_data.append({
                        "name": name,
                        "value": float(row[1]) if row[1] is not None else None,
                        "trend": row[2],
                        "interpretation": row[3],
                        "action": row[4],
                        "timestamp": row[5].isoformat() if row[5] else None
                    })

            # ✅ Setups ophalen
            cur.execute("""
                SELECT name, status, timestamp
                FROM setups
                ORDER BY timestamp DESC
                LIMIT 50
            """)
            setup_rows = cur.fetchall()
            setups = []
            seen_setups = set()
            for row in setup_rows:
                name = row[0]
                if name not in seen_setups:
                    seen_setups.add(name)
                    setups.append({
                        "name": name,
                        "status": row[1],
                        "timestamp": row[2].isoformat() if row[2] else None
                    })

        return {
            "market_data": market_data,
            "technical_data": technical_data,
            "macro_data": macro_data,
            "setups": setups
        }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen dashboard_data: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen dashboard_data")
    finally:
        conn.close()

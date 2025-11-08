import logging
from fastapi import APIRouter, HTTPException
from backend.utils.db import get_db_connection
import psycopg2.extras
from backend.utils.scoring_utils import get_scores_for_symbol  # ✅ Nieuwe import

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@router.get("/dashboard")
async def get_dashboard_data():
    """
    Dashboard-endpoint dat macro-, technische en marktdata combineert met setups en totale scores.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DASH00: Databaseverbinding mislukt.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ✅ Market data (laatste BTC snapshot)
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol) symbol, price, volume, change_24h, timestamp
                    FROM market_data
                    WHERE symbol = 'BTC'
                    ORDER BY symbol, timestamp DESC
                """)
                market_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"📈 DASH01: Market data geladen ({len(market_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH01: Market data fout: {e}")
                market_data = []

            # ✅ Technical data (laatste waarden)
            try:
                cur.execute("""
                    SELECT symbol, LOWER(indicator) AS indicator, value, score, timestamp
                    FROM technical_indicators
                    WHERE symbol = 'BTC'
                    ORDER BY indicator, timestamp DESC
                """)
                rows = cur.fetchall()
                technical_data = {
                    row["indicator"]: {
                        "value": row["value"],
                        "score": row["score"],
                        "timestamp": row["timestamp"]
                    }
                    for row in rows
                }
                logger.info(f"🧪 DASH02: Technical data geladen ({len(technical_data)} indicatoren)")
            except Exception as e:
                logger.warning(f"⚠️ DASH02: Technical data fout: {e}")
                technical_data = {}

            # ✅ Macro data (laatste per naam)
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    ORDER BY name, timestamp DESC
                """)
                macro_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"🌍 DASH03: Macro data geladen ({len(macro_data)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH03: Macro data fout: {e}")
                macro_data = []

            # ✅ Setup-status (laatste per setup)
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name) name, created_at AS timestamp
                    FROM setups
                    ORDER BY name, created_at DESC
                """)
                setups = [dict(row) for row in cur.fetchall()]
                logger.info(f"📋 DASH04: Setups geladen ({len(setups)} rijen)")
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups fout: {e}")
                setups = []

        # ✅ Nieuwe manier: haal gecombineerde scores uit scoring_utils
        scores = get_scores_for_symbol(include_metadata=True)
        macro_score = scores.get("macro_score", 0)
        technical_score = scores.get("technical_score", 0)
        market_score = scores.get("market_score", 0)
        setup_score = scores.get("setup_score", 0)

        # ✅ Uitleg per categorie
        macro_explanation = (
            "📊 Gebaseerd op " + ", ".join(d["name"] for d in macro_data)
            if macro_data else "❌ Geen macrodata"
        )

        if technical_data:
            lines = [
                f"{k.upper()}: {v.get('value')} (score {v.get('score')})"
                for k, v in technical_data.items()
            ]
            technical_explanation = "📈 " + " • ".join(lines)
        else:
            technical_explanation = "❌ Geen technische data"

        setup_explanation = (
            f"🧠 {len(setups)} setups geladen"
            if setups else "❌ Geen setups actief"
        )

        return {
            "market_data": market_data,
            "technical_data": technical_data,
            "macro_data": macro_data,
            "setups": setups,
            "scores": {
                "macro": macro_score,
                "technical": technical_score,
                "market": market_score,
                "setup": setup_score
            },
            "explanation": {
                "macro": macro_explanation,
                "technical": technical_explanation,
                "setup": setup_explanation
            }
        }

    except Exception as e:
        logger.error(f"❌ DASH05: Dashboard error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="DASH05: Dashboard data ophalen mislukt.")
    finally:
        conn.close()


@router.get("/dashboard/health")
async def health_check():
    """Simpele healthcheck voor dashboard."""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="HEALTH01: DB-connectie faalt.")
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ HEALTH02: {e}")
        raise HTTPException(status_code=500, detail="HEALTH02: Interne fout")


@router.get("/dashboard/trading_advice")
async def get_trading_advice(symbol: str = "BTC"):
    """Laatste tradingadvies ophalen uit de trading_advice-tabel."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, advice, explanation, timestamp
                FROM trading_advice
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen advies voor {symbol}.")
            return dict(row)
    except Exception as e:
        logger.error(f"❌ ADVICE01: {e}")
        raise HTTPException(status_code=500, detail="ADVICE01: Ophalen advies mislukt.")
    finally:
        conn.close()


@router.get("/dashboard/top_setups")
async def get_top_setups():
    """Top 5 strategieën op basis van hoogste score."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, score, timeframe, symbol, explanation, timestamp
                FROM strategies
                WHERE data->>'score' IS NOT NULL
                ORDER BY CAST(data->>'score' AS FLOAT) DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"❌ SETUPS01: {e}")
        raise HTTPException(status_code=500, detail="SETUPS01: Ophalen top setups mislukt.")
    finally:
        conn.close()


@router.get("/dashboard/setup_summary")
async def get_setup_summary():
    """Beknopte lijst van de laatste setups per naam."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (name) name, created_at AS timestamp
                FROM setups
                ORDER BY name, created_at DESC
            """)
            rows = cur.fetchall()
            return [{"name": row["name"], "timestamp": row["timestamp"].isoformat()} for row in rows]
    except Exception as e:
        logger.warning(f"⚠️ DASH06: Fout bij ophalen setup summary: {e}")
        return []
    finally:
        conn.close()

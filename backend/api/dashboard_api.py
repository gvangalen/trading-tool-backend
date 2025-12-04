import logging
from fastapi import APIRouter, HTTPException, Depends
import psycopg2.extras

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user       # 🔐 USER SUPPORT
from backend.utils.scoring_utils import get_scores_for_symbol

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =========================================================
# 🔥 DASHBOARD DATA (USER-SPECIFIEK)
# =========================================================
@router.get("/dashboard")
async def get_dashboard_data(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DASH00: Databaseverbinding mislukt.")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # =====================================================
            # 📈 MARKET DATA (laatste snapshot per user)
            # =====================================================
            try:
                cur.execute("""
                    SELECT DISTINCT ON (symbol)
                        symbol, price, volume, change_24h, timestamp
                    FROM market_data
                    WHERE user_id = %s AND symbol = 'BTC'
                    ORDER BY symbol, timestamp DESC
                """, (user_id,))
                market_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"📈 DASH01: Market data voor user {user_id} geladen")
            except Exception as e:
                logger.warning(f"⚠️ DASH01: Market data fout: {e}")
                market_data = []

            # =====================================================
            # 🧪 TECHNICAL DATA (laatste indicator-waardes)
            # =====================================================
            try:
                cur.execute("""
                    SELECT symbol, LOWER(indicator) AS indicator, value, score, timestamp
                    FROM technical_indicators
                    WHERE user_id = %s
                    ORDER BY indicator, timestamp DESC
                """, (user_id,))
                rows = cur.fetchall()

                technical_data = {
                    row["indicator"]: {
                        "value": row["value"],
                        "score": row["score"],
                        "timestamp": row["timestamp"],
                    }
                    for row in rows
                }
                logger.info(f"🧮 DASH02: Technical data geladen ({len(technical_data)} items)")
            except Exception as e:
                logger.warning(f"⚠️ DASH02: Technical data fout: {e}")
                technical_data = {}

            # =====================================================
            # 🌍 MACRO DATA (laatste per naam)
            # =====================================================
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name)
                        name, value, trend, interpretation, action, score, timestamp
                    FROM macro_data
                    WHERE user_id = %s
                    ORDER BY name, timestamp DESC
                """, (user_id,))
                macro_data = [dict(row) for row in cur.fetchall()]
                logger.info(f"🌍 DASH03: Macro data geladen")
            except Exception as e:
                logger.warning(f"⚠️ DASH03: Macro data fout: {e}")
                macro_data = []

            # =====================================================
            # 🧾 SETUPS (laatste per naam)
            # =====================================================
            try:
                cur.execute("""
                    SELECT DISTINCT ON (name)
                        name, created_at AS timestamp
                    FROM setups
                    WHERE user_id = %s
                    ORDER BY name, created_at DESC
                """, (user_id,))
                setups = [dict(row) for row in cur.fetchall()]
            except Exception as e:
                logger.warning(f"⚠️ DASH04: Setups fout: {e}")
                setups = []

        # =====================================================
        # 🧠 SCORES — user-specifiek
        # =====================================================
        try:
            scores = get_scores_for_symbol(user_id=user_id, include_metadata=True)
        except TypeError:
            scores = get_scores_for_symbol(include_metadata=True)

        macro_score = scores.get("macro_score", 0)
        technical_score = scores.get("technical_score", 0)
        market_score = scores.get("market_score", 0)
        setup_score = scores.get("setup_score", 0)

        # =====================================================
        # 📘 UITLEGGEN
        # =====================================================
        macro_explanation = (
            "📊 Gebaseerd op: " + ", ".join(d["name"] for d in macro_data)
            if macro_data else "❌ Geen macrodata"
        )

        if technical_data:
            technical_explanation = " | ".join(
                f"{k.upper()}: {v['value']} (score {v['score']})"
                for k, v in technical_data.items()
            )
        else:
            technical_explanation = "❌ Geen technische data"

        setup_explanation = (
            f"🧠 {len(setups)} actieve setups" if setups else "❌ Geen setups"
        )

        # =====================================================
        # 🎯 RESPONSE
        # =====================================================
        return {
            "user_id": user_id,
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
        logger.error(f"❌ DASH05: Dashboard error — {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="DASH05: Dashboard data ophalen mislukt.")
    finally:
        conn.close()


# =========================================================
# ❤️ HEALTH CHECK
# =========================================================
@router.get("/dashboard/health")
async def health_check():
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="HEALTH01: DB-connectie faalt.")
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ HEALTH02: {e}")
        raise HTTPException(status_code=500, detail="HEALTH02: Interne fout")


# =========================================================
# 🧠 TRADING ADVICE (user-specifiek)
# =========================================================
@router.get("/dashboard/trading_advice")
async def get_trading_advice(
    symbol: str = "BTC",
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, advice, explanation, timestamp
                FROM trading_advice
                WHERE symbol = %s AND user_id = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol, user_id))

            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Geen advies voor {symbol}.")
            return dict(row)
    finally:
        conn.close()


# =========================================================
# ⭐ TOP SETUPS (user-specifiek)
# =========================================================
@router.get("/dashboard/top_setups")
async def get_top_setups(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, score, timeframe, symbol, explanation, timestamp
                FROM strategies
                WHERE user_id = %s AND data->>'score' IS NOT NULL
                ORDER BY CAST(data->>'score' AS FLOAT) DESC
                LIMIT 5
            """, (user_id,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


# =========================================================
# 📝 SETUP SUMMARY (user-specifiek)
# =========================================================
@router.get("/dashboard/setup_summary")
async def get_setup_summary(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (name)
                    name, created_at AS timestamp
                FROM setups
                WHERE user_id = %s
                ORDER BY name, created_at DESC
            """, (user_id,))
            rows = cur.fetchall()
            return [
                {"name": row["name"], "timestamp": row["timestamp"].isoformat()}
                for row in rows
            ]
    finally:
        conn.close()

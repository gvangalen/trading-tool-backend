import logging
import traceback
from collections import defaultdict
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Depends
import httpx

from backend.utils.db import get_db_connection
from backend.utils.scoring_utils import (
    get_scores_for_symbol,
    get_score_rule_from_db,   # 🔥 gebruikt voor market-indicator scoring
)
from backend.utils.auth_utils import get_current_user

# ⭐ Onboarding helper importeren
from backend.api.onboarding_api import mark_step_completed  

# =========================================================
# ⚙️ Router setup
# =========================================================
router = APIRouter()
logger = logging.getLogger(__name__)
logger.info(
    "🚀 market_data_api.py geladen – globale market-data + user-based market_data_indicators + onboarding bij POST /market_data/indicator."
)


# =========================================================
# 🔄 Dynamisch laden van API endpoints uit database (RAW)
# =========================================================
def get_market_raw_endpoints():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name, link
                FROM indicators
                WHERE category = 'market_raw'
            """
            )
            result = {row[0]: row[1] for row in cur.fetchall()}

        conn.close()
        logger.info(f"✅ Market RAW endpoints geladen: {list(result.keys())}")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen market_raw endpoints: {e}")
        return {}


# Globale cache
MARKET_RAW_ENDPOINTS = get_market_raw_endpoints()


# =========================================================
# 🧩 Helper — laatste globale BTC snapshot (price/change/volume)
# =========================================================
def _get_latest_global_btc_snapshot(cur):
    """
    Haalt de laatste BTC rij op uit market_data (globale tabel).
    Verwacht dat conn/cursor al open is.
    """
    cur.execute(
        """
        SELECT price, change_24h, volume, timestamp
        FROM market_data
        WHERE symbol = 'BTC'
        ORDER BY timestamp DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Geen globale BTC market_data gevonden.")

    price, change_24h, volume, ts = row
    return {
        "price": float(price) if price is not None else None,
        "change_24h": float(change_24h) if change_24h is not None else None,
        "volume": float(volume) if volume is not None else None,
        "timestamp": ts,
    }


# =========================================================
# 🆕 POST /market_data/indicator — user-indicator opslaan
#    → gebruikt market_data_indicators (met user_id)
#    → triggert onboarding stap 'market'
# =========================================================
@router.post("/market_data/indicator")
def add_user_market_indicator(
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]

    raw_name = payload.get("indicator") or payload.get("name")
    if not raw_name:
        raise HTTPException(400, "❌ 'indicator' (of 'name') is verplicht.")

    indicator = raw_name.strip()
    if not indicator:
        raise HTTPException(400, "❌ Indicator mag niet leeg zijn.")

    value = payload.get("value")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        cur = conn.cursor()

        # =====================================================
        # 🔒 DUPLICATE CHECK (BELANGRIJK)
        # =====================================================
        cur.execute(
            """
            SELECT 1
            FROM market_data_indicators
            WHERE name = %s AND user_id = %s
            LIMIT 1
            """,
            (indicator, user_id),
        )

        if cur.fetchone():
            raise HTTPException(
                status_code=409,
                detail=f"Indicator '{indicator}' is al toegevoegd."
            )

        # =====================================================
        # 📈 VALUE BEPALEN
        # =====================================================
        if value is None:
            snapshot = _get_latest_global_btc_snapshot(cur)
            lname = indicator.lower()

            if "price" in lname:
                value = snapshot["price"]
            elif "change" in lname:
                value = snapshot["change_24h"]
            elif "volume" in lname:
                value = snapshot["volume"]
            else:
                raise HTTPException(
                    400,
                    "❌ Geen 'value' meegegeven en indicator kan niet automatisch "
                    "worden gemapt op price/change_24h/volume.",
                )

        try:
            value = float(value)
        except Exception:
            raise HTTPException(400, "❌ 'value' moet numeriek zijn.")

        # =====================================================
        # 🧮 SCORE LOGICA
        # =====================================================
        rule = get_score_rule_from_db("market", indicator, value)
        if rule:
            score = rule.get("score")
            trend = rule.get("trend")
            interpretation = rule.get("interpretation")
            action = rule.get("action")
        else:
            score = None
            trend = None
            interpretation = "Geen scoreregel gevonden voor deze waarde."
            action = None

        # =====================================================
        # 💾 OPSLAAN
        # =====================================================
        cur.execute(
            """
            INSERT INTO market_data_indicators
                (name, value, trend, interpretation, action, score, user_id, timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())
            RETURNING id, timestamp;
            """,
            (indicator, value, trend, interpretation, action, score, user_id),
        )
        new_id, ts = cur.fetchone()
        conn.commit()

        # ⭐ Onboarding stap afronden
        mark_step_completed(conn, user_id, "market")

        return {
            "id": new_id,
            "name": indicator,
            "value": value,
            "score": score,
            "trend": trend,
            "interpretation": interpretation,
            "action": action,
            "timestamp": ts.isoformat() if ts else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [market_data/indicator] {e}", exc_info=True)
        raise HTTPException(500, "Fout bij opslaan market-indicator.")
    finally:
        conn.close()

# =========================================================
# GET /market_data/indicators — alle user-indicatoren (history)
# =========================================================
@router.get("/market_data/indicators")
def list_user_market_indicators(
    limit: int = Query(200, ge=1, le=1000),
    current_user: dict = Depends(get_current_user),
):
    """
    Haalt alle market_data_indicators op voor de huidige gebruiker.
    Handig voor een uitgebreide tabel (history).
    """
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, value, trend, interpretation, action, score, timestamp
            FROM market_data_indicators
            WHERE user_id = %s
            ORDER BY timestamp DESC
            LIMIT %s;
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()

        return [
            {
                "id": r[0],
                "name": r[1],
                "value": float(r[2]) if r[2] is not None else None,
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
                "score": float(r[6]) if r[6] is not None else None,
                "timestamp": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [market_data/indicators] {e}", exc_info=True)
        raise HTTPException(500, "Fout bij ophalen market-indicatoren.")
    finally:
        conn.close()

# =========================================================
# DELETE /market_data/indicator/{indicator_name}
# =========================================================
@router.delete("/market_data/indicator/{name}")
def delete_market_indicator(
    name: str,
    current_user: dict = Depends(get_current_user),
):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            # check bestaat
            cur.execute(
                """
                SELECT COUNT(*)
                FROM market_data_indicators
                WHERE name = %s AND user_id = %s
                """,
                (name, current_user["id"]),
            )
            count = cur.fetchone()[0]

            if count == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"Indicator '{name}' niet gevonden voor deze gebruiker."
                )

            # delete
            cur.execute(
                """
                DELETE FROM market_data_indicators
                WHERE name = %s AND user_id = %s
                """,
                (name, current_user["id"]),
            )
            conn.commit()

        return {
            "message": f"Indicator '{name}' verwijderd.",
            "rows_deleted": count,
        }

    finally:
        conn.close()


# =========================================================
# 📅 GET /market_data/day — DAGTABEL (user-based)
# =========================================================
@router.get("/market_data/day")
def get_market_day_data(current_user: dict = Depends(get_current_user)):
    """
    ACTIEVE market-indicatoren (1 per naam, nieuwste).
    """
    user_id = current_user["id"]

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "❌ Geen databaseverbinding.")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (name)
                    name, value, trend, interpretation, action, score, timestamp
                FROM market_data_indicators
                WHERE user_id = %s
                ORDER BY name, timestamp DESC;
                """,
                (user_id,),
            )
            rows = cur.fetchall()

        return [
            {
                "name": r[0],
                "value": float(r[1]) if r[1] is not None else None,
                "trend": r[2],
                "interpretation": r[3],
                "action": r[4],
                "score": float(r[5]) if r[5] is not None else None,
                "timestamp": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ]

    finally:
        conn.close()

# =========================================================
# 📌 GET /market/indicator_names — lijst beschikbare indicators
# (globale config, niet per user)
# =========================================================
@router.get("/market/indicator_names")
def get_market_indicator_names():
    """
    Geeft alle MARKET-indicators terug die:
    - in de tabel 'indicators' staan (category = 'market')
    - én een entry hebben in 'market_indicator_rules' (scoreregels)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT i.name, i.display_name
            FROM indicators i
            JOIN market_indicator_rules r
                ON r.indicator = i.name
            WHERE i.category = 'market'
            GROUP BY i.name, i.display_name
            ORDER BY i.display_name ASC;
        """
        )
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "name": r[0],  # bv. 'btc_change_24h'
                "display_name": r[1],  # bv. 'BTC Change 24h'
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [indicator_names] {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# 📌 GET /market/indicator_rules/{name}
# (globale scoreregels)
# =========================================================
@router.get("/market/indicator_rules/{name}")
def get_market_indicator_rules(name: str):
    """
    Haalt alle scoreregels op voor één market-indicator
    vanuit 'market_indicator_rules'.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT range_min, range_max, score, trend, interpretation, action
            FROM market_indicator_rules
            WHERE indicator = %s
            ORDER BY range_min ASC;
        """,
            (name,),
        )
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "range_min": r[0],
                "range_max": r[1],
                "score": r[2],
                "trend": r[3],
                "interpretation": r[4],
                "action": r[5],
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [indicator_rules] {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# GET /market_data/list — ruwe BTC data (GLOBAAL)
# =========================================================
@router.get("/market_data/list")
async def list_market_data(
    since_minutes: int = Query(default=1440),
):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)
        cur.execute(
            """
            SELECT id, symbol, price, open, high, low, change_24h, volume, timestamp
            FROM market_data
            WHERE timestamp >= %s
            ORDER BY timestamp DESC
        """,
            (time_threshold,),
        )
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "id": r[0],
                "symbol": r[1],
                "price": r[2],
                "open": r[3],
                "high": r[4],
                "low": r[5],
                "change_24h": r[6],
                "volume": r[7],
                "timestamp": r[8],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"❌ [list] DB-fout: {e}")
        raise HTTPException(500, "❌ Kon marktdata niet ophalen.")


# =========================================================
# GET /market_data/btc/7d/fill — BTC 7d ophalen & opslaan
# (globale 7d tabel, geen user_id)
# =========================================================
@router.post("/market_data/btc/7d/fill")
async def fill_btc_7day_data():
    logger.info("📥 Handmatig ophalen BTC 7d market data gestart")
    conn = get_db_connection()
    if not conn:
        return {"error": "❌ Geen databaseverbinding"}

    try:
        coingecko_id = "bitcoin"
        url_ohlc = (
            f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/ohlc?vs_currency=usd&days=7"
        )
        url_volume = MARKET_RAW_ENDPOINTS.get(
            "btc_volume",
            f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=7",
        )

        with httpx.Client(timeout=10.0) as client:
            ohlc_data = client.get(url_ohlc).json()
            volume_data = client.get(url_volume).json().get("total_volumes", [])

        volume_by_date = {
            datetime.utcfromtimestamp(ts / 1000).date(): vol
            for ts, vol in volume_data
        }

        inserted = 0
        with conn.cursor() as cur:
            for entry in ohlc_data:
                ts, open_p, high_p, low_p, close_p = entry
                date = datetime.utcfromtimestamp(ts / 1000).date()
                change = round((close_p - open_p) / open_p * 100, 2)
                volume = volume_by_date.get(date)

                cur.execute(
                    "SELECT 1 FROM market_data_7d WHERE symbol = %s AND date = %s",
                    ("BTC", date),
                )
                if cur.fetchone():
                    continue

                cur.execute(
                    """
                    INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, volume, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                    ("BTC", date, open_p, high_p, low_p, close_p, change, volume),
                )
                inserted += 1

        conn.commit()
        return {"status": f"✅ Gegevens opgeslagen voor {inserted} dagen."}

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en opslaan BTC market data: {e}")
        return {"error": f"❌ {str(e)}"}

    finally:
        conn.close()


# =========================================================
# GET /market_data/btc/latest  (GLOBAAL)
# =========================================================
@router.get("/market_data/btc/latest")
def get_latest_btc_price():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, symbol, price, change_24h, volume, timestamp
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Geen BTC data gevonden")

        keys = ["id", "symbol", "price", "change_24h", "volume", "timestamp"]
        return dict(zip(keys, row))


# =========================================================
# GET /market_data/interpreted — GEEN ONBOARDING MEER
# =========================================================
@router.get("/market_data/interpreted")
async def fetch_interpreted_data(current_user: dict = Depends(get_current_user)):
    """
    User krijgt interpretatie op basis van scores, maar er wordt
    GEEN onboarding-stap meer gemarkeerd.
    """
    user_id = current_user["id"]

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, price, change_24h, volume, timestamp
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(404, "Geen BTC data gevonden.")

        symbol, price, change, volume, timestamp = row

        # USER SCORE combinatie ophalen (macro/technical/setup/market)
        scores = get_scores_for_symbol(user_id=user_id, include_metadata=True)

        return {
            "symbol": symbol,
            "timestamp": timestamp.isoformat(),
            "price": float(price),
            "change_24h": float(change),
            "volume": float(volume),
            "score": scores.get("market_score", 0),
            "top_contributors": scores.get("market_top_contributors", []),
            "interpretation": scores.get(
                "market_interpretation", "Geen interpretatie"
            ),
            "action": "Market-score is globaal, advies is informatief.",
        }

    except Exception as e:
        logger.error(f"❌ [interpreted] Fout: {e}")
        raise HTTPException(500, "❌ Interpretatiefout.")


# =========================================================
# GET /market_data/7d — laatste 7 dagen (globaal)
# =========================================================
@router.get("/market_data/7d")
async def get_market_data_7d():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, symbol, date, open, high, low, close, change, volume, created_at
                FROM market_data_7d
                WHERE symbol = 'BTC'
                ORDER BY date DESC
                LIMIT 7;
            """
            )
            rows = cur.fetchall()

        rows.reverse()  # van oud → nieuw
        return [
            {
                "id": r[0],
                "symbol": r[1],
                "date": r[2].isoformat(),
                "open": float(r[3]) if r[3] else None,
                "high": float(r[4]) if r[4] else None,
                "low": float(r[5]) if r[5] else None,
                "close": float(r[6]) if r[6] else None,
                "change": float(r[7]) if r[7] else None,
                "volume": float(r[8]) if r[8] else None,
                "created_at": r[9].isoformat() if r[9] else None,
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [7d] Fout bij ophalen market_data_7d: {e}")
        raise HTTPException(500, "Fout bij ophalen 7-daagse data.")

    finally:
        conn.close()


# =========================================================
# GET /market_data/forward — alle forward returns (globaal)
# =========================================================
@router.get("/market_data/forward")
async def get_market_forward_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, symbol, period, start_date, end_date, change, avg_daily, created_at
            FROM market_forward_returns
            WHERE symbol = 'BTC'
            ORDER BY period, start_date DESC
        """
        )
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "id": r[0],
                "symbol": r[1],
                "period": r[2],
                "start": r[3].isoformat(),
                "end": r[4].isoformat(),
                "change": float(r[5]) if r[5] else None,
                "avgDaily": float(r[6]) if r[6] else None,
                "created_at": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]

    except Exception as e:
        logger.error(f"❌ [forward] Fout bij ophalen returns: {e}")
        raise HTTPException(500, "Fout bij ophalen forward returns.")


# =========================================================
# GET /market_data/forward/week (globaal)
# =========================================================
@router.get("/market_data/forward/week")
def get_week_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '7d'
            ORDER BY start_date ASC
        """
        )
        rows = cur.fetchall()
        conn.close()

        data = defaultdict(lambda: [None] * 53)
        for start_date, change in rows:
            data[start_date.year][int(start_date.strftime("%U"))] = float(change)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Week returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen week returns.")


# =========================================================
# GET /market_data/forward/maand (globaal)
# =========================================================
@router.get("/market_data/forward/maand")
def get_month_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '30d'
            ORDER BY start_date ASC
        """
        )
        rows = cur.fetchall()
        conn.close()

        data = defaultdict(lambda: [None] * 12)
        for s, c in rows:
            data[s.year][s.month - 1] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Month returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen maand returns.")


# =========================================================
# GET /market_data/forward/kwartaal (globaal)
# =========================================================
@router.get("/market_data/forward/kwartaal")
def get_quarter_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '90d'
            ORDER BY start_date ASC
        """
        )
        rows = cur.fetchall()
        conn.close()

        data = defaultdict(lambda: [None] * 4)
        for s, c in rows:
            data[s.year][(s.month - 1) // 3] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Quarter returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen kwartaal returns.")


# =========================================================
# GET /market_data/forward/jaar (globaal)
# =========================================================
@router.get("/market_data/forward/jaar")
def get_year_returns():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_date, change
            FROM market_forward_returns
            WHERE symbol = 'BTC' AND period = '365d'
            ORDER BY start_date ASC
        """
        )
        rows = cur.fetchall()
        conn.close()

        data = defaultdict(lambda: [None])
        for s, c in rows:
            data[s.year][0] = float(c)

        return [{"year": y, "values": v} for y, v in sorted(data.items())]

    except Exception as e:
        logger.error(f"❌ Year returns error: {e}")
        raise HTTPException(500, "Fout bij ophalen jaar returns.")


# =========================================================
# POST /market_data/7d/save (globaal)
# =========================================================
@router.post("/market_data/7d/save")
async def save_market_data_7d(data: list[dict]):
    if not data:
        raise HTTPException(400, "❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for row in data:
            cur.execute(
                """
                INSERT INTO market_data_7d (symbol, date, open, high, low, close, change, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) DO NOTHING
            """,
                (
                    row["symbol"],
                    row["date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["change"],
                ),
            )

        conn.commit()
        conn.close()
        return {"status": "✅ 7d data opgeslagen."}

    except Exception as e:
        logger.error(f"❌ [7d/save] {e}")
        raise HTTPException(500, "Fout bij opslaan 7d data.")


# =========================================================
# POST /market_data/forward/save (globaal)
# =========================================================
@router.post("/market_data/forward/save")
async def save_forward_returns(data: list[dict]):
    if not data:
        raise HTTPException(400, "❌ Geen data ontvangen.")

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for row in data:
            cur.execute(
                """
                INSERT INTO market_forward_returns (symbol, period, start_date, end_date, change, avg_daily, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """,
                (
                    row["symbol"],
                    row["period"],
                    row["start_date"],
                    row["end_date"],
                    row["change"],
                    row["avg_daily"],
                ),
            )

        conn.commit()
        conn.close()
        return {"status": "✅ Forward returns opgeslagen."}

    except Exception as e:
        logger.error(f"❌ [forward/save] {e}")
        raise HTTPException(500, "Fout bij opslaan forward returns.")

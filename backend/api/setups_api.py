from fastapi import APIRouter, HTTPException, Request, Query, Path
from backend.utils.db import get_db_connection
from datetime import datetime
import logging
from backend.ai_agents.setup_ai_agent import generate_setup_explanation
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ✅ Helper om database-rows te formatteren naar JSON
def format_setup_rows(rows):
    return [
        {
            "id": row[0],
            "name": row[1],
            "symbol": row[2],
            "timeframe": row[3],
            "account_type": row[4],
            "strategy_type": row[5],
            "min_investment": row[6],
            "dynamic_investment": row[7],
            "tags": row[8],
            "trend": row[9],
            "score_logic": row[10],
            "favorite": row[11],
            "explanation": row[12],
            "description": row[13],
            "action": row[14],
            "category": row[15],
            "min_macro_score": row[16],
            "max_macro_score": row[17],
            "min_technical_score": row[18],
            "max_technical_score": row[19],
            "min_market_score": row[20],
            "max_market_score": row[21],
            "created_at": row[22].isoformat() if row[22] else None
        }
        for row in rows
    ]


# ✅ 1. Nieuwe setup opslaan
@router.post("/setups")
async def save_setup(request: Request):
    data = await request.json()
    logger.info(f"[save_setup] Ontvangen data: {data}")

    required_fields = ["name", "symbol", "strategy_type"]
    for field in required_fields:
        if not data.get(field):
            raise HTTPException(status_code=400, detail=f"'{field}' is verplicht")

    # 💡 Validate min/max logic
    for cat in ["macro", "technical", "market"]:
        min_val = data.get(f"min_{cat}_score")
        max_val = data.get(f"max_{cat}_score")
        if min_val and max_val and int(min_val) > int(max_val):
            raise HTTPException(
                status_code=400,
                detail=f"min_{cat}_score mag niet hoger zijn dan max_{cat}_score",
            )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM setups WHERE name = %s AND symbol = %s", (data["name"], data["symbol"]))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Setup met deze naam en symbool bestaat al")

            query_insert = """
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, tags, trend,
                    score_logic, favorite, explanation, description, action,
                    category,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    created_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            params = (
                data["name"],
                data["symbol"],
                data.get("timeframe"),
                data.get("account_type"),
                data.get("strategy_type"),
                data.get("min_investment"),
                data.get("dynamic_investment", False),
                tags,
                data.get("trend"),
                data.get("score_logic"),
                data.get("favorite", False),
                data.get("explanation"),
                data.get("description"),
                data.get("action"),
                data.get("category"),
                data.get("min_macro_score"),
                data.get("max_macro_score"),
                data.get("min_technical_score"),
                data.get("max_technical_score"),
                data.get("min_market_score"),
                data.get("max_market_score"),
                datetime.utcnow(),
            )
            cur.execute(query_insert, params)
            conn.commit()

        logger.info(f"✅ Setup '{data['name']}' opgeslagen voor {data['symbol']}")
        return {"status": "success", "message": "Setup succesvol opgeslagen"}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[save_setup] Fout bij opslaan: {e}")
        raise HTTPException(status_code=500, detail="Fout bij opslaan setup")
    finally:
        conn.close()


# ✅ 2. Alle setups ophalen
@router.get("/setups")
async def get_setups(strategy_type: Optional[str] = Query(None), exclude_strategy_type: Optional[str] = Query(None)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, tags, trend, score_logic,
                       favorite, explanation, description, action, category,
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score, created_at
                FROM setups
                WHERE TRUE
            """
            params = []
            if strategy_type:
                query += " AND LOWER(strategy_type) = LOWER(%s)"
                params.append(strategy_type)
            if exclude_strategy_type:
                query += " AND LOWER(strategy_type) != LOWER(%s)"
                params.append(exclude_strategy_type)
            query += " ORDER BY created_at DESC LIMIT 100"
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            return format_setup_rows(rows)
    except Exception as e:
        logger.error(f"❌ get_setups fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen setups")
    finally:
        conn.close()


# ✅ 3. DCA setups ophalen
@router.get("/setups/dca")
async def get_dca_setups():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, tags, trend, score_logic,
                       favorite, explanation, description, action, category,
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score, created_at
                FROM setups
                WHERE LOWER(strategy_type) = 'dca'
                ORDER BY created_at DESC LIMIT 50
            """)
            rows = cur.fetchall()
            return format_setup_rows(rows)
    finally:
        conn.close()


# ✅ 4. Setup bijwerken
@router.patch("/setups/{setup_id}")
async def update_setup(request: Request, setup_id: int):
    data = await request.json()
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            query = """
                UPDATE setups SET
                    name=%s, symbol=%s, timeframe=%s, account_type=%s,
                    strategy_type=%s, min_investment=%s, dynamic_investment=%s,
                    tags=%s, trend=%s, score_logic=%s, favorite=%s,
                    explanation=%s, description=%s, action=%s, category=%s,
                    min_macro_score=%s, max_macro_score=%s,
                    min_technical_score=%s, max_technical_score=%s,
                    min_market_score=%s, max_market_score=%s,
                    last_validated=%s
                WHERE id=%s
            """
            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            params = (
                data.get("name"), data.get("symbol"), data.get("timeframe"), data.get("account_type"),
                data.get("strategy_type"), data.get("min_investment"), data.get("dynamic_investment"),
                tags, data.get("trend"), data.get("score_logic"), data.get("favorite"),
                data.get("explanation"), data.get("description"), data.get("action"), data.get("category"),
                data.get("min_macro_score"), data.get("max_macro_score"),
                data.get("min_technical_score"), data.get("max_technical_score"),
                data.get("min_market_score"), data.get("max_market_score"),
                datetime.utcnow(), setup_id
            )
            cur.execute(query, params)
            conn.commit()
            return {"message": "Setup succesvol bijgewerkt"}
    except Exception as e:
        logger.error(f"Fout bij update setup: {e}")
        raise HTTPException(status_code=500, detail="Fout bij update setup")
    finally:
        conn.close()


# ✅ 5. Setup verwijderen (incl. cascade strategies)
@router.delete("/setups/{setup_id}")
async def delete_setup(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            # Check of setup bestaat
            cur.execute("SELECT id FROM setups WHERE id = %s", (setup_id,))
            exists = cur.fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Setup niet gevonden")

            # ❗ Cascade zorgt voor automatische deletion van strategies
            cur.execute("DELETE FROM setups WHERE id = %s", (setup_id,))
            conn.commit()

        return {
            "status": "success",
            "message": f"Setup {setup_id} succesvol verwijderd (inclusief gekoppelde strategies)"
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"[delete_setup] Fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij verwijderen setup")
    finally:
        conn.close()


# ✅ 6. Naamcheck
@router.get("/setups/check_name/{name}")
async def check_setup_name(name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM setups WHERE name = %s", (name,))
            exists = cur.fetchone()[0] > 0
            return {"exists": exists}
    finally:
        conn.close()


# ✅ 7. AI-uitleg genereren (ongewijzigd)
@router.post("/setups/explanation/{setup_id}")
async def generate_explanation(setup_id: int):
    explanation = generate_setup_explanation(setup_id)
    return {"explanation": explanation}


# ✅ 8. Celery trigger
@router.post("/setups/trigger")
def trigger_setup_task():
    validate_setups_task.delay()
    return {"message": "Setup-validatie gestart via Celery"}

# ✅ 9. Top setups
@router.get("/setups/top")
async def get_top_setups(limit: int = 3):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, tags, trend, score_logic,
                       favorite, explanation, description, action, category,
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score, created_at
                FROM setups
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return format_setup_rows(rows)
    finally:
        conn.close()

# ✅ 10. Laatste setup ophalen (fallback voor Active Setup Card)
@router.get("/setups/last")
async def get_last_setup():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id, name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, tags, trend,
                    score_logic, favorite, explanation, description, action,
                    category,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    created_at
                FROM setups
                ORDER BY created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()

        # Geen setups in database
        if not row:
            return {"setup": None}

        # Format één enkele rij
        formatted = format_setup_rows([row])[0]
        return {"setup": formatted}

    except Exception as e:
        logger.error(f"❌ get_last_setup fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen laatste setup")
    finally:
        conn.close()

# ✅ 11. Één enkele setup ophalen via ID
@router.get("/setups/{setup_id}")
async def get_setup_by_id(setup_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, symbol, timeframe, account_type, strategy_type,
                       min_investment, dynamic_investment, tags, trend,
                       score_logic, favorite, explanation, description, action,
                       category,
                       min_macro_score, max_macro_score,
                       min_technical_score, max_technical_score,
                       min_market_score, max_market_score, created_at
                FROM setups
                WHERE id = %s
            """, (setup_id,))
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Setup niet gevonden")

        return format_setup_rows([row])[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()



# ✅ 12. Actieve setup van vandaag ophalen (uit Setup Agent)
@router.get("/setups/active")
async def get_active_setup():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="❌ Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            # 1️⃣ Zoek de beste setup van vandaag (door de Setup-Agent bepaald)
            cur.execute("""
                SELECT 
                    ds.setup_id,
                    ds.score,
                    ds.explanation,
                    s.name,
                    s.symbol,
                    s.timeframe,
                    s.trend,
                    s.strategy_type,
                    s.min_investment,
                    s.dynamic_investment,
                    s.tags,
                    s.favorite,
                    s.action,
                    s.explanation as setup_explanation
                FROM daily_setup_scores ds
                JOIN setups s ON s.id = ds.setup_id
                WHERE ds.date = CURRENT_DATE
                AND ds.is_best = TRUE
                LIMIT 1
            """)

            row = cur.fetchone()

        if not row:
            # Geen active setup → frontend gebruikt fallback (last setup)
            return {"active": None}

        (
            setup_id, score, ai_explanation,
            name, symbol, timeframe, trend, strategy_type,
            min_investment, dynamic_investment, tags, favorite,
            action, setup_explanation
        ) = row

        return {
            "active": {
                "setup_id": setup_id,
                "score": score,
                "ai_explanation": ai_explanation,
                "name": name,
                "symbol": symbol,
                "timeframe": timeframe,
                "trend": trend,
                "strategy_type": strategy_type,
                "min_investment": min_investment,
                "dynamic_investment": dynamic_investment,
                "tags": tags,
                "favorite": favorite,
                "action": action,
                "setup_explanation": setup_explanation,
            }
        }

    except Exception as e:
        logger.error(f"❌ get_active_setup fout: {e}")
        raise HTTPException(status_code=500, detail="Fout bij ophalen actieve setup")

    finally:
        conn.close()

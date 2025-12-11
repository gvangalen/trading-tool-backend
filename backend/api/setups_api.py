from fastapi import APIRouter, HTTPException, Request, Query, Depends
from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.api.onboarding_api import mark_step_completed
from datetime import datetime
import logging
from backend.ai_agents.setup_ai_agent import generate_setup_explanation
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ============================================================
# 🧩 Helper — SAFE formatter voor setups (nooit meer index errors)
# ============================================================
def format_setup_rows(rows, cursor=None):
    """
    Future-proof formatter die automatisch kolomnamen koppelt aan waarden.
    Werkt met SELECT *, ongeacht kolomvolgorde.
    """

    # 1️⃣ Kolomnamen ophalen
    if cursor is None or cursor.description is None:
        raise RuntimeError("Cursor metadata is vereist voor format_setup_rows()")

    columns = [col[0] for col in cursor.description]

    formatted = []
    for row in rows:
        item = dict(zip(columns, row))

        # created_at netjes omzetten
        created_at = item.get("created_at")
        if hasattr(created_at, "isoformat"):
            created_at = created_at.isoformat()

        formatted.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "symbol": item.get("symbol"),
            "timeframe": item.get("timeframe"),
            "account_type": item.get("account_type"),
            "strategy_type": item.get("strategy_type"),
            "min_investment": item.get("min_investment"),
            "dynamic_investment": item.get("dynamic_investment"),
            "tags": item.get("tags"),
            "trend": item.get("trend"),
            "score_logic": item.get("score_logic"),
            "favorite": item.get("favorite"),
            "explanation": item.get("explanation"),
            "description": item.get("description"),
            "action": item.get("action"),
            "category": item.get("category"),

            "min_macro_score": item.get("min_macro_score"),
            "max_macro_score": item.get("max_macro_score"),
            "min_technical_score": item.get("min_technical_score"),
            "max_technical_score": item.get("max_technical_score"),
            "min_market_score": item.get("min_market_score"),
            "max_market_score": item.get("max_market_score"),

            "created_at": created_at,
            "user_id": item.get("user_id"),
        })

    return formatted


# ============================================================
# 1️⃣ Setup aanmaken
# ============================================================
@router.post("/setups")
async def save_setup(request: Request, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    data = await request.json()

    logger.info(f"[save_setup] user={user_id} data={data}")

    required_fields = ["name", "symbol", "strategy_type"]
    for f in required_fields:
        if not data.get(f):
            raise HTTPException(400, f"'{f}' is verplicht")

    # min/max validation
    for cat in ["macro", "technical", "market"]:
        mn = data.get(f"min_{cat}_score")
        mx = data.get(f"max_{cat}_score")
        if mn and mx and int(mn) > int(mx):
            raise HTTPException(400, f"min_{cat}_score mag niet hoger zijn dan max_{cat}_score")

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # Duplicate check per user
            cur.execute("""
                SELECT id FROM setups 
                WHERE name = %s AND symbol = %s AND user_id = %s
            """, (data["name"], data["symbol"], user_id))

            if cur.fetchone():
                raise HTTPException(409, "Setup met deze naam bestaat al voor deze gebruiker")

            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            query = """
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, dynamic_investment, tags, trend,
                    score_logic, favorite, explanation, description, action,
                    category,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    created_at, user_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            cur.execute(query, (
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
                user_id,
            ))

            conn.commit()

        mark_step_completed(conn, user_id, "setup")
        return {"status": "success", "message": "Setup opgeslagen"}

    finally:
        conn.close()


# ============================================================
# 2️⃣ Alle setups ophalen
# ============================================================
@router.get("/setups")
async def get_setups(
    strategy_type: Optional[str] = Query(None),
    exclude_strategy_type: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            query = """
                SELECT * FROM setups
                WHERE user_id = %s
            """
            params = [user_id]

            if strategy_type:
                query += " AND LOWER(strategy_type) = LOWER(%s)"
                params.append(strategy_type)

            if exclude_strategy_type:
                query += " AND LOWER(strategy_type) != LOWER(%s)"
                params.append(exclude_strategy_type)

            query += " ORDER BY created_at DESC LIMIT 200"

            cur.execute(query, tuple(params))
            rows = cur.fetchall()

            return format_setup_rows(rows, cur)

    finally:
        conn.close()


# ============================================================
# 3️⃣ DCA setups
# ============================================================
@router.get("/setups/dca")
async def get_dca_setups(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM setups
                WHERE LOWER(strategy_type) = 'dca'
                AND user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))

            rows = cur.fetchall()
            return format_setup_rows(rows, cur)

    finally:
        conn.close()


# ============================================================
# 4️⃣ Setup bijwerken
# ============================================================
@router.patch("/setups/{setup_id}")
async def update_setup(
    setup_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM setups WHERE id=%s AND user_id=%s",
                        (setup_id, user_id))

            if not cur.fetchone():
                raise HTTPException(403, "Setup behoort niet tot deze gebruiker")

            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",")]

            cur.execute("""
                UPDATE setups SET
                    name=%s, symbol=%s, timeframe=%s, account_type=%s,
                    strategy_type=%s, min_investment=%s, dynamic_investment=%s,
                    tags=%s, trend=%s, score_logic=%s, favorite=%s,
                    explanation=%s, description=%s, action=%s, category=%s,
                    min_macro_score=%s, max_macro_score=%s,
                    min_technical_score=%s, max_technical_score=%s,
                    min_market_score=%s, max_market_score=%s,
                    last_validated=%s
                WHERE id=%s AND user_id=%s
            """, (
                data.get("name"),
                data.get("symbol"),
                data.get("timeframe"),
                data.get("account_type"),
                data.get("strategy_type"),
                data.get("min_investment"),
                data.get("dynamic_investment"),
                tags,
                data.get("trend"),
                data.get("score_logic"),
                data.get("favorite"),
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
                setup_id,
                user_id,
            ))

            conn.commit()

        mark_step_completed(conn, user_id, "setup")
        return {"message": "Setup bijgewerkt"}

    finally:
        conn.close()


# ============================================================
# 5️⃣ Setup verwijderen
# ============================================================
@router.delete("/setups/{setup_id}")
async def delete_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("SELECT id FROM setups WHERE id=%s AND user_id=%s",
                        (setup_id, user_id))

            if not cur.fetchone():
                raise HTTPException(404, "Setup niet gevonden")

            cur.execute("DELETE FROM setups WHERE id=%s AND user_id=%s",
                        (setup_id, user_id))

            conn.commit()

        mark_step_completed(conn, user_id, "setup")
        return {"message": "Setup verwijderd"}

    finally:
        conn.close()


# ============================================================
# 6️⃣ Naamcheck
# ============================================================
@router.get("/setups/check_name/{name}")
async def check_name(name: str, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM setups
                WHERE name=%s AND user_id=%s
            """, (name, user_id))

            exists = cur.fetchone()[0] > 0

        return {"exists": exists}

    finally:
        conn.close()


# ============================================================
# 7️⃣ AI Explanation
# ============================================================
@router.post("/setups/explanation/{setup_id}")
async def ai_explanation(setup_id: int, current_user: dict = Depends(get_current_user)):
    explanation = generate_setup_explanation(setup_id, current_user["id"])
    return {"explanation": explanation}


# ============================================================
# 8️⃣ Top setups
# ============================================================
@router.get("/setups/top")
async def get_top_setups(limit: int = 3, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM setups
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (user_id, limit))

            rows = cur.fetchall()
            return format_setup_rows(rows, cur)

    finally:
        conn.close()


# ============================================================
# 9️⃣ Eén setup ophalen
# ============================================================
@router.get("/setups/{setup_id}")
async def get_setup_by_id(setup_id: int, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM setups
                WHERE id = %s AND user_id = %s
            """, (setup_id, user_id))

            row = cur.fetchone()

            if not row:
                raise HTTPException(404, "Setup niet gevonden")

            return format_setup_rows([row], cur)[0]

    finally:
        conn.close()

# ============================================================
# 🔟 Laatste setup (SAFE + ondersteunt optional setup_id)
# ============================================================
@router.get("/setups/last")
async def last_setup(
    setup_id: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Veilig ophalen van de laatste setup.
    - setup_id is optioneel
    - Geen crashes meer bij lege / verkeerde waardes
    """

    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            # 1️⃣ Als setup_id is meegegeven → fetch die ene setup
            if setup_id:
                cur.execute("""
                    SELECT * FROM setups
                    WHERE id = %s AND user_id = %s
                    LIMIT 1
                """, (setup_id, user_id))

                row = cur.fetchone()

                if not row:
                    return {"setup": None}

                return {"setup": format_setup_rows([row], cur)[0]}

            # 2️⃣ Anders → pak de laatste setup van deze user
            cur.execute("""
                SELECT * FROM setups
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))

            row = cur.fetchone()

            if not row:
                return {"setup": None}

            return {"setup": format_setup_rows([row], cur)[0]}

    except Exception as e:
        # ⛑️ NOOIT meer een crash — fout altijd veilig teruggeven
        return {"setup": None, "error": str(e)}

    finally:
        conn.close()

# ============================================================
# 🔥 Active setup (beste score van vandaag)
# ============================================================
@router.get("/setups/active")
async def get_active_setup(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
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
                AND ds.user_id = %s
                AND s.user_id = %s
                AND ds.is_best = TRUE
                LIMIT 1
            """, (user_id, user_id))

            row = cur.fetchone()

        if not row:
            return {"active": None}

        (
            setup_id,
            score,
            ai_explanation,
            name,
            symbol,
            timeframe,
            trend,
            strategy_type,
            min_inv,
            dyn_inv,
            tags,
            favorite,
            action,
            setup_explanation,
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
                "min_investment": min_inv,
                "dynamic_investment": dyn_inv,
                "tags": tags,
                "favorite": favorite,
                "action": action,
                "setup_explanation": setup_explanation,
            }
        }

    finally:
        conn.close()

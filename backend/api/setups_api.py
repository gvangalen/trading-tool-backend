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
# 🧩 Helper — SAFE formatter voor setups
# ============================================================
def format_setup_rows(rows, cursor=None):
    if cursor is None or cursor.description is None:
        raise RuntimeError("Cursor metadata is vereist voor format_setup_rows()")

    columns = [col[0] for col in cursor.description]
    formatted = []

    for row in rows:
        item = dict(zip(columns, row))

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

    required_fields = ["name", "symbol", "strategy_type"]
    for f in required_fields:
        if not data.get(f):
            raise HTTPException(400, f"'{f}' is verplicht")

    for cat in ["macro", "technical", "market"]:
        mn = data.get(f"min_{cat}_score")
        mx = data.get(f"max_{cat}_score")
        if mn is not None and mx is not None and int(mn) > int(mx):
            raise HTTPException(400, f"min_{cat}_score mag niet hoger zijn dan max_{cat}_score")

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM setups
                WHERE name=%s AND symbol=%s AND user_id=%s
            """, (data["name"], data["symbol"], user_id))

            if cur.fetchone():
                raise HTTPException(409, "Setup met deze naam bestaat al")

            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            cur.execute("""
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
            """, (
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
        return {"status": "success"}

    finally:
        conn.close()


# ============================================================
# 🔟 Laatste setup (MOET BOVEN {setup_id})
# ============================================================
@router.get("/setups/last")
async def last_setup(
    setup_id: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            if setup_id:
                cur.execute("""
                    SELECT * FROM setups
                    WHERE id=%s AND user_id=%s
                    LIMIT 1
                """, (setup_id, user_id))
                row = cur.fetchone()
                return {"setup": format_setup_rows([row], cur)[0]} if row else {"setup": None}

            cur.execute("""
                SELECT * FROM setups
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))
            row = cur.fetchone()
            return {"setup": format_setup_rows([row], cur)[0]} if row else {"setup": None}

    except Exception:
        return {"setup": None}

    finally:
        conn.close()


# ============================================================
# 2️⃣ Alle setups
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
            query = "SELECT * FROM setups WHERE user_id=%s"
            params = [user_id]

            if strategy_type:
                query += " AND LOWER(strategy_type)=LOWER(%s)"
                params.append(strategy_type)

            if exclude_strategy_type:
                query += " AND LOWER(strategy_type)!=LOWER(%s)"
                params.append(exclude_strategy_type)

            query += " ORDER BY created_at DESC LIMIT 200"
            cur.execute(query, tuple(params))

            return format_setup_rows(cur.fetchall(), cur)

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
                WHERE LOWER(strategy_type)='dca'
                AND user_id=%s
                ORDER BY created_at DESC
            """, (user_id,))
            return format_setup_rows(cur.fetchall(), cur)

    finally:
        conn.close()

# ============================================================
# 🔟 Daily setup scores (voor SetupMatchCard)
# ============================================================
@router.get("/setups/daily-scores")
async def get_daily_setup_scores(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ds.setup_id,
                    ds.score,
                    ds.is_best,
                    s.name,
                    s.symbol,
                    s.timeframe
                FROM daily_setup_scores ds
                JOIN setups s ON s.id = ds.setup_id
                WHERE ds.user_id = %s
                  AND ds.report_date = CURRENT_DATE
                ORDER BY ds.score DESC
                """,
                (user_id,),
            )

            rows = cur.fetchall()

        return [
            {
                "setup_id": int(r[0]),
                "score": int(r[1]) if r[1] is not None else None,
                "is_best": bool(r[2]),
                "name": r[3],
                "symbol": r[4],
                "timeframe": r[5],
            }
            for r in rows
        ]

    finally:
        conn.close()


# ============================================================
# 4️⃣ Setup bijwerken (FIX: min/max validatie)
# ============================================================
@router.patch("/setups/{setup_id}")
async def update_setup(
    setup_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()

    # -------------------------------
    # ✅ Validatie min/max scores
    # -------------------------------
    for cat in ["macro", "technical", "market"]:
        mn = data.get(f"min_{cat}_score")
        mx = data.get(f"max_{cat}_score")
        if mn is not None and mx is not None and int(mn) > int(mx):
            raise HTTPException(
                400, f"min_{cat}_score mag niet hoger zijn dan max_{cat}_score"
            )

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # -------------------------------
            # 🔐 Ownership check
            # -------------------------------
            cur.execute(
                "SELECT id FROM setups WHERE id=%s AND user_id=%s",
                (setup_id, user_id),
            )
            if not cur.fetchone():
                raise HTTPException(403, "Geen toegang")

            # -------------------------------
            # 🏷️ Tags normaliseren
            # -------------------------------
            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            # -------------------------------
            # 🧠 Dynamische UPDATE (BELANGRIJK)
            # -------------------------------
            update_fields = []
            values = []

            def add(field, value):
                update_fields.append(f"{field}=%s")
                values.append(value)

            add("name", data.get("name"))
            add("symbol", data.get("symbol"))
            add("timeframe", data.get("timeframe"))
            add("account_type", data.get("account_type"))
            add("strategy_type", data.get("strategy_type"))
            add("min_investment", data.get("min_investment"))
            add("dynamic_investment", data.get("dynamic_investment"))
            add("tags", tags)
            add("trend", data.get("trend"))
            add("score_logic", data.get("score_logic"))
            add("favorite", data.get("favorite"))
            add("description", data.get("description"))
            add("action", data.get("action"))
            add("category", data.get("category"))

            add("min_macro_score", data.get("min_macro_score"))
            add("max_macro_score", data.get("max_macro_score"))
            add("min_technical_score", data.get("min_technical_score"))
            add("max_technical_score", data.get("max_technical_score"))
            add("min_market_score", data.get("min_market_score"))
            add("max_market_score", data.get("max_market_score"))

            add("last_validated", datetime.utcnow())

            # 🚨 CRUCIAAL:
            # explanation ALLEEN aanpassen als frontend hem expliciet stuurt
            if "explanation" in data:
                add("explanation", data["explanation"])

            # -------------------------------
            # 🧱 SQL uitvoeren
            # -------------------------------
            query = f"""
                UPDATE setups SET
                    {", ".join(update_fields)}
                WHERE id=%s AND user_id=%s
            """

            values.extend([setup_id, user_id])
            cur.execute(query, tuple(values))

            conn.commit()

        mark_step_completed(conn, user_id, "setup")
        return {"message": "Setup bijgewerkt"}

    finally:
        conn.close()


# ============================================================
# 5️⃣ Setup verwijderen
# ============================================================
@router.delete("/setups/{setup_id}")
async def delete_setup(setup_id: int, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM setups WHERE id=%s AND user_id=%s",
                (setup_id, user_id),
            )
            if not cur.fetchone():
                raise HTTPException(404, "Niet gevonden")

            cur.execute(
                "DELETE FROM setups WHERE id=%s AND user_id=%s",
                (setup_id, user_id),
            )
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
            cur.execute(
                "SELECT COUNT(*) FROM setups WHERE name=%s AND user_id=%s",
                (name, user_id),
            )
            return {"exists": cur.fetchone()[0] > 0}

    finally:
        conn.close()


# ============================================================
# 7️⃣ AI explanation
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
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
            """, (user_id, limit))
            return format_setup_rows(cur.fetchall(), cur)

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
            cur.execute(
                "SELECT * FROM setups WHERE id=%s AND user_id=%s",
                (setup_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Niet gevonden")
            return format_setup_rows([row], cur)[0]

    finally:
        conn.close()


# ============================================================
# 🔥 Active setup
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
                    s.explanation
                FROM daily_setup_scores ds
                JOIN setups s ON s.id = ds.setup_id
                WHERE ds.report_date = CURRENT_DATE
                  AND ds.user_id = %s
                  AND ds.is_best = TRUE
                LIMIT 1
            """, (user_id,))

            row = cur.fetchone()

        if not row:
            return {"active": None}

        (
            setup_id, score, ai_exp, name, symbol, timeframe,
            trend, strategy_type, min_inv, dyn_inv,
            tags, favorite, action, setup_exp
        ) = row

        return {
            "active": {
                "setup_id": setup_id,
                "score": score,
                "ai_explanation": ai_exp,
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
                "setup_explanation": setup_exp,
            }
        }

    finally:
        conn.close()



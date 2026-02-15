print("✅ strategy_api.py geladen!")

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from psycopg2.extras import RealDictCursor
import json, csv, io, logging
from datetime import datetime

router = APIRouter()
logger = logging.getLogger(__name__)

# ==========================================================
# DB & AUTH
# ==========================================================
from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.api.onboarding_api import mark_step_completed


# ==========================================================
# 🧩 FORMATTER — ÉÉN waarheid voor strategy cards & edit
# ==========================================================
def format_strategy_row(row: dict):
    data = row.get("data") or {}

    return {
        "id": row.get("id"),
        "setup_id": row.get("setup_id"),
        "strategy_type": row.get("strategy_type"),

        # ======================================================
        # Execution (position sizing logic)
        # ======================================================
        "execution_mode": row.get("execution_mode"),
        "base_amount": row.get("base_amount"),
        "frequency": row.get("frequency"),
        "decision_curve": row.get("decision_curve"),

        # ⭐ NIEUW — curve metadata
        "decision_curve_name": data.get("decision_curve_name"),
        "decision_curve_id": data.get("decision_curve_id"),

        # ======================================================
        # Core info
        # ======================================================
        "symbol": data.get("symbol"),
        "timeframe": data.get("timeframe"),

        # ======================================================
        # Trading levels
        # ======================================================
        "entry": row.get("entry") or data.get("entry"),
        "target": row.get("target") or data.get("target"),
        "targets": data.get("targets"),
        "stop_loss": row.get("stop_loss") or data.get("stop_loss"),

        # ======================================================
        # Explanation
        # ======================================================
        "explanation": row.get("explanation") or data.get("explanation"),
        "ai_explanation": data.get("ai_explanation"),

        # ======================================================
        # Meta
        # ======================================================
        "risk_profile": row.get("risk_profile") or data.get("risk_profile"),
        "tags": data.get("tags", []),
        "favorite": data.get("favorite", False),

        # ======================================================
        # Timestamps
        # ======================================================
        "created_at": (
            row.get("created_at").isoformat()
            if row.get("created_at") else None
        ),
    }

# ==========================================================
# 1️⃣ CREATE STRATEGY
# ==========================================================
@router.post("/strategies")
async def save_strategy(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    data = await request.json()

    strategy_type = (data.get("strategy_type") or "").lower()
    if strategy_type not in ["manual", "trading", "dca"]:
        raise HTTPException(400, "Ongeldig strategy_type")

    execution_mode = data.get("execution_mode", "none")
    if execution_mode not in ["none", "fixed", "custom"]:
        raise HTTPException(400, "Ongeldige execution_mode")

    if not data.get("setup_id"):
        raise HTTPException(400, "setup_id is verplicht")

    if execution_mode in ["fixed", "custom"] and not data.get("base_amount"):
        raise HTTPException(400, "base_amount is verplicht bij execution")

    if execution_mode == "custom" and not data.get("decision_curve"):
        raise HTTPException(400, "decision_curve is verplicht bij custom execution")

    if strategy_type != "dca":
        if not data.get("entry") or not data.get("stop_loss"):
            raise HTTPException(400, "entry en stop_loss zijn verplicht")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            # --------------------------------------------------
            # VERIFY OWNERSHIP
            # --------------------------------------------------
            cur.execute(
                "SELECT id FROM setups WHERE id=%s AND user_id=%s",
                (data["setup_id"], user_id)
            )
            if not cur.fetchone():
                raise HTTPException(403, "Setup niet van gebruiker")

            # --------------------------------------------------
            # UNIQUE STRATEGY CHECK
            # --------------------------------------------------
            cur.execute("""
                SELECT id FROM strategies
                WHERE setup_id=%s AND strategy_type=%s AND user_id=%s
            """, (data["setup_id"], strategy_type, user_id))
            if cur.fetchone():
                raise HTTPException(409, "Strategie bestaat al")

            # --------------------------------------------------
            # 🔥 SAVE CUSTOM CURVE (NEW)
            # --------------------------------------------------
            curve_id = None

            if execution_mode == "custom":
                curve = data.get("decision_curve")

                curve_name = data.get("decision_curve_name")
                if not curve_name:
                    ts = datetime.utcnow().strftime("%Y%m%d-%H%M")
                    curve_name = f"Custom Curve {ts}"

                cur.execute("""
                    INSERT INTO indicator_curves (
                        user_id,
                        domain,
                        indicator,
                        curve,
                        name,
                        is_active,
                        is_preset,
                        created_at
                    )
                    VALUES (%s,'execution','position_size',%s,%s,true,false,NOW())
                    RETURNING id
                """, (
                    user_id,
                    json.dumps(curve),
                    curve_name
                ))

                curve_id = cur.fetchone()[0]

                # store reference
                data["decision_curve_id"] = curve_id
                data["decision_curve_name"] = curve_name

            # --------------------------------------------------
            # INSERT STRATEGY
            # --------------------------------------------------
            cur.execute("""
                INSERT INTO strategies (
                    setup_id,
                    strategy_type,
                    execution_mode,
                    base_amount,
                    decision_curve,
                    frequency,
                    entry,
                    target,
                    stop_loss,
                    explanation,
                    risk_profile,
                    data,
                    created_at,
                    user_id
                )
                VALUES (
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,
                    %s::jsonb,
                    NOW(),%s
                )
                RETURNING id
            """, (
                data["setup_id"],
                strategy_type,
                execution_mode,
                data.get("base_amount"),
                json.dumps(data.get("decision_curve")) if data.get("decision_curve") else None,
                data.get("frequency"),
                str(data.get("entry")) if data.get("entry") else None,
                data.get("target"),
                str(data.get("stop_loss")) if data.get("stop_loss") else None,
                data.get("explanation"),
                data.get("risk_profile"),
                json.dumps(data),
                user_id
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        mark_step_completed(conn, user_id, "strategy")

        return {
            "id": strategy_id,
            "curve_id": curve_id,
            "message": "✅ Strategie opgeslagen"
        }

    finally:
        conn.close()
        
# ==========================================================
# 2️⃣ QUERY STRATEGIES (cards)
# ==========================================================
@router.post("/strategies/query")
async def query_strategies(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    filters = await request.json()
    user_id = current_user["id"]

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            q = """
                SELECT *
                FROM strategies
                WHERE user_id=%s
            """
            p = [user_id]

            if filters.get("symbol"):
                q += " AND data->>'symbol'=%s"
                p.append(filters["symbol"])

            if filters.get("timeframe"):
                q += " AND data->>'timeframe'=%s"
                p.append(filters["timeframe"])

            q += " ORDER BY created_at DESC"
            cur.execute(q, tuple(p))
            rows = cur.fetchall()

        return [format_strategy_row(r) for r in rows]

    finally:
        conn.close()


# ==========================================================
# 3️⃣ GENERATE STRATEGY (AI)
# ==========================================================
@router.post("/strategies/generate/{setup_id}")
async def generate_strategy_for_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    from backend.celery_task.strategy_task import generate_for_setup
    task = generate_for_setup.delay(
        user_id=current_user["id"],
        setup_id=setup_id
    )
    return {"task_id": task.id}


# ==========================================================
# 4️⃣ UPDATE STRATEGY (incl curve editor)
# ==========================================================
@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    data = await request.json()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies SET
                    execution_mode=%s,
                    base_amount=%s,
                    decision_curve=%s,
                    frequency=%s,
                    data=%s,
                    explanation=%s,
                    risk_profile=%s
                WHERE id=%s AND user_id=%s
            """, (
                data.get("execution_mode"),
                data.get("base_amount"),
                json.dumps(data.get("decision_curve")) if data.get("decision_curve") else None,
                data.get("frequency"),
                json.dumps(data),
                data.get("explanation"),
                data.get("risk_profile"),
                strategy_id,
                current_user["id"]
            ))

            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")

            conn.commit()

        return {"message": "✅ Strategie bijgewerkt"}

    finally:
        conn.close()


# ==========================================================
# 5️⃣ DELETE STRATEGY
# ==========================================================
@router.delete("/strategies/{strategy_id}")
async def delete_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Strategie niet gevonden")
            conn.commit()
        return {"message": "🗑 Verwijderd"}

    finally:
        conn.close()


# ==========================================================
# 6️⃣ AI STRATEGY ANALYSE
# ==========================================================
@router.post("/strategies/analyze/{strategy_id}")
async def analyze_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Strategie niet gevonden")

        strategy_data = row[0]

    finally:
        conn.close()

    from backend.ai_agents.strategy_ai_agent import analyze_and_store_strategy
    analyze_and_store_strategy(
        strategy_id=strategy_id,
        strategies=[strategy_data]
    )

    return {"message": "🧠 Strategy AI analyse uitgevoerd"}


# ==========================================================
# 7️⃣ GET STRATEGY BY SETUP
# ==========================================================
@router.get("/strategies/by_setup/{setup_id}")
async def get_strategy_by_setup(
    setup_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM strategies
                WHERE setup_id=%s AND user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (setup_id, current_user["id"]))
            row = cur.fetchone()

        if not row:
            return {"exists": False}

        return {"exists": True, "strategy": format_strategy_row(row)}

    finally:
        conn.close()


# ==========================================================
# 8️⃣ LAST STRATEGY
# ==========================================================
@router.get("/strategies/last")
async def get_last_strategy(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM strategies
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
            """, (current_user["id"],))
            row = cur.fetchone()

        return format_strategy_row(row) if row else None

    finally:
        conn.close()


# ==========================================================
# 9️⃣ FAVORITE TOGGLE
# ==========================================================
@router.patch("/strategies/{strategy_id}/favorite")
async def toggle_favorite(
    strategy_id: int,
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM strategies WHERE id=%s AND user_id=%s",
                (strategy_id, current_user["id"])
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Niet gevonden")

            data = row[0] or {}
            data["favorite"] = not data.get("favorite", False)

            cur.execute(
                "UPDATE strategies SET data=%s WHERE id=%s",
                (json.dumps(data), strategy_id)
            )
            conn.commit()

        return {"favorite": data["favorite"]}

    finally:
        conn.close()


# ==========================================================
# 🔟 EXPORT CSV
# ==========================================================
@router.get("/strategies/export")
async def export_strategies(current_user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, data, created_at
                FROM strategies
                WHERE user_id=%s
                ORDER BY created_at DESC
            """, (current_user["id"],))
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Symbol", "Entry", "Stop Loss", "Created"])

        for id_, s, created in rows:
            writer.writerow([
                id_,
                s.get("symbol"),
                s.get("entry"),
                s.get("stop_loss"),
                created.strftime("%Y-%m-%d %H:%M")
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=strategies.csv"}
        )

    finally:
        conn.close()

# ==========================================================
# 11 Get Curvers 
# ==========================================================
@router.get("/curves/execution")
async def get_execution_curves(
    current_user: dict = Depends(get_current_user)
):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, curve
                FROM indicator_curves
                WHERE user_id=%s
                  AND domain='execution'
                  AND is_active=true
                ORDER BY created_at DESC
            """, (current_user["id"],))

            rows = cur.fetchall()

        return rows

    finally:
        conn.close()


# ==========================================================
# 12 ACTIVE STRATEGY FOR TODAY
# ==========================================================
@router.get("/strategies/active-today")
async def get_active_strategy_today(
    current_user: dict = Depends(get_current_user)
):
    """
    Geeft exact 1 strategie terug die vandaag uitgevoerd moet worden.
    Deterministisch — geen AI — pure execution logica.
    """
    user_id = current_user["id"]
    today = datetime.utcnow().date()

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM strategies
                WHERE user_id = %s
                  AND execution_mode IN ('fixed', 'custom')
                ORDER BY created_at DESC
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            return {"active": False}

        # ----------------------------
        # Execution filter (simpel & robuust)
        # ----------------------------
        for row in rows:
            frequency = row.get("frequency")

            if frequency == "daily":
                return {
                    "active": True,
                    "strategy": format_strategy_row(row)
                }

            if frequency == "weekly":
                created = row.get("created_at")
                if created and created.date().weekday() == today.weekday():
                    return {
                        "active": True,
                        "strategy": format_strategy_row(row)
                    }

        return {"active": False}

    finally:
        conn.close()

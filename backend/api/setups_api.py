# backend/api/setups_api.py
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from datetime import datetime
from typing import Optional, Any, Dict, List
import logging
import json

from psycopg2.extras import Json

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user
from backend.api.onboarding_api import mark_step_completed
from backend.ai_agents.setup_ai_agent import generate_setup_explanation

router = APIRouter()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# ✅ Helpers
# ============================================================

ALLOWED_EXECUTION_MODES = {"fixed", "custom"}


def _iso(dt: Any) -> Optional[str]:
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return None


def _normalize_tags(tags: Any) -> List[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    return [str(tags).strip()] if str(tags).strip() else []


def _ensure_json(value: Any) -> Optional[Any]:
    """
    Zorgt dat jsonb-velden netjes als dict/list terugkomen wanneer psycopg ze als string geeft.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return value
    return value


def _validate_score_ranges(data: Dict[str, Any]) -> None:
    for cat in ["macro", "technical", "market"]:
        mn = data.get(f"min_{cat}_score")
        mx = data.get(f"max_{cat}_score")
        if mn is not None and mx is not None and int(mn) > int(mx):
            raise HTTPException(400, f"min_{cat}_score mag niet hoger zijn dan max_{cat}_score")


def _validate_execution_logic(data: Dict[str, Any]) -> None:
    """
    Validatie voor jullie 2-state execution model:
    - execution_mode: fixed | custom
    - base_amount verplicht en > 0 (voor beide)
    - custom vereist decision_curve
    """
    mode = (data.get("execution_mode") or "fixed").lower().strip()
    if mode not in ALLOWED_EXECUTION_MODES:
        raise HTTPException(400, "execution_mode moet 'fixed' of 'custom' zijn")

    base_amount = data.get("base_amount")
    if base_amount is None:
        raise HTTPException(400, "base_amount is verplicht")
    try:
        base_amount_num = float(base_amount)
    except Exception:
        raise HTTPException(400, "base_amount moet een getal zijn")
    if base_amount_num <= 0:
        raise HTTPException(400, "base_amount moet > 0 zijn")

    if mode == "custom":
        if not data.get("decision_curve"):
            raise HTTPException(400, "custom mode vereist decision_curve")
        _validate_decision_curve(data.get("decision_curve"))

    # pause_conditions is optioneel, maar als aanwezig moet het JSON zijn
    if "pause_conditions" in data and data.get("pause_conditions") is not None:
        pc = data.get("pause_conditions")
        if not isinstance(pc, (dict, list)):
            raise HTTPException(400, "pause_conditions moet JSON (object/array) zijn")

    # sell_allowed default false, maar als meegegeven: bool
    if "sell_allowed" in data and data.get("sell_allowed") is not None:
        if not isinstance(data.get("sell_allowed"), bool):
            raise HTTPException(400, "sell_allowed moet true/false zijn")


def _validate_decision_curve(curve: Any) -> None:
    """
    Curve format:
    {
      "input": "market_score",
      "points": [
        {"x": 20, "y": 1.5},
        {"x": 40, "y": 1.2},
        {"x": 60, "y": 1.0},
        {"x": 80, "y": 0.5}
      ]
    }
    """
    if not isinstance(curve, dict):
        raise HTTPException(400, "decision_curve moet een JSON object zijn")

    points = curve.get("points")
    if not isinstance(points, list) or len(points) < 2:
        raise HTTPException(400, "decision_curve.points moet een lijst zijn met minimaal 2 punten")

    cleaned: List[Dict[str, float]] = []
    for p in points:
        if not isinstance(p, dict):
            raise HTTPException(400, "decision_curve.points bevat ongeldig punt")
        if "x" not in p or "y" not in p:
            raise HTTPException(400, "decision_curve.points vereist x en y")
        try:
            x = float(p["x"])
            y = float(p["y"])
        except Exception:
            raise HTTPException(400, "decision_curve.points x/y moeten getallen zijn")
        if y < 0:
            raise HTTPException(400, "decision_curve.points y mag niet negatief zijn")
        cleaned.append({"x": x, "y": y})

    # Check monotonic x (na sorteren)
    cleaned_sorted = sorted(cleaned, key=lambda d: d["x"])
    xs = [d["x"] for d in cleaned_sorted]
    if len(xs) != len(set(xs)):
        raise HTTPException(400, "decision_curve.points x-waarden moeten uniek zijn")

    # Optional: enforce range (handig omdat scores 0-100 zijn)
    # Niet hard verplicht, maar wel sane defaults:
    # if xs[0] > 0 or xs[-1] < 100: ... (laten we nu niet blokkeren)

    # input is optioneel, default market_score
    inp = curve.get("input")
    if inp is not None and not isinstance(inp, str):
        raise HTTPException(400, "decision_curve.input moet een string zijn")


def format_setup_rows(rows, cursor=None):
    if cursor is None or cursor.description is None:
        raise RuntimeError("Cursor metadata is vereist voor format_setup_rows()")

    columns = [col[0] for col in cursor.description]
    formatted = []

    for row in rows:
        item = dict(zip(columns, row))

        formatted.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "symbol": item.get("symbol"),
            "timeframe": item.get("timeframe"),
            "account_type": item.get("account_type"),
            "strategy_type": item.get("strategy_type"),
            "min_investment": item.get("min_investment"),
            "tags": item.get("tags") or [],
            "trend": item.get("trend"),
            "score_logic": item.get("score_logic"),
            "favorite": bool(item.get("favorite")) if item.get("favorite") is not None else False,
            "explanation": item.get("explanation"),
            "description": item.get("description"),
            "action": item.get("action"),
            "category": item.get("category"),
            "filters": _ensure_json(item.get("filters")),

            "min_macro_score": item.get("min_macro_score"),
            "max_macro_score": item.get("max_macro_score"),
            "min_technical_score": item.get("min_technical_score"),
            "max_technical_score": item.get("max_technical_score"),
            "min_market_score": item.get("min_market_score"),
            "max_market_score": item.get("max_market_score"),

            # ✅ New execution fields
            "execution_mode": item.get("execution_mode") or "fixed",
            "base_amount": item.get("base_amount"),
            "decision_curve": _ensure_json(item.get("decision_curve")),
            "pause_conditions": _ensure_json(item.get("pause_conditions")),
            "sell_allowed": bool(item.get("sell_allowed")) if item.get("sell_allowed") is not None else False,

            "last_validated": _iso(item.get("last_validated")),
            "created_at": _iso(item.get("created_at")),
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

    _validate_score_ranges(data)
    _validate_execution_logic(data)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM setups
                WHERE name=%s AND symbol=%s AND user_id=%s
                """,
                (data["name"], data["symbol"], user_id),
            )
            if cur.fetchone():
                raise HTTPException(409, "Setup met deze naam bestaat al")

            tags = _normalize_tags(data.get("tags", []))

            execution_mode = (data.get("execution_mode") or "fixed").lower().strip()
            base_amount = data.get("base_amount")
            decision_curve = data.get("decision_curve")
            pause_conditions = data.get("pause_conditions")
            sell_allowed = bool(data.get("sell_allowed")) if data.get("sell_allowed") is not None else False

            cur.execute(
                """
                INSERT INTO setups (
                    name, symbol, timeframe, account_type, strategy_type,
                    min_investment, tags, trend,
                    score_logic, favorite, explanation, description, action,
                    category,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    execution_mode, base_amount, decision_curve, pause_conditions, sell_allowed,
                    created_at, user_id
                )
                VALUES (
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,
                    %s,%s,
                    %s,%s,
                    %s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s
                )
                """,
                (
                    data["name"],
                    data["symbol"],
                    data.get("timeframe"),
                    data.get("account_type"),
                    data.get("strategy_type"),
                    data.get("min_investment"),
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
                    execution_mode,
                    base_amount,
                    Json(decision_curve) if decision_curve is not None else None,
                    Json(pause_conditions) if pause_conditions is not None else None,
                    sell_allowed,
                    datetime.utcnow(),
                    user_id,
                ),
            )

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
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            if setup_id:
                cur.execute(
                    """
                    SELECT * FROM setups
                    WHERE id=%s AND user_id=%s
                    LIMIT 1
                    """,
                    (setup_id, user_id),
                )
                row = cur.fetchone()
                return {"setup": format_setup_rows([row], cur)[0]} if row else {"setup": None}

            cur.execute(
                """
                SELECT * FROM setups
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
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
    current_user: dict = Depends(get_current_user),
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
            cur.execute(
                """
                SELECT * FROM setups
                WHERE LOWER(strategy_type)='dca'
                  AND user_id=%s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )
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
# 4️⃣ Setup bijwerken (PATCH)
# ============================================================
@router.patch("/setups/{setup_id}")
async def update_setup(
    setup_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    data = await request.json()

    # Validaties (alleen als velden aanwezig zijn)
    if any(k in data for k in ["min_macro_score", "max_macro_score", "min_technical_score", "max_technical_score", "min_market_score", "max_market_score"]):
        _validate_score_ranges(data)

    # Execution logica valideren alleen als één van deze velden ge-update wordt,
    # of als je mode custom zet.
    if any(k in data for k in ["execution_mode", "base_amount", "decision_curve", "pause_conditions", "sell_allowed"]):
        # Voor validate hebben we base_amount nodig als het ontbreekt -> haal uit DB
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT execution_mode, base_amount, decision_curve, pause_conditions, sell_allowed
                    FROM setups
                    WHERE id=%s AND user_id=%s
                    """,
                    (setup_id, user_id),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(403, "Geen toegang")

                current = {
                    "execution_mode": row[0],
                    "base_amount": row[1],
                    "decision_curve": _ensure_json(row[2]),
                    "pause_conditions": _ensure_json(row[3]),
                    "sell_allowed": row[4],
                }

            merged = {**current, **data}
            _validate_execution_logic(merged)
        finally:
            conn.close()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Ownership check
            cur.execute("SELECT id FROM setups WHERE id=%s AND user_id=%s", (setup_id, user_id))
            if not cur.fetchone():
                raise HTTPException(403, "Geen toegang")

            tags = _normalize_tags(data.get("tags")) if "tags" in data else None

            update_fields = []
            values = []

            def add(field: str, value: Any):
                update_fields.append(f"{field}=%s")
                values.append(value)

            # Alleen updaten als key aanwezig is (voorkomt per ongeluk overschrijven met None)
            if "name" in data: add("name", data.get("name"))
            if "symbol" in data: add("symbol", data.get("symbol"))
            if "timeframe" in data: add("timeframe", data.get("timeframe"))
            if "account_type" in data: add("account_type", data.get("account_type"))
            if "strategy_type" in data: add("strategy_type", data.get("strategy_type"))
            if "min_investment" in data: add("min_investment", data.get("min_investment"))
            if "trend" in data: add("trend", data.get("trend"))
            if "score_logic" in data: add("score_logic", data.get("score_logic"))
            if "favorite" in data: add("favorite", data.get("favorite"))
            if "description" in data: add("description", data.get("description"))
            if "action" in data: add("action", data.get("action"))
            if "category" in data: add("category", data.get("category"))
            if tags is not None: add("tags", tags)

            if "min_macro_score" in data: add("min_macro_score", data.get("min_macro_score"))
            if "max_macro_score" in data: add("max_macro_score", data.get("max_macro_score"))
            if "min_technical_score" in data: add("min_technical_score", data.get("min_technical_score"))
            if "max_technical_score" in data: add("max_technical_score", data.get("max_technical_score"))
            if "min_market_score" in data: add("min_market_score", data.get("min_market_score"))
            if "max_market_score" in data: add("max_market_score", data.get("max_market_score"))

            # ✅ New execution fields
            if "execution_mode" in data:
                add("execution_mode", (data.get("execution_mode") or "fixed").lower().strip())
            if "base_amount" in data:
                add("base_amount", data.get("base_amount"))
            if "decision_curve" in data:
                dc = data.get("decision_curve")
                add("decision_curve", Json(dc) if dc is not None else None)
            if "pause_conditions" in data:
                pc = data.get("pause_conditions")
                add("pause_conditions", Json(pc) if pc is not None else None)
            if "sell_allowed" in data:
                add("sell_allowed", bool(data.get("sell_allowed")))

            # explanation alleen aanpassen als expliciet gestuurd
            if "explanation" in data:
                add("explanation", data.get("explanation"))

            add("last_validated", datetime.utcnow())

            if not update_fields:
                return {"message": "Geen wijzigingen ontvangen"}

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
            cur.execute("SELECT id FROM setups WHERE id=%s AND user_id=%s", (setup_id, user_id))
            if not cur.fetchone():
                raise HTTPException(404, "Niet gevonden")

            cur.execute("DELETE FROM setups WHERE id=%s AND user_id=%s", (setup_id, user_id))
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
            cur.execute("SELECT COUNT(*) FROM setups WHERE name=%s AND user_id=%s", (name, user_id))
            return {"exists": cur.fetchone()[0] > 0}

    finally:
        conn.close()


# ============================================================
# 7️⃣ AI explanation
# ============================================================
@router.post("/setups/explanation/{setup_id}")
async def ai_explanation(setup_id: int, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        explanation = generate_setup_explanation(setup_id, user_id)
        if not explanation:
            raise HTTPException(500, "AI uitleg kon niet worden gegenereerd")

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE setups
                SET explanation = %s,
                    last_validated = NOW()
                WHERE id = %s AND user_id = %s
                """,
                (explanation, setup_id, user_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Setup niet gevonden")

        conn.commit()
        return {"explanation": explanation}

    except Exception:
        conn.rollback()
        logger.exception("❌ AI setup explanation failed")
        raise

    finally:
        conn.close()


# ============================================================
# 8️⃣ Top setups
# ============================================================
@router.get("/setups/top")
async def get_top_setups(limit: int = 3, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM setups
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
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
            cur.execute("SELECT * FROM setups WHERE id=%s AND user_id=%s", (setup_id, user_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Niet gevonden")
            return format_setup_rows([row], cur)[0]

    finally:
        conn.close()


# ============================================================
# 🔥 Active setup (best setup van vandaag)
# ============================================================
@router.get("/setups/active")
async def get_active_setup(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                    s.tags,
                    s.favorite,
                    s.action,
                    s.explanation,

                    -- ✅ New fields needed for live execution
                    s.execution_mode,
                    s.base_amount,
                    s.decision_curve,
                    s.pause_conditions,
                    s.sell_allowed
                FROM daily_setup_scores ds
                JOIN setups s ON s.id = ds.setup_id
                WHERE ds.report_date = CURRENT_DATE
                  AND ds.user_id = %s
                  AND ds.is_best = TRUE
                LIMIT 1
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if not row:
            return {"active": None}

        (
            setup_id, score, ai_exp,
            name, symbol, timeframe,
            trend, strategy_type, min_inv,
            tags, favorite, action, setup_exp,
            execution_mode, base_amount, decision_curve, pause_conditions, sell_allowed
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
                "tags": tags,
                "favorite": favorite,
                "action": action,
                "setup_explanation": setup_exp,

                # ✅ Execution payload
                "execution_mode": execution_mode or "fixed",
                "base_amount": base_amount,
                "decision_curve": _ensure_json(decision_curve),
                "pause_conditions": _ensure_json(pause_conditions),
                "sell_allowed": bool(sell_allowed) if sell_allowed is not None else False,
            }
        }

    finally:
        conn.close()

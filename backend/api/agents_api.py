import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user

from celery.result import AsyncResult
from backend.celery_task.celery_app import celery_app

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================
# Helper
# ============================================================
def get_conn_cursor():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Geen databaseverbinding.")
    return conn, conn.cursor()


# ============================================================
# ========== MACRO ===========================================
# ============================================================

@router.get("/agents/insights/macro")
async def get_macro_insight(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT avg_score, trend, bias, risk, summary, top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='macro' AND user_id=%s
            ORDER BY date DESC, created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"insight": None}

        avg, trend, bias, risk, summary, top, d, created = row
        top = json.loads(top) if isinstance(top, str) else (top or [])

        return {
            "insight": {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top,
                "date": d.isoformat() if d else None,
                "created_at": created.isoformat() if created else None,
            }
        }
    finally:
        conn.close()


@router.get("/agents/reflections/macro")
async def get_macro_reflections(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='macro' AND user_id=%s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10
        """, (user_id,))
        rows = cur.fetchall()
        return {
            "reflections": [
                {
                    "indicator": i,
                    "raw_score": float(rs) if rs is not None else None,
                    "ai_score": float(ai) if ai is not None else None,
                    "compliance": float(c) if c is not None else None,
                    "comment": cm,
                    "recommendation": r,
                    "date": d.isoformat() if d else None,
                    "timestamp": ts.isoformat() if ts else None,
                }
                for (i, rs, ai, c, cm, r, d, ts) in rows
            ]
        }
    finally:
        conn.close()


# ============================================================
# ========== MARKET ==========================================
# ============================================================

@router.get("/agents/insights/market")
async def get_market_insight(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT avg_score, trend, bias, risk, summary, top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='market' AND user_id=%s
            ORDER BY date DESC, created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"insight": None}

        avg, trend, bias, risk, summary, top, d, created = row
        top = json.loads(top) if isinstance(top, str) else (top or [])

        return {
            "insight": {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top,
                "date": d.isoformat() if d else None,
                "created_at": created.isoformat() if created else None,
            }
        }
    finally:
        conn.close()


@router.get("/agents/reflections/market")
async def get_market_reflections(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='market' AND user_id=%s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10
        """, (user_id,))
        rows = cur.fetchall()
        return {
            "reflections": [
                {
                    "indicator": i,
                    "raw_score": float(rs) if rs is not None else None,
                    "ai_score": float(ai) if ai is not None else None,
                    "compliance": float(c) if c is not None else None,
                    "comment": cm,
                    "recommendation": r,
                    "date": d.isoformat() if d else None,
                    "timestamp": ts.isoformat() if ts else None,
                }
                for (i, rs, ai, c, cm, r, d, ts) in rows
            ]
        }
    finally:
        conn.close()


# ============================================================
# ========== TECHNICAL =======================================
# ============================================================

@router.get("/agents/insights/technical")
async def get_technical_insight(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT avg_score, trend, bias, risk, summary, top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='technical' AND user_id=%s
            ORDER BY date DESC, created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"insight": None}

        avg, trend, bias, risk, summary, top, d, created = row
        top = json.loads(top) if isinstance(top, str) else (top or [])

        return {
            "insight": {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top,
                "date": d.isoformat() if d else None,
                "created_at": created.isoformat() if created else None,
            }
        }
    finally:
        conn.close()


@router.get("/agents/reflections/technical")
async def get_technical_reflections(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()

    try:
        cur.execute("""
            SELECT DISTINCT ON (indicator)
                   indicator,
                   raw_score,
                   ai_score,
                   compliance,
                   comment,
                   recommendation,
                   date,
                   timestamp
            FROM ai_reflections
            WHERE category = 'technical'
              AND user_id = %s
              AND date = CURRENT_DATE
            ORDER BY indicator, timestamp DESC;
        """, (user_id,))

        rows = cur.fetchall()

        return {
            "reflections": [
                {
                    "indicator": i,
                    "raw_score": float(rs) if rs is not None else None,
                    "ai_score": float(ai) if ai is not None else None,
                    "compliance": float(c) if c is not None else None,
                    "comment": cm,
                    "recommendation": r,
                    "date": d.isoformat() if d else None,
                    "timestamp": ts.isoformat() if ts else None,
                }
                for (i, rs, ai, c, cm, r, d, ts) in rows
            ]
        }

    finally:
        conn.close()


# ============================================================
# ========== SETUP ===========================================
# ============================================================

@router.get("/agents/insights/setup")
async def get_setup_insight(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT avg_score, trend, bias, risk, summary, top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='setup' AND user_id=%s
            ORDER BY date DESC, created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"insight": None}

        avg, trend, bias, risk, summary, top, d, created = row
        top = json.loads(top) if isinstance(top, str) else (top or [])

        return {
            "insight": {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top,
                "date": d.isoformat() if d else None,
                "created_at": created.isoformat() if created else None,
            }
        }
    finally:
        conn.close()


@router.get("/agents/reflections/setup")
async def get_setup_reflections(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='setup' AND user_id=%s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10
        """, (user_id,))
        rows = cur.fetchall()
        return {
            "reflections": [
                {
                    "indicator": i,
                    "raw_score": float(rs) if rs is not None else None,
                    "ai_score": float(ai) if ai is not None else None,
                    "compliance": float(c) if c is not None else None,
                    "comment": cm,
                    "recommendation": r,
                    "date": d.isoformat() if d else None,
                    "timestamp": ts.isoformat() if ts else None,
                }
                for (i, rs, ai, c, cm, r, d, ts) in rows
            ]
        }
    finally:
        conn.close()


# ============================================================
# ========== STRATEGY ========================================
# ============================================================

@router.get("/agents/insights/strategy")
async def get_strategy_insight(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT avg_score, trend, bias, risk, summary, top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='strategy' AND user_id=%s
            ORDER BY date DESC, created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if not row:
            return {"insight": None}

        avg, trend, bias, risk, summary, top, d, created = row
        top = json.loads(top) if isinstance(top, str) else (top or [])

        return {
            "insight": {
                "score": float(avg) if avg is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top,
                "date": d.isoformat() if d else None,
                "created_at": created.isoformat() if created else None,
            }
        }
    finally:
        conn.close()


@router.get("/agents/reflections/strategy")
async def get_strategy_reflections(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='strategy' AND user_id=%s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10
        """, (user_id,))
        rows = cur.fetchall()
        return {
            "reflections": [
                {
                    "indicator": i,
                    "raw_score": float(rs) if rs is not None else None,
                    "ai_score": float(ai) if ai is not None else None,
                    "compliance": float(c) if c is not None else None,
                    "comment": cm,
                    "recommendation": r,
                    "date": d.isoformat() if d else None,
                    "timestamp": ts.isoformat() if ts else None,
                }
                for (i, rs, ai, c, cm, r, d, ts) in rows
            ]
        }
    finally:
        conn.close()


# ============================================================
# ========== TASK STATUS (CELERY) ============================
# ============================================================

@router.get("/tasks/{task_id}")
async def get_task_status(
    task_id: str,
    current_user: dict = Depends(get_current_user),
):
    try:
        result = AsyncResult(task_id, app=celery_app)

        response = {
            "task_id": task_id,
            "state": result.state,
        }

        if result.state == "SUCCESS":
            response["result"] = result.result
        elif result.state == "FAILURE":
            response["error"] = str(result.result)

        return response

    except Exception:
        logger.exception("Task status fout")
        raise HTTPException(status_code=500, detail="Task status ophalen mislukt")

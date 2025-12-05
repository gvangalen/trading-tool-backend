import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends

from backend.utils.db import get_db_connection
from backend.utils.auth_utils import get_current_user  # 🔐 user uit token

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
# ==========  MACRO  =========================================
# ============================================================

@router.get("/agents/insights/macro")
async def get_macro_insight(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [macro] Insight ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category = 'macro'
              AND user_id = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """, (user_id,))

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk, summary,
         top_signals_raw, d, created_at) = row

        # Parsing
        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except Exception:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/macro")
async def get_macro_reflections(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [macro] Reflecties ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category = 'macro'
              AND user_id = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """, (user_id,))

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  MARKET  ========================================
# ============================================================

@router.get("/agents/insights/market")
async def get_market_insight(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [market] Insight ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category = 'market'
              AND user_id = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """, (user_id,))

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except Exception:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/market")
async def get_market_reflections(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [market] Reflecties ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category = 'market'
              AND user_id = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """, (user_id,))

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  TECHNICAL  =====================================
# ============================================================

@router.get("/agents/insights/technical")
async def get_technical_insight(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [technical] Insight ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category = 'technical'
              AND user_id = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """, (user_id,))

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except Exception:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/technical")
async def get_technical_reflections(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [technical] Reflecties ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category = 'technical'
              AND user_id = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """, (user_id,))

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  SETUP  =========================================
# ============================================================

@router.get("/agents/insights/setup")
async def get_setup_insight(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [setup] Insight ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category = 'setup'
              AND user_id = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """, (user_id,))

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except Exception:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/setup")
async def get_setup_reflections(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [setup] Reflecties ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category = 'setup'
              AND user_id = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """, (user_id,))

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  STRATEGY  ======================================
# ============================================================

@router.get("/agents/insights/strategy")
async def get_strategy_insight(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [strategy] Insight ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category = 'strategy'
              AND user_id = %s
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """, (user_id,))

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (
            _, avg_score, trend, bias, risk, summary,
            top_signals_raw, d, created_at
        ) = row

        # JSON parse top_signals
        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except Exception:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score is not None else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None,
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/strategy")
async def get_strategy_reflections(current_user: dict = Depends(get_current_user)):
    logger.info("📡 [strategy] Reflecties ophalen")
    user_id = current_user["id"]

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category = 'strategy'
              AND user_id = %s
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """, (user_id,))

        rows = cur.fetchall()
        reflections = []

        for (
            indicator, raw_score, ai_score, compliance,
            comment, recommendation, d, ts
        ) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score is not None else None,
                "ai_score": float(ai_score) if ai_score is not None else None,
                "compliance": float(compliance) if compliance is not None else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()

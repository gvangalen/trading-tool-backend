import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException

from backend.utils.db import get_db_connection

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
async def get_macro_insight():
    logger.info("📡 [macro] Insight ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='macro'
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """)

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk, summary,
         top_signals_raw, d, created_at) = row

        # Parsing
        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score else None,
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
async def get_macro_reflections():
    logger.info("📡 [macro] Reflecties ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='macro'
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score else None,
                "ai_score": float(ai_score) if ai_score else None,
                "compliance": float(compliance) if compliance else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  MARKET  =========================================
# ============================================================

@router.get("/agents/insights/market")
async def get_market_insight():
    logger.info("📡 [market] Insight ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='market'
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """)

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat(),
                "created_at": created_at.isoformat(),
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/market")
async def get_market_reflections():
    logger.info("📡 [market] Reflecties ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='market'
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score else None,
                "ai_score": float(ai_score) if ai_score else None,
                "compliance": float(compliance) if compliance else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat(),
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  TECHNICAL  =====================================
# ============================================================

@router.get("/agents/insights/technical")
async def get_technical_insight():
    logger.info("📡 [technical] Insight ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='technical'
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """)

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat(),
                "created_at": created_at.isoformat(),
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/technical")
async def get_technical_reflections():
    logger.info("📡 [technical] Reflecties ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='technical'
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score else None,
                "ai_score": float(ai_score) if ai_score else None,
                "compliance": float(compliance) if compliance else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat(),
                "timestamp": ts.isoformat(),
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  SETUP  =========================================
# ============================================================

@router.get("/agents/insights/setup")
async def get_setup_insight():
    logger.info("📡 [setup] Insight ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='setup'
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """)

        row = cur.fetchone()
        if not row:
            return {"insight": None}

        (_, avg_score, trend, bias, risk,
         summary, top_signals_raw, d, created_at) = row

        if isinstance(top_signals_raw, str):
            try:
                top_signals = json.loads(top_signals_raw)
            except:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat(),
                "created_at": created_at.isoformat(),
            }
        }

    finally:
        conn.close()


@router.get("/agents/reflections/setup")
async def get_setup_reflections():
    logger.info("📡 [setup] Reflecties ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='setup'
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()
        reflections = []

        for (indicator, raw_score, ai_score, compliance,
             comment, recommendation, d, ts) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score else None,
                "ai_score": float(ai_score) if ai_score else None,
                "compliance": float(compliance) if compliance else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat(),
                "timestamp": ts.isoformat(),
            })

        return {"reflections": reflections}

    finally:
        conn.close()


# ============================================================
# ==========  STRATEGY  =========================================
# ============================================================

@router.get("/agents/insights/strategy")
async def get_strategy_insight():
    logger.info("📡 [strategy] Insight ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT category, avg_score, trend, bias, risk, summary,
                   top_signals, date, created_at
            FROM ai_category_insights
            WHERE category='strategy'
            ORDER BY date DESC, created_at DESC
            LIMIT 1;
        """)

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
            except:
                top_signals = []
        else:
            top_signals = top_signals_raw or []

        return {
            "insight": {
                "score": float(avg_score) if avg_score else None,
                "trend": trend,
                "bias": bias,
                "risk": risk,
                "summary": summary,
                "top_signals": top_signals,
                "date": d.isoformat() if d else None,
                "created_at": created_at.isoformat() if created_at else None
            }
        }

    finally:
        conn.close()

@router.get("/agents/reflections/strategy")
async def get_strategy_reflections():
    logger.info("📡 [strategy] Reflecties ophalen")

    conn, cur = get_conn_cursor()
    try:
        cur.execute("""
            SELECT indicator, raw_score, ai_score, compliance,
                   comment, recommendation, date, timestamp
            FROM ai_reflections
            WHERE category='strategy'
            ORDER BY date DESC, timestamp DESC
            LIMIT 10;
        """)

        rows = cur.fetchall()
        reflections = []

        for (
            indicator, raw_score, ai_score, compliance,
            comment, recommendation, d, ts
        ) in rows:

            reflections.append({
                "indicator": indicator,
                "raw_score": float(raw_score) if raw_score else None,
                "ai_score": float(ai_score) if ai_score else None,
                "compliance": float(compliance) if compliance else None,
                "comment": comment,
                "recommendation": recommendation,
                "date": d.isoformat() if d else None,
                "timestamp": ts.isoformat() if ts else None,
            })

        return {"reflections": reflections}

    finally:
        conn.close()

import logging
import json
from celery import shared_task
from datetime import date

from backend.utils.db import get_db_connection
from backend.ai_agents.strategy_ai_agent import (
    generate_strategy_from_setup,
    analyze_strategies,
    adjust_strategy_for_today,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================
# 🔹 Load setup (STRICT volgens DB schema)
# ============================================================
def load_setup_from_db(setup_id: int, user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    name,
                    symbol,
                    timeframe,
                    strategy_type,
                    description,
                    filters
                FROM setups
                WHERE id = %s AND user_id = %s
                LIMIT 1;
            """, (setup_id, user_id))

            row = cur.fetchone()
            if not row:
                raise ValueError("Setup niet gevonden")

            return {
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "strategy_type": row[4],
                "description": row[5],
                "filters": row[6],
            }
    finally:
        conn.close()


# ============================================================
# 🔹 Load LAATSTE strategy voor setup
# ============================================================
def load_latest_strategy(setup_id: int, user_id: int) -> dict | None:
    conn = get_db_connection()
    if not conn:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    entry,
                    target,
                    stop_loss,
                    risk_reward,
                    explanation,
                    data,
                    created_at
                FROM strategies
                WHERE setup_id = %s AND user_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (setup_id, user_id))

            row = cur.fetchone()
            if not row:
                return None

            return {
                "strategy_id": row[0],
                "entry": row[1],
                "targets": row[2].split(",") if row[2] else [],
                "stop_loss": row[3],
                "risk_reward": row[4],
                "explanation": row[5],
                "data": row[6] or {},
                "created_at": row[7].isoformat() if row[7] else None,
            }
    finally:
        conn.close()


# ============================================================
# 🚀 INITIËLE STRATEGY GENERATIE
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int):
    logger.info(f"🚀 Strategy generatie | user={user_id} setup={setup_id}")
    conn = None

    try:
        setup = load_setup_from_db(setup_id, user_id)
        strategy = generate_strategy_from_setup(setup)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO strategies (
                    setup_id,
                    entry,
                    target,
                    stop_loss,
                    explanation,
                    risk_reward,
                    strategy_type,
                    data,
                    user_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                RETURNING id;
            """, (
                setup_id,
                strategy.get("entry"),
                ",".join(map(str, strategy.get("targets", []))),
                strategy.get("stop_loss"),
                strategy.get("explanation"),
                strategy.get("risk_reward"),
                setup.get("strategy_type"),
                json.dumps(strategy),
                user_id,
            ))

            strategy_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"✅ Strategy opgeslagen (id={strategy_id})")
        return {"success": True, "strategy_id": strategy_id}

    except Exception:
        logger.error("❌ Strategy generatie fout", exc_info=True)
        return {"success": False}

    finally:
        if conn:
            conn.close()


# ============================================================
# 🧠 ANALYSE BESTAANDE STRATEGY (AI)
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.analyze_strategy")
def analyze_strategy(user_id: int, strategy_id: int):
    logger.info(f"🧠 Analyse strategy | user={user_id} strategy={strategy_id}")
    conn = get_db_connection()
    if not conn:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, setup_id, entry, target, stop_loss,
                    risk_reward, explanation, data, created_at
                FROM strategies
                WHERE id = %s AND user_id = %s;
            """, (strategy_id, user_id))

            row = cur.fetchone()
        if not row:
            return

        payload = [{
            "strategy_id": row[0],
            "setup_id": row[1],
            "entry": row[2],
            "targets": row[3].split(",") if row[3] else [],
            "stop_loss": row[4],
            "risk_reward": row[5],
            "explanation": row[6],
            "data": row[7],
            "created_at": row[8].isoformat() if row[8] else None,
        }]

        analysis = analyze_strategies(payload)
        if not analysis:
            return

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies
                SET data = jsonb_set(
                    COALESCE(data,'{}'::jsonb),
                    '{ai_analysis}',
                    to_jsonb(%s),
                    true
                )
                WHERE id = %s AND user_id = %s;
            """, (analysis, strategy_id, user_id))

        conn.commit()
        logger.info("✅ Strategy analyse opgeslagen")

    finally:
        conn.close()


# ============================================================
# 🟡 DAGELIJKSE STRATEGY SNAPSHOT + DASHBOARD INSIGHT
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.run_daily_strategy_snapshot")
def run_daily_strategy_snapshot(user_id: int):
    logger.info(f"🟡 Daily strategy snapshot | user={user_id}")
    conn = get_db_connection()

    try:
        # 1️⃣ Best-of-day setup
        with conn.cursor() as cur:
            cur.execute("""
                SELECT setup_id
                FROM daily_setup_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                  AND is_best = TRUE
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen best-of-day setup")
            return

        setup_id = row[0]

        # 2️⃣ Market context
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
            """, (user_id,))
            scores = cur.fetchone()

        if not scores:
            return

        market_context = {
            "macro_score": scores[0],
            "technical_score": scores[1],
            "market_score": scores[2],
        }

        setup = load_setup_from_db(setup_id, user_id)
        base_strategy = load_latest_strategy(setup_id, user_id)
        if not base_strategy:
            return

        adjustment = adjust_strategy_for_today(
            base_strategy=base_strategy,
            setup=setup,
            market_context=market_context,
        )

        if not adjustment or not adjustment.get("entry") or not adjustment.get("targets"):
            logger.warning("⚠️ Onvolledige strategy adjustment — snapshot overgeslagen")
            return

        # 3️⃣ Snapshot opslaan
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO active_strategy_snapshot (
                    user_id,
                    setup_id,
                    strategy_id,
                    snapshot_date,
                    entry,
                    targets,
                    stop_loss,
                    adjustment_reason,
                    confidence_score,
                    market_context,
                    changes
                ) VALUES (
                    %s,%s,%s,CURRENT_DATE,
                    %s,%s,%s,
                    %s,%s,
                    %s::jsonb,
                    %s::jsonb
                )
                ON CONFLICT (user_id, setup_id, snapshot_date)
                DO UPDATE SET
                    entry = EXCLUDED.entry,
                    targets = EXCLUDED.targets,
                    stop_loss = EXCLUDED.stop_loss,
                    adjustment_reason = EXCLUDED.adjustment_reason,
                    confidence_score = EXCLUDED.confidence_score,
                    market_context = EXCLUDED.market_context,
                    changes = EXCLUDED.changes,
                    created_at = NOW();
            """, (
                user_id,
                setup_id,
                base_strategy["strategy_id"],
                adjustment.get("entry"),
                ",".join(map(str, adjustment.get("targets", []))),
                adjustment.get("stop_loss"),
                adjustment.get("adjustment_reason"),
                adjustment.get("confidence_score"),
                json.dumps(market_context),
                json.dumps(adjustment.get("changes")),
            ))

        # 4️⃣ STRATEGY AI INSIGHT (DASHBOARD)
        analysis = analyze_strategies([{
            "setup_id": setup_id,
            "strategy_id": base_strategy["strategy_id"],
            "entry": adjustment["entry"],
            "targets": adjustment["targets"],
            "stop_loss": adjustment["stop_loss"],
            "confidence_score": adjustment.get("confidence_score"),
        }])

        if analysis:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ai_category_insights (
                        category,
                        user_id,
                        avg_score,
                        trend,
                        bias,
                        risk,
                        summary,
                        top_signals,
                        date
                    ) VALUES (
                        'strategy',
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s::jsonb,
                        CURRENT_DATE
                    )
                    ON CONFLICT (user_id, category, date)
                    DO UPDATE SET
                        avg_score = EXCLUDED.avg_score,
                        summary = EXCLUDED.summary,
                        top_signals = EXCLUDED.top_signals,
                        created_at = NOW();
                """, (
                    user_id,
                    adjustment.get("confidence_score", 50),
                    "Actief" if adjustment.get("confidence_score", 0) >= 60 else "Neutraal",
                    "Kansrijk" if adjustment.get("confidence_score", 0) >= 60 else "Afwachten",
                    "Gemiddeld",
                    analysis["comment"],
                    json.dumps([analysis["recommendation"]]),
                ))

        conn.commit()
        logger.info("✅ Daily strategy snapshot + AI insight opgeslagen")

    except Exception:
        logger.error("❌ Daily strategy snapshot fout", exc_info=True)

    finally:
        conn.close()


# ============================================================
# 🔄 BULK GENERATIE — BEWUST UIT
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_all")
def generate_all(user_id: int):
    return {
        "state": "IGNORED",
        "success": False,
        "reason": "Bulk AI strategie-generatie is uitgeschakeld",
    }

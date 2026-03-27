import logging
import json
from datetime import date
from typing import Any, Dict, Optional

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.ai_agents.strategy_ai_agent import (
    generate_strategy_from_setup,
    analyze_strategies,
    adjust_strategy_for_today,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# 🔧 Helpers
# ============================================================
def safe_json(value: Any) -> Dict[str, Any]:
    """Zorgt dat 'data' altijd een dict is."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def safe_numeric(value: Any) -> Optional[float]:
    """
    Probeert AI-output om te zetten naar numeric voor DB.
    Accepteert:
    - 42500
    - 42500.5
    - "42500"
    - "42,500"
    - "42500 - 43000" (pakt eerste getal)
    Alles anders -> None
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            s = value.strip().replace(",", ".")
            s = s.split()[0]
            s = s.split("-")[0]
            s = s.replace("..", ".")
            return float(s)
        except Exception:
            return None

    return None


def safe_confidence(value: Any, fallback: int = 50) -> float:
    try:
        v = float(value)
        if v < 0:
            return 0.0
        if v > 100:
            return 100.0
        return v
    except Exception:
        return float(fallback)


def _get_strategy_columns(conn) -> set:
    """Check welke kolommen bestaan in de strategies tabel (schema-proof)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='strategies';
            """
        )
        return {r[0] for r in cur.fetchall()}


# ============================================================
# 🔹 Load setup (STRICT volgens DB schema)
# ============================================================
def load_setup_from_db(setup_id: int, user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    name,
                    symbol,
                    timeframe,
                    setup_type,
                    description,
                    filters
                FROM setups
                WHERE id = %s AND user_id = %s
                LIMIT 1;
                """,
                (setup_id, user_id),
            )

            row = cur.fetchone()
            if not row:
                raise ValueError("Setup niet gevonden")

            return {
                "id": row[0],
                "name": row[1],
                "symbol": row[2],
                "timeframe": row[3],
                "setup_type": row[4],  # ✅ NIEUW
                "description": row[5],
                "filters": row[6],
            }
    finally:
        conn.close()


# ============================================================
# 🔹 Load LAATSTE strategy voor setup (schema-proof)
# ============================================================
def load_latest_strategy(setup_id: int, user_id: int) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cols = _get_strategy_columns(conn)

        select_fields = [
            "id",
            "entry",
            "targets",
            "stop_loss",
            "explanation",
            "data",
            "created_at",
        ]

        if "risk_reward" in cols:
            select_fields.insert(4, "risk_reward")

        query = f"""
            SELECT {", ".join(select_fields)}
            FROM strategies
            WHERE setup_id = %s AND user_id = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """

        with conn.cursor() as cur:
            cur.execute(query, (setup_id, user_id))
            row = cur.fetchone()

        if not row:
            return None

        row_map = dict(zip(select_fields, row))
        targets = row_map.get("targets") or []

        result = {
            "strategy_id": row_map.get("id"),
            "entry": float(row_map["entry"]) if row_map.get("entry") is not None else None,
            "targets": [float(t) for t in targets],
            "stop_loss": float(row_map["stop_loss"]) if row_map.get("stop_loss") is not None else None,
            "explanation": row_map.get("explanation"),
            "data": safe_json(row_map.get("data")),
            "created_at": row_map.get("created_at").isoformat()
            if row_map.get("created_at")
            else None,
        }

        if "risk_reward" in row_map:
            result["risk_reward"] = row_map.get("risk_reward")

        return result

    finally:
        conn.close()


# ============================================================
# 🚀 INITIËLE STRATEGY GENERATIE
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.generate_for_setup")
def generate_for_setup(user_id: int, setup_id: int):

    logger.info("🚀 Strategy generatie | user=%s setup=%s", user_id, setup_id)
    conn = None

    try:
        setup = load_setup_from_db(setup_id, user_id)
        strategy = generate_strategy_from_setup(setup)

        conn = get_db_connection()
        if not conn:
            raise RuntimeError("Geen databaseverbinding")

        cols = _get_strategy_columns(conn)
        has_risk_reward = "risk_reward" in cols

        # ❌ strategy_type volledig weg
        insert_cols = [
            "setup_id",
            "entry",
            "targets",
            "stop_loss",
            "explanation",
            "data",
            "user_id",
        ]

        if has_risk_reward:
            insert_cols.insert(5, "risk_reward")

        placeholders = ", ".join(["%s"] * len(insert_cols))

        targets = strategy.get("targets") or []
        targets = [safe_numeric(t) for t in targets if safe_numeric(t) is not None]

        # 🔥 setup_type opslaan in data (BELANGRIJK)
        enriched_data = {
            **strategy,
            "setup_type": setup.get("setup_type"),
        }

        values = [
            setup_id,
            safe_numeric(strategy.get("entry")),
            targets,
            safe_numeric(strategy.get("stop_loss")),
            strategy.get("explanation"),
        ]

        if has_risk_reward:
            values.append(strategy.get("risk_reward"))

        values.extend([
            json.dumps(enriched_data),  # ✅ FIX
            user_id,
        ])

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO strategies ({", ".join(insert_cols)})
                VALUES ({placeholders})
                RETURNING id;
                """,
                tuple(values),
            )
            strategy_id = cur.fetchone()[0]
            conn.commit()

        logger.info("✅ Strategy opgeslagen (id=%s)", strategy_id)

        return {
            "success": True,
            "strategy_id": strategy_id,
        }

    except Exception:
        logger.error("❌ Strategy generatie fout", exc_info=True)

        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        return {"success": False}

    finally:
        if conn:
            conn.close()


# ============================================================
# 🧠 ANALYSE BESTAANDE STRATEGY (AI)
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.analyze_strategy")
def analyze_strategy(user_id: int, strategy_id: int):

    logger.info("🧠 Analyse strategy | user=%s strategy=%s", user_id, strategy_id)

    conn = get_db_connection()
    if not conn:
        raise RuntimeError("Geen databaseverbinding")

    try:
        cols = _get_strategy_columns(conn)

        select_fields = [
            "id",
            "setup_id",
            "entry",
            "targets",
            "stop_loss",
            "explanation",
            "data",
            "created_at",
        ]

        if "risk_reward" in cols:
            select_fields.insert(5, "risk_reward")

        query = f"""
            SELECT {", ".join(select_fields)}
            FROM strategies
            WHERE id = %s AND user_id = %s;
        """

        with conn.cursor() as cur:
            cur.execute(query, (strategy_id, user_id))
            row = cur.fetchone()

        if not row:
            raise ValueError("Strategy niet gevonden")

        row_map = dict(zip(select_fields, row))

        payload = [
            {
                "strategy_id": row_map["id"],
                "setup_id": row_map["setup_id"],
                "entry": row_map["entry"],
                "targets": row_map.get("targets") or [],
                "stop_loss": row_map["stop_loss"],
                "risk_reward": row_map.get("risk_reward"),
                "explanation": row_map["explanation"],
                "data": safe_json(row_map["data"]),
                "created_at": row_map["created_at"].isoformat()
                if row_map["created_at"] else None,
            }
        ]

        analysis = analyze_strategies(
            user_id=user_id,
            strategies=payload,
        )

        if not analysis:
            raise RuntimeError("AI analyse gaf None terug")

        explanation_text = (
            f"{analysis.get('comment', '')}\n\n"
            f"{analysis.get('recommendation', '')}"
        ).strip()

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE strategies
                SET data = jsonb_set(
                    COALESCE(data,'{}'::jsonb),
                    '{ai_explanation}',
                    %s::jsonb,
                    true
                )
                WHERE id = %s AND user_id = %s;
                """,
                (json.dumps(explanation_text), strategy_id, user_id),
            )

        conn.commit()

        logger.info("✅ Strategy AI explanation opgeslagen")

        return {"success": True}

    except Exception:
        logger.exception("❌ analyze_strategy crash")
        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# 🧠 Run DCA-Strategy Snapshot
# ============================================================
def run_dca_strategy_snapshot(user_id: int, setup: dict):
    logger.info("🟢 DCA snapshot gestart | user=%s setup=%s", user_id, setup["id"])

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    today = date.today()

    try:
        # 1️⃣ Scores ophalen
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1;
                """,
                (user_id,),
            )
            scores = cur.fetchone()

        if not scores:
            logger.warning("⚠️ Geen daily_scores gevonden")
            return

        market_context = {
            "macro_score": float(scores[0]) if scores[0] is not None else None,
            "technical_score": float(scores[1]) if scores[1] is not None else None,
            "market_score": float(scores[2]) if scores[2] is not None else None,
        }

        base_strategy = load_latest_strategy(setup["id"], user_id)
        if not base_strategy:
            logger.warning("⚠️ Geen base strategy gevonden")
            return

        adjustment = adjust_strategy_for_today(
            user_id=user_id,
            base_strategy=base_strategy,
            setup=setup,
            market_context=market_context,
        )

        if not adjustment:
            logger.warning("⚠️ Geen AI adjustment")
            return

        confidence = safe_confidence(
            adjustment.get("confidence_score"),
            fallback=50,
        )

        # ❌ strategy_type eruit
        # ✅ setup_type erin
        analysis = analyze_strategies(
            user_id=user_id,
            strategies=[
                {
                    "strategy_id": base_strategy["strategy_id"],
                    "setup_id": setup["id"],
                    "setup_type": setup.get("setup_type"),  # 🔥 FIX
                    "confidence_score": confidence,
                    "market_context": market_context,
                    "adjustment_reason": adjustment.get("adjustment_reason"),
                }
            ],
        )

        if not analysis:
            logger.warning("⚠️ Geen AI analyse")
            return

        with conn.cursor() as cur:
            entry = safe_numeric(base_strategy.get("entry"))
            stop = safe_numeric(base_strategy.get("stop_loss"))
            targets = base_strategy.get("targets") or []
            targets_text = json.dumps(targets) if targets else None

            cur.execute(
                """
                INSERT INTO active_strategy_snapshot (
                    user_id,
                    strategy_id,
                    setup_id,
                    entry,
                    stop_loss,
                    targets,
                    confidence_score,
                    adjustment_reason,
                    market_context,
                    changes,
                    snapshot_date
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb,
                    %s::jsonb,
                    %s
                )
                ON CONFLICT (user_id, setup_id, snapshot_date)
                DO UPDATE SET
                    strategy_id = EXCLUDED.strategy_id,
                    entry = EXCLUDED.entry,
                    stop_loss = EXCLUDED.stop_loss,
                    targets = EXCLUDED.targets,
                    confidence_score = EXCLUDED.confidence_score,
                    adjustment_reason = EXCLUDED.adjustment_reason,
                    market_context = EXCLUDED.market_context,
                    changes = EXCLUDED.changes,
                    created_at = NOW();
                """,
                (
                    user_id,
                    base_strategy["strategy_id"],
                    setup["id"],
                    entry,
                    stop,
                    targets_text,
                    confidence,
                    adjustment.get("adjustment_reason"),
                    json.dumps(market_context),
                    json.dumps(adjustment),
                    today,
                ),
            )

        conn.commit()
        logger.info("✅ DCA snapshot opgeslagen")

    except Exception:
        logger.error("❌ DCA snapshot fout", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()

# ============================================================
# 🟡 DAGELIJKSE STRATEGY SNAPSHOT + DASHBOARD INSIGHT
# ============================================================
@shared_task(name="backend.celery_task.strategy_task.run_daily_strategy_snapshot")
def run_daily_strategy_snapshot(user_id: int):

    logger.info("🟡 Daily strategy snapshot | user=%s", user_id)
    today = date.today()

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    try:
        # ----------------------------------------------------
        # 1️⃣ BEST SETUP
        # ----------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT setup_id
                FROM daily_setup_scores
                WHERE user_id = %s
                  AND report_date = %s
                  AND is_best = TRUE
                LIMIT 1;
                """,
                (user_id, today),
            )
            row = cur.fetchone()

        if not row:
            logger.warning("⚠️ Geen best-of-day setup")
            return

        setup_id = row[0]
        setup = load_setup_from_db(setup_id, user_id)

        logger.info("📌 Best setup gevonden | id=%s type=%s", setup_id, setup.get("setup_type"))

        # ----------------------------------------------------
        # 2️⃣ STRATEGY
        # ----------------------------------------------------
        base_strategy = load_latest_strategy(setup_id, user_id)

        setup_type = (setup.get("setup_type") or "").lower()

        # 🔥 FIX → DCA hoeft geen levels
        if setup_type == "dca":
            needs_bootstrap = False
        else:
            needs_bootstrap = (
                not base_strategy
                or base_strategy.get("entry") is None
                or base_strategy.get("stop_loss") is None
                or not base_strategy.get("targets")
            )

        # ----------------------------------------------------
        # 3️⃣ BOOTSTRAP
        # ----------------------------------------------------
        if needs_bootstrap:

            logger.warning("⚠️ Strategy ontbreekt → bootstrap")

            strategy = generate_strategy_from_setup(setup)

            entry = safe_numeric(strategy.get("entry"))
            stop = safe_numeric(strategy.get("stop_loss"))

            targets = strategy.get("targets") or []
            targets = [safe_numeric(t) for t in targets if safe_numeric(t) is not None]

            with conn.cursor() as cur:

                if not base_strategy:
                    cur.execute(
                        """
                        INSERT INTO strategies (
                            setup_id,
                            entry,
                            targets,
                            stop_loss,
                            explanation,
                            setup_type,
                            data,
                            user_id
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                        """,
                        (
                            setup_id,
                            entry,
                            targets,
                            stop,
                            strategy.get("explanation"),
                            setup_type,  # 🔥 FIX
                            json.dumps(strategy),
                            user_id,
                        ),
                    )
                    strategy_id = cur.fetchone()[0]

                else:
                    strategy_id = base_strategy["strategy_id"]

                    cur.execute(
                        """
                        UPDATE strategies
                        SET entry=%s, stop_loss=%s, targets=%s
                        WHERE id=%s
                        """,
                        (entry, stop, targets, strategy_id),
                    )

            conn.commit()

            base_strategy = {
                "strategy_id": strategy_id,
                "entry": entry,
                "stop_loss": stop,
                "targets": targets,
            }

            logger.info(
                "✅ Bootstrap gedaan | entry=%s stop=%s targets=%s",
                entry, stop, targets
            )

        # ----------------------------------------------------
        # 4️⃣ MARKET CONTEXT
        # ----------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = %s
                LIMIT 1;
                """,
                (user_id, today),
            )
            scores = cur.fetchone()

        if not scores:
            logger.warning("⚠️ Geen daily_scores")
            return

        market_context = {
            "macro_score": float(scores[0]) if scores[0] else None,
            "technical_score": float(scores[1]) if scores[1] else None,
            "market_score": float(scores[2]) if scores[2] else None,
        }

        logger.info("📊 Market context: %s", market_context)

        # ----------------------------------------------------
        # 5️⃣ AI ANALYSE
        # ----------------------------------------------------
        analysis = analyze_strategies(
            user_id=user_id,
            strategies=[
                {
                    "strategy_id": base_strategy["strategy_id"],
                    "setup_id": setup_id,
                    "setup_type": setup_type,  # 🔥 FIX
                    "entry": base_strategy.get("entry"),
                    "targets": base_strategy.get("targets"),
                    "stop_loss": base_strategy.get("stop_loss"),
                    "market_context": market_context,
                }
            ],
        )

        if not analysis:
            logger.warning("⚠️ AI analyse None")
            return

        logger.info("🧠 AI analyse OK")

        # ----------------------------------------------------
        # 6️⃣ SNAPSHOT
        # ----------------------------------------------------
        with conn.cursor() as cur:

            entry = safe_numeric(base_strategy.get("entry"))
            stop = safe_numeric(base_strategy.get("stop_loss"))

            targets = base_strategy.get("targets") or []
            targets_text = json.dumps(targets) if targets else None

            confidence = safe_confidence(
                analysis.get("confidence_score"),
                fallback=50,
            )

            cur.execute(
                """
                INSERT INTO active_strategy_snapshot (
                    user_id,
                    strategy_id,
                    setup_id,
                    entry,
                    stop_loss,
                    targets,
                    confidence_score,
                    adjustment_reason,
                    market_context,
                    changes,
                    snapshot_date
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s)
                ON CONFLICT (user_id, setup_id, snapshot_date)
                DO UPDATE SET
                    strategy_id = EXCLUDED.strategy_id,
                    entry = EXCLUDED.entry,
                    stop_loss = EXCLUDED.stop_loss,
                    targets = EXCLUDED.targets,
                    confidence_score = EXCLUDED.confidence_score,
                    adjustment_reason = EXCLUDED.adjustment_reason,
                    market_context = EXCLUDED.market_context,
                    changes = EXCLUDED.changes,
                    created_at = NOW();
                """,
                (
                    user_id,
                    base_strategy["strategy_id"],
                    setup_id,
                    entry,
                    stop,
                    targets_text,
                    confidence,
                    analysis.get("recommendation"),
                    json.dumps(market_context),
                    json.dumps(analysis),
                    today,
                ),
            )

        conn.commit()

        logger.info("✅ Snapshot opgeslagen (strategy_id=%s)", base_strategy["strategy_id"])

    except Exception:
        logger.exception("❌ Daily strategy snapshot crash")
        conn.rollback()
        raise

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


def debug_analyze_strategy(user_id: int, strategy_id: int):
    """
    Debug helper zonder Celery async gedrag.
    Roept de task-functie direct aan.
    """
    return analyze_strategy(user_id=user_id, strategy_id=strategy_id)

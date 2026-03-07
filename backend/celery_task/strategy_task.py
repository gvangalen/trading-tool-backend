import logging
import json
from datetime import date
from typing import Any, Dict, List, Optional

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
            # Pak eerste getal uit ranges/teksten
            s = value.strip().replace(",", ".")
            s = s.split()[0]           # eerste token
            s = s.split("-")[0]        # eerste deel van range
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
                    strategy_type,
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
                "strategy_type": row[4],
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

    logger.info(f"🚀 Strategy generatie | user={user_id} setup={setup_id}")
    conn = None

    try:
        setup = load_setup_from_db(setup_id, user_id)
        strategy = generate_strategy_from_setup(setup)

        conn = get_db_connection()
        if not conn:
            raise RuntimeError("Geen databaseverbinding")

        cols = _get_strategy_columns(conn)

        has_risk_reward = "risk_reward" in cols

        insert_cols = [
            "setup_id",
            "entry",
            "targets",
            "stop_loss",
            "explanation",
            "strategy_type",
            "data",
            "user_id",
        ]

        if has_risk_reward:
            insert_cols.insert(5, "risk_reward")

        placeholders = ", ".join(["%s"] * len(insert_cols))

        targets = strategy.get("targets") or []
        targets = [safe_numeric(t) for t in targets if safe_numeric(t) is not None]

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
            setup.get("strategy_type"),
            json.dumps(strategy),
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

        logger.info(f"✅ Strategy opgeslagen (id={strategy_id})")

        return {
            "success": True,
            "strategy_id": strategy_id
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

    logger.info(f"🧠 Analyse strategy | user={user_id} strategy={strategy_id}")

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
            strategies=payload
        )

        if not analysis:
            raise RuntimeError("AI analyse gaf None terug")

        explanation_text = (
            f"{analysis.get('comment','')}\n\n"
            f"{analysis.get('recommendation','')}"
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
    logger.info(f"🟢 DCA snapshot gestart | user={user_id} setup={setup['id']}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    try:
        # ==================================================
        # 1️⃣ Scores van vandaag ophalen
        # ==================================================
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

        # ==================================================
        # 2️⃣ Laatste bestaande strategy laden
        # ==================================================
        base_strategy = load_latest_strategy(setup["id"], user_id)
        if not base_strategy:
            logger.warning("⚠️ Geen base strategy gevonden voor DCA setup")
            return

        # ==================================================
        # 3️⃣ Subtiele AI-aanpassing (🔥 user_id FIX)
        # ==================================================
        adjustment = adjust_strategy_for_today(
            user_id=user_id,  # 🔴 CRUCIAAL — dit was de bug
            base_strategy=base_strategy,
            setup=setup,
            market_context=market_context,
        )

        if not adjustment:
            logger.warning("⚠️ Geen AI adjustment ontvangen")
            return

        confidence = safe_confidence(
            adjustment.get("confidence_score"),
            fallback=50,
        )

        # ==================================================
        # 4️⃣ Strategy AI analyse (execution & discipline)
        # ==================================================
        analysis = analyze_strategies(
            user_id=user_id,
            strategies=[
                {
                    "strategy_id": base_strategy["strategy_id"],
                    "setup_id": setup["id"],
                    "strategy_type": "dca",
                    "confidence_score": confidence,
                    "market_context": market_context,
                    "adjustment_reason": adjustment.get("adjustment_reason"),
                }
            ],
        )

        if not analysis:
            logger.warning("⚠️ Geen AI analyse-resultaat")
            return

        # ==================================================
        # 5️⃣ AI Category Insight opslaan (dashboard kaart)
        # ==================================================
        with conn.cursor() as cur:
            cur.execute(
                """
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
                )
                VALUES (
                    'strategy',
                    %s, %s,
                    %s, %s, %s,
                    %s,
                    %s::jsonb,
                    CURRENT_DATE
                )
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
                """,
                (
                    user_id,
                    confidence,
                    "Actief" if confidence >= 60 else "Neutraal",
                    "Accumuleer" if confidence >= 60 else "Rustig",
                    "Laag",
                    analysis["comment"],
                    json.dumps(
                        [analysis["recommendation"]],
                        ensure_ascii=False,
                    ),
                ),
            )

        conn.commit()
        logger.info("✅ DCA strategy snapshot + AI insight opgeslagen")

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

    logger.info(f"🟡 Daily strategy snapshot (BEST-SETUP driven) | user={user_id}")
    today = date.today()

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return

    try:
        # ==================================================
        # 1️⃣ Beste setup van vandaag ophalen
        # ==================================================
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

        # ==================================================
        # 2️⃣ Strategy ophalen
        # ==================================================
        base_strategy = load_latest_strategy(setup_id, user_id)

        if not base_strategy:
            logger.info("ℹ️ Beste setup heeft nog geen strategy")

            # 👉 Hier slaan we een nette AI-melding op
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    )
                    VALUES (
                        'strategy',
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s::jsonb,
                        %s
                    )
                    ON CONFLICT (user_id, category, date)
                    DO UPDATE SET
                        summary = EXCLUDED.summary,
                        top_signals = EXCLUDED.top_signals,
                        updated_at = NOW();
                    """,
                    (
                        user_id,
                        0,
                        "Geen strategy",
                        "In afwachting",
                        "N.v.t.",
                        "De beste setup van vandaag heeft nog geen gekoppelde strategy. Voeg eerst een strategy toe om AI-analyse en execution mogelijk te maken.",
                        json.dumps(
                            ["Klik op 'Genereer strategy (AI)' of voeg handmatig een strategy toe."],
                            ensure_ascii=False,
                        ),
                        today,
                    ),
                )

            conn.commit()
            return

        # ==================================================
        # 3️⃣ Market context
        # ==================================================
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
            logger.warning("⚠️ Geen daily_scores gevonden")
            return

        market_context = {
            "macro_score": float(scores[0]) if scores[0] else None,
            "technical_score": float(scores[1]) if scores[1] else None,
            "market_score": float(scores[2]) if scores[2] else None,
        }

        # ==================================================
        # 4️⃣ AI analyse (GEEN bot, puur strategy-level)
        # ==================================================
        analysis = analyze_strategies(
            user_id=user_id,
            strategies=[
                {
                    "strategy_id": base_strategy["strategy_id"],
                    "setup_id": setup_id,
                    "entry": base_strategy.get("entry"),
                    "targets": base_strategy.get("targets"),
                    "stop_loss": base_strategy.get("stop_loss"),
                    "market_context": market_context,
                }
            ],
        )

        if not analysis:
            logger.warning("⚠️ AI analyse gaf None terug")
            return

        # ==================================================
        # 5️⃣ Insight opslaan
        # ==================================================
        with conn.cursor() as cur:
            cur.execute(
                """
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
                )
                VALUES (
                    'strategy',
                    %s, %s,
                    %s, %s, %s,
                    %s,
                    %s::jsonb,
                    %s
                )
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    updated_at = NOW();
                """,
                (
                    user_id,
                    50,
                    "Actief",
                    "Plan actief",
                    "Gemiddeld",
                    analysis.get("comment", ""),
                    json.dumps(
                        [analysis.get("recommendation", "")],
                        ensure_ascii=False,
                    ),
                    today,
                ),
            )

        conn.commit()
        logger.info("✅ Strategy AI insight opgeslagen")

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

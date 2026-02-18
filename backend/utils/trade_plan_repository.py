import json
from typing import Dict, Any


def save_trade_plan(
    conn,
    user_id: int,
    bot_id: int,
    decision_id: int,
    plan: Dict[str, Any],
) -> int:
    """
    Persist trade execution plan for a decision.

    Guarantees:
    ✔ idempotent (ON CONFLICT decision_id)
    ✔ safe JSON serialization
    ✔ timestamps maintained
    ✔ never crashes on missing fields
    """

    entry_plan = plan.get("entry_plan") or []
    stop_loss = plan.get("stop_loss") or {}
    targets = plan.get("targets") or []
    risk_json = plan.get("risk") or {}

    symbol = plan.get("symbol")
    side = plan.get("side")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_trade_plans (
                user_id,
                bot_id,
                decision_id,
                symbol,
                side,
                entry_plan,
                stop_loss,
                targets,
                risk_json,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
            ON CONFLICT (decision_id)
            DO UPDATE SET
                entry_plan = EXCLUDED.entry_plan,
                stop_loss  = EXCLUDED.stop_loss,
                targets    = EXCLUDED.targets,
                risk_json  = EXCLUDED.risk_json,
                updated_at = NOW()
            RETURNING id
            """,
            (
                user_id,
                bot_id,
                decision_id,
                symbol,
                side,
                json.dumps(entry_plan, default=float),
                json.dumps(stop_loss, default=float),
                json.dumps(targets, default=float),
                json.dumps(risk_json, default=float),
            ),
        )

        row = cur.fetchone()
        return int(row[0])

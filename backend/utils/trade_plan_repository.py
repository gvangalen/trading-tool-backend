def save_trade_plan(conn, user_id, bot_id, decision_id, plan):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bot_trade_plans (
                user_id, bot_id, decision_id,
                symbol, side,
                entry_plan, stop_loss, targets, risk_json
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (decision_id)
            DO UPDATE SET
                entry_plan = EXCLUDED.entry_plan,
                stop_loss  = EXCLUDED.stop_loss,
                targets    = EXCLUDED.targets,
                risk_json  = EXCLUDED.risk_json,
                updated_at = NOW()
        """, (
            user_id,
            bot_id,
            decision_id,
            plan["symbol"],
            plan["side"],
            json.dumps(plan["entry_plan"]),
            json.dumps(plan["stop_loss"]),
            json.dumps(plan["targets"]),
            json.dumps(plan.get("risk", {})),
        ))

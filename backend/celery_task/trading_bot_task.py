# backend/celery_task/trading_bot_task.py

import logging
from datetime import date
from typing import Optional

from celery import shared_task

from backend.ai_agents.trading_bot_agent import run_trading_bot_agent
from backend.services.portfolio_snapshot_service import snapshot_all_for_user
from backend.celery_task.strategy_task import run_daily_strategy_snapshot
from backend.utils.db import get_db_connection
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@shared_task(
    name="backend.celery_task.trading_bot_task.run_daily_trading_bot",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def run_trading_bot_agent(
    user_id: int,
    report_date: Optional[date] = None,
    bot_id: Optional[int] = None,
) -> Dict[str, Any]:

    report_date = report_date or date.today()

    conn = get_db_connection()
    if not conn:
        return {"ok": False, "error": "db_unavailable"}

    try:
        bots = _get_active_bots(conn, user_id)

        if bot_id is not None:
            bots = [b for b in bots if b["bot_id"] == bot_id]

        if not bots:
            return {
                "ok": True,
                "date": str(report_date),
                "bots": 0,
                "decisions": [],
                "bot_ids": [],
            }

        scores = _get_daily_scores(conn, user_id, report_date)
        results = []
        touched_bot_ids = []

        for bot in bots:

            # =====================================================
            # SYMBOL
            # =====================================================
            symbol = (bot.get("symbol") or DEFAULT_SYMBOL).upper()

            # =====================================================
            # LIVE PRICE
            # =====================================================
            live_price = _get_live_price(conn, symbol)

            # =====================================================
            # SNAPSHOT
            # =====================================================
            snapshot = _get_active_strategy_snapshot(
                conn,
                user_id,
                bot["strategy_id"],
                report_date,
            )

            # =====================================================
            # SETUP PAYLOAD
            # =====================================================
            setup_payload = _get_strategy_setup_payload(
                conn,
                user_id=user_id,
                strategy_id=bot["strategy_id"],
                setup_id=bot.get("setup_id"),
                setup_name=bot.get("setup_type"),
                symbol=symbol,
            )

            if setup_payload.get("symbol"):
                symbol = setup_payload.get("symbol").upper()

            if snapshot:
                setup_payload.update({
                    "entry": snapshot.get("entry"),
                    "stop_loss": snapshot.get("stop_loss"),
                    "targets": snapshot.get("targets"),
                })

            # =====================================================
            # PORTFOLIO CONTEXT
            # =====================================================
            today_spent_eur = get_today_spent_eur(
                conn,
                user_id,
                bot["bot_id"],
                report_date,
            )

            cash_balance_eur = get_bot_balance(
                conn,
                user_id,
                bot["bot_id"],
            )

            current_asset_value_eur = get_asset_position_value(
                conn,
                user_id,
                bot["bot_id"],
                symbol,
            )

            cash_available = max(0.0, cash_balance_eur)

            portfolio_value_eur = max(
                current_asset_value_eur + cash_available,
                1.0,
            )

            portfolio_context = {
                "today_allocated_eur": today_spent_eur,
                "portfolio_value_eur": portfolio_value_eur,
                "current_asset_value_eur": current_asset_value_eur,
                "max_trade_risk_eur": bot["budget"].get("max_order_eur"),
                "daily_allocation_eur": bot["budget"].get("daily_limit_eur"),
                "max_asset_exposure_pct": bot["budget"].get("max_asset_exposure_pct"),
                "total_budget_eur": bot["budget"].get("total_eur"),
                "kill_switch": True,
                "live_price": live_price,
            }

            # =====================================================
            # BOT BRAIN
            # =====================================================
            brain = run_bot_brain(
                user_id=user_id,
                setup=setup_payload,
                scores={
                    "macro_score": scores.get("macro"),
                    "technical_score": scores.get("technical"),
                    "market_score": scores.get("market"),
                    "setup_score": scores.get("setup"),
                },
                portfolio_context=portfolio_context,
            )

            action = _normalize_action(brain.get("action"))

            # =====================================================
            # SETUP MATCH
            # =====================================================
            setup_match = _build_setup_match(
                bot=bot,
                scores=scores,
                snapshot=snapshot,
            )

            # =====================================================
            # TRADE PLAN
            # =====================================================
            trade_plan = brain.get("trade_plan")

            if not trade_plan:
                logger.error(
                    "❌ Trade plan missing | bot=%s | action=%s | snapshot=%s",
                    bot["bot_id"],
                    action,
                    bool(snapshot),
                )
                trade_plan = _default_trade_plan(
                    symbol=symbol,
                    action=action,
                    reason="fallback_missing_trade_plan",
                    snapshot=snapshot,
                )

            elif not isinstance(trade_plan, dict):
                logger.error(
                    "❌ Trade plan invalid format | bot=%s | type=%s",
                    bot["bot_id"],
                    type(trade_plan),
                )
                trade_plan = _default_trade_plan(
                    symbol=symbol,
                    action=action,
                    reason="fallback_invalid_trade_plan",
                    snapshot=snapshot,
                )

            # =====================================================
            # POSITION SIZE
            # =====================================================
            raw_position_size = brain.get("position_size")
            if raw_position_size is None:
                raw_position_size = brain.get("metrics", {}).get("position_size")
            if raw_position_size is None:
                raw_position_size = 0.0

            position_size = float(raw_position_size)
            position_size = max(0.0, min(position_size, 1.0))

            # =====================================================
            # METRICS
            # =====================================================
            metrics = brain.get("metrics") or {}

            # =====================================================
            # DECISION
            # =====================================================
            decision = {
                "bot_id": bot["bot_id"],
                "symbol": symbol,

                "action": action,
                "confidence": _map_confidence(float(brain.get("confidence") or 0.0)),
                "status": "planned",

                "amount_eur": round(float(brain.get("amount_eur") or 0), 2),
                "requested_amount_eur": round(
                    float(brain.get("debug", {}).get("final_amount") or 0), 2
                ),

                "base_amount": brain.get("base_amount") or setup_payload.get("base_amount"),
                "execution_mode": setup_payload.get("execution_mode"),

                "position_size": round(position_size, 2),
                "exposure_multiplier": float(brain.get("exposure_multiplier") or 1.0),

                # ✅ single source of truth = DB
                "score": scores.get("setup"),

                "strategy_reason": brain.get("reason"),
                "regime": brain.get("regime"),
                "risk_state": brain.get("risk_state"),

                "market_pressure": metrics.get("market_pressure"),
                "transition_risk": metrics.get("transition_risk"),

                "volatility_state": brain.get("volatility_state"),
                "trend_strength": brain.get("trend_strength"),
                "structure_bias": brain.get("structure_bias"),

                "trade_plan": trade_plan,
                "watch_levels": brain.get("watch_levels"),
                "monitoring": brain.get("monitoring"),
                "alerts_active": brain.get("alerts_active"),

                "guardrails_result": brain.get("guardrails_result"),
                "guardrail_reason": brain.get("guardrail_reason"),

                "setup_match": setup_match,
                "live_price": live_price,

                "metrics": metrics,
            }

            # =====================================================
            # SAVE DECISION
            # =====================================================
            decision_id = _persist_decision_and_order(
                conn=conn,
                user_id=user_id,
                bot_id=bot["bot_id"],
                strategy_id=bot["strategy_id"],
                setup_id=bot.get("setup_id"),
                report_date=report_date,
                decision=decision,
                scores=scores,
            )

            # Oude pending orders opruimen voor dezelfde decision
            _clear_existing_pending_orders_for_day(
                conn,
                user_id=user_id,
                bot_id=bot["bot_id"],
                decision_id=decision_id,
            )

            # =====================================================
            # BUILD ORDER
            # =====================================================
            order = build_order_proposal(
                conn=conn,
                bot=bot,
                decision=decision,
                today_spent_eur=today_spent_eur,
                total_balance_eur=cash_balance_eur,
            )

            execution_status = None
            bot_order_id = None

            # =====================================================
            # SAVE + AUTO EXECUTE
            # =====================================================
            if order:
                bot_order_id = _persist_bot_order(
                    conn=conn,
                    user_id=user_id,
                    bot_id=bot["bot_id"],
                    decision_id=decision_id,
                    order=order,
                )

                try:
                    _auto_execute_decision(
                        conn=conn,
                        user_id=user_id,
                        bot_id=bot["bot_id"],
                        decision_id=decision_id,
                        order=order,
                    )
                    execution_status = "filled"
                except Exception as exec_err:
                    logger.exception(
                        "❌ Auto execution failed | bot=%s | decision=%s",
                        bot["bot_id"],
                        decision_id,
                    )
                    execution_status = f"execution_failed: {exec_err}"
            else:
                execution_status = "no_order"

            # =====================================================
            # TOUCH LAST RUN
            # =====================================================
            _touch_bot_last_run(
                conn,
                user_id=user_id,
                bot_id=bot["bot_id"],
            )

            touched_bot_ids.append(bot["bot_id"])

            results.append({
                "bot_id": bot["bot_id"],
                "decision_id": decision_id,
                "bot_order_id": bot_order_id,
                "action": decision["action"],
                "decision": decision,
                "trade_plan": trade_plan,
                "execution_status": execution_status,
            })

        conn.commit()

        return {
            "ok": True,
            "date": str(report_date),
            "bots": len(bots),
            "decisions": results,
            "bot_ids": touched_bot_ids,
        }

    except Exception as e:
        logger.exception("❌ trading_bot_agent failed")
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

    finally:
        conn.close()

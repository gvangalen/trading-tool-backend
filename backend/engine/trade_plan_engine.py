def build_trade_plan(snapshot, brain, decision, bot):
    """
    Builds structured trade plan for execution + UI.

    Features
    --------
    • Long + Short support
    • R-multiple targets
    • Risk / Reward calculation
    • Risk in EUR
    • Position sizing
    • Regime-aware stop logic

    Targets
    -------
    TP1 = 0.6R
    TP2 = 1.5R
    TP3 = 2.5R
    """

    if not snapshot:
        return None

    action = decision.get("action", "hold")
    symbol = decision.get("symbol")

    entry = snapshot.get("entry")
    stop = snapshot.get("stop_loss")

    if not entry or not stop:
        return None

    # ensure list
    if not isinstance(entry, list):
        entry = [entry]

    entry_price = float(entry[0])
    stop = float(stop)

    regime = brain.get("regime")

    # =====================================================
    # REGIME AWARE STOP
    # =====================================================

    if regime == "risk_off":
        stop = entry_price - (entry_price - stop) * 0.6

    elif regime == "high_volatility":
        stop = entry_price - (entry_price - stop) * 1.25

    # =====================================================
    # EXIT LOGIC
    # =====================================================

    if action == "sell":
        return {
            "symbol": symbol,
            "side": "sell",
            "exit_plan": [
                {"type": "market", "reason": "strategy_exit"}
            ],
            "risk": {
                "reason": brain.get("reason", "exit_signal")
            },
        }

    # =====================================================
    # ENTRY PLAN
    # =====================================================

    entry_plan = [
        {"type": "limit", "price": float(p)}
        for p in entry
    ]

    # =====================================================
    # RISK PER UNIT
    # =====================================================

    try:
        risk_per_unit = abs(entry_price - stop)
    except Exception:
        return None

    if risk_per_unit == 0:
        return None

    # =====================================================
    # TARGETS USING R MULTIPLES
    # =====================================================

    r_levels = [0.6, 1.5, 2.5]

    targets_plan = []

    for i, r in enumerate(r_levels):

        if action == "buy":
            target_price = entry_price + (risk_per_unit * r)
        else:
            target_price = entry_price - (risk_per_unit * r)

        targets_plan.append({
            "label": f"TP{i+1}",
            "price": round(target_price, 2),
            "r_multiple": r,
            "profit_per_unit": round(risk_per_unit * r, 2)
        })

    # =====================================================
    # REWARD CALCULATION
    # =====================================================

    reward_per_unit = targets_plan[-1]["profit_per_unit"]

    try:
        rr = round(reward_per_unit / risk_per_unit, 2)
    except Exception:
        rr = None

    # =====================================================
    # MIN RR FILTER
    # =====================================================

    min_rr = bot.get("min_rr", 1.5)

    if rr and rr < min_rr:
        return None

    # =====================================================
    # POSITION SIZE
    # =====================================================

    max_risk_eur = bot.get("max_risk_per_trade")

    position_size_units = None
    risk_eur = None

    if max_risk_eur and risk_per_unit > 0:

        try:
            position_size_units = round(max_risk_eur / risk_per_unit, 6)
            risk_eur = round(position_size_units * risk_per_unit, 2)
        except Exception:
            position_size_units = None
            risk_eur = None

    # =====================================================
    # FINAL PLAN
    # =====================================================

    plan = {

        "symbol": symbol,

        "side": action,

        "entry_plan": entry_plan,

        "stop_loss": {
            "price": round(stop, 2)
        },

        "targets": targets_plan,

        "position": {
            "units": position_size_units,
        },

        "risk": {
            "risk_per_unit": round(risk_per_unit, 2),
            "reward_per_unit": round(reward_per_unit, 2),
            "risk_eur": risk_eur,
            "rr": rr,
            "regime": regime
        }

    }

    return plan

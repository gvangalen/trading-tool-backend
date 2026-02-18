def build_trade_plan(snapshot, brain, decision, bot):
    """
    Builds structured trade plan for execution + UI.
    Safe, flexible & future-proof.
    """

    if not snapshot:
        return None

    action = decision.get("action", "hold")
    symbol = decision.get("symbol")

    entry = snapshot.get("entry")
    stop = snapshot.get("stop_loss")
    targets = snapshot.get("targets") or []

    if not entry or not stop:
        return None

    # ensure lists
    if not isinstance(entry, list):
        entry = [entry]

    if not isinstance(targets, list):
        targets = [targets]

    regime = brain.get("regime")

    # =====================================================
    # 🔒 Stop adjustment logic (regime aware)
    # =====================================================
    if regime == "risk_off":
        # tighten stop slightly toward entry
        stop = entry[0] - (entry[0] - stop) * 0.6

    elif regime == "high_volatility":
        # widen stop
        stop = entry[0] - (entry[0] - stop) * 1.25

    # =====================================================
    # SELL / EXIT logic
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
    # TARGET PLAN
    # =====================================================
    targets_plan = [
        {"label": f"TP{i+1}", "price": float(t)}
        for i, t in enumerate(targets)
    ]

    # fallback if no targets
    if not targets_plan:
        targets_plan = [{"label": "TP1", "price": entry[0] * 1.05}]

    # =====================================================
    # RISK METRICS
    # =====================================================
    rr = brain.get("rr_ratio")

    if not rr:
        try:
            rr = round((targets_plan[-1]["price"] - entry[0]) / (entry[0] - stop), 2)
        except Exception:
            rr = None

    plan = {
        "symbol": symbol,
        "side": action,
        "entry_plan": entry_plan,
        "stop_loss": {"price": float(stop)},
        "targets": targets_plan,
        "risk": {
            "rr": rr,
            "regime": regime,
        },
    }

    return plan

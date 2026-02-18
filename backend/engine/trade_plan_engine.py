def build_trade_plan(snapshot, brain, decision, bot):
    if not snapshot:
        return None

    entry = snapshot["entry"]
    stop = snapshot["stop_loss"]
    targets = snapshot["targets"]

    # engine adjustments
    if brain.get("regime") == "risk_off":
        stop *= 0.99  # tighten stop

    plan = {
        "symbol": decision["symbol"],
        "side": decision["action"],
        "entry_plan": [{"type": "limit", "price": entry}],
        "stop_loss": {"price": stop},
        "targets": [
            {"label": f"TP{i+1}", "price": t}
            for i, t in enumerate(targets)
        ],
        "risk": {
            "rr": brain.get("rr_ratio"),
        },
    }

    return plan

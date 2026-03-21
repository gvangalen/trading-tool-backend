def build_trade_plan(snapshot, brain, decision, bot):
    symbol = (decision.get("symbol") or "BTC").upper()
    action = (decision.get("action") or "hold").lower()
    strategy_type = (bot.get("strategy_type") or "").lower()

    def empty_plan(reason="no_data"):
        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": [],
            "stop_loss": {"price": None},
            "targets": [],
            "position": {"units": None},
            "risk": {
                "risk_per_unit": None,
                "reward_per_unit": None,
                "risk_eur": None,
                "rr": None,
                "regime": brain.get("regime"),
                "note": reason,
            },
        }

    if not snapshot:
        return empty_plan("no_snapshot")

    entry = snapshot.get("entry")
    stop = snapshot.get("stop_loss")
    raw_targets = snapshot.get("targets") or []

    # =====================================================
    # 🔥 DCA MODE (SUPER SIMPEL)
    # =====================================================
    if strategy_type == "dca":
        return {
            "symbol": symbol,
            "side": "buy",
            "entry_plan": [
                {
                    "type": "dca",
                    "label": "DCA accumulation",
                    "price": round(float(entry), 2) if entry else None,
                }
            ] if entry else [],
            "stop_loss": {"price": round(float(stop), 2) if stop else None},
            "targets": [
                {"label": f"TP{i+1}", "price": round(float(t), 2)}
                for i, t in enumerate(raw_targets) if t is not None
            ],
            "position": {"units": None},
            "risk": {
                "risk_per_unit": None,
                "reward_per_unit": None,
                "risk_eur": None,
                "rr": None,
                "regime": brain.get("regime"),
                "note": "dca_mode",
            },
        }

    # =====================================================
    # HOLD / OBSERVE = WATCH MODE
    # =====================================================
    if action in ("hold", "observe"):
        entry_plan = []

        if entry is not None:
            if isinstance(entry, list):
                for p in entry:
                    if p is not None:
                        entry_plan.append({
                            "type": "watch",
                            "label": "Observe entry zone",
                            "price": round(float(p), 2),
                        })
            else:
                entry_plan.append({
                    "type": "watch",
                    "label": "Observe entry zone",
                    "price": round(float(entry), 2),
                })

        targets = [
            {"label": f"TP{i+1}", "price": round(float(t), 2)}
            for i, t in enumerate(raw_targets) if t is not None
        ]

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": entry_plan,
            "stop_loss": {"price": round(float(stop), 2) if stop else None},
            "targets": targets,
            "position": {"units": None},
            "risk": {
                "risk_per_unit": None,
                "reward_per_unit": None,
                "risk_eur": None,
                "rr": None,
                "regime": brain.get("regime"),
                "note": "watch_mode",
            },
        }

    # =====================================================
    # SELL
    # =====================================================
    if action == "sell":
        return {
            "symbol": symbol,
            "side": "sell",
            "entry_plan": [],
            "stop_loss": {"price": None},
            "targets": [],
            "position": {"units": None},
            "risk": {
                "risk_per_unit": None,
                "reward_per_unit": None,
                "risk_eur": None,
                "rr": None,
                "regime": brain.get("regime"),
                "note": brain.get("reason", "exit"),
            },
        }

    # =====================================================
    # BUY / SHORT
    # =====================================================
    if entry is None or stop is None:
        return empty_plan("missing_entry_or_stop")

    if not isinstance(entry, list):
        entry = [entry]

    try:
        entry = [float(p) for p in entry if p is not None]
        stop = float(stop)
    except Exception:
        return empty_plan("invalid_price_format")

    if not entry:
        return empty_plan("empty_entry")

    entry_price = sum(entry) / len(entry)
    regime = brain.get("regime") or "neutral"

    risk_distance = abs(entry_price - stop)

    if regime == "risk_off":
        risk_distance *= 0.6
    elif regime == "high_volatility":
        risk_distance *= 1.25

    stop = entry_price - risk_distance if action == "buy" else entry_price + risk_distance

    entry_plan = [{"type": "limit", "price": round(p, 2)} for p in entry]

    risk_per_unit = abs(entry_price - stop)
    if risk_per_unit <= 0:
        return empty_plan("invalid_risk_distance")

    targets_plan = []
    for i, r in enumerate([0.6, 1.5, 2.5]):
        price = entry_price + (risk_per_unit * r) if action == "buy" else entry_price - (risk_per_unit * r)
        targets_plan.append({
            "label": f"TP{i+1}",
            "price": round(price, 2),
            "r_multiple": r,
        })

    reward_per_unit = risk_per_unit * 2.5
    rr = round(reward_per_unit / risk_per_unit, 2)

    return {
        "symbol": symbol,
        "side": action,
        "entry_plan": entry_plan,
        "stop_loss": {"price": round(stop, 2)},
        "targets": targets_plan,
        "position": {"units": None},
        "risk": {
            "risk_per_unit": round(risk_per_unit, 2),
            "reward_per_unit": round(reward_per_unit, 2),
            "risk_eur": None,
            "rr": rr,
            "regime": regime,
            "note": None,
        },
    }

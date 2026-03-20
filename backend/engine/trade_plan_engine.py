def build_trade_plan(snapshot, brain, decision, bot):
    symbol = (decision.get("symbol") or "BTC").upper()
    action = (decision.get("action") or "hold").lower()

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

    # -----------------------------
    # HOLD / OBSERVE = WATCH MODE
    # -----------------------------
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

        targets = []
        for i, t in enumerate(raw_targets):
            if t is not None:
                targets.append({
                    "label": f"TP{i+1}",
                    "price": round(float(t), 2),
                })

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": entry_plan,
            "stop_loss": {
                "price": round(float(stop), 2) if stop is not None else None
            },
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

    # -----------------------------
    # SELL = CONSISTENT SCHEMA
    # -----------------------------
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
                "note": brain.get("reason", "strategy_exit"),
            },
        }

    # -----------------------------
    # BUY / SHORT REQUIRE ENTRY+STOP
    # -----------------------------
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

    if action == "buy":
        stop = entry_price - risk_distance
    elif action == "short":
        stop = entry_price + risk_distance
    else:
        return empty_plan("unsupported_action")

    entry_plan = [
        {"type": "limit", "price": round(p, 2)}
        for p in entry
    ]

    risk_per_unit = abs(entry_price - stop)

    if risk_per_unit <= 0:
        return empty_plan("invalid_risk_distance")

    r_levels = [0.6, 1.5, 2.5]
    targets_plan = []

    for i, r in enumerate(r_levels):
        if action == "buy":
            target_price = entry_price + (risk_per_unit * r)
        elif action == "short":
            target_price = entry_price - (risk_per_unit * r)
        else:
            continue

        targets_plan.append({
            "label": f"TP{i+1}",
            "price": round(target_price, 2),
            "r_multiple": r,
            "profit_per_unit": round(risk_per_unit * r, 2),
        })

    if not targets_plan:
        return empty_plan("no_targets")

    reward_per_unit = targets_plan[-1]["profit_per_unit"]
    rr = round(reward_per_unit / risk_per_unit, 2) if risk_per_unit > 0 else None

    min_rr = bot.get("min_rr", 1.5)
    rr_note = "rr_below_threshold" if rr is not None and rr < min_rr else None

    max_risk_eur = bot.get("max_risk_per_trade")
    position_size_units = None
    risk_eur = None

    if max_risk_eur and risk_per_unit > 0:
        try:
            position_size_units = round(float(max_risk_eur) / risk_per_unit, 6)
            risk_eur = round(position_size_units * risk_per_unit, 2)
        except Exception:
            position_size_units = None
            risk_eur = None

    return {
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
            "regime": regime,
            "note": rr_note,
        },
    }

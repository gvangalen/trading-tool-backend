def build_trade_plan(snapshot, brain, decision, bot):
    symbol = (decision.get("symbol") or "BTC").upper()
    action = (decision.get("action") or "hold").lower()
    strategy_type = (bot.get("strategy_type") or "").lower().strip()
    live_price = decision.get("live_price")

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
    # 1️⃣ DCA → ALTIJD EIGEN PLAN, OOK ALS ACTION = HOLD
    # =====================================================
    if strategy_type == "dca":
        price = None

        try:
            if entry is not None:
                if isinstance(entry, list):
                    first_valid = next((p for p in entry if p is not None), None)
                    price = float(first_valid) if first_valid is not None else None
                else:
                    price = float(entry)
            elif live_price is not None:
                price = float(live_price)
        except Exception:
            price = None

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": [
                {
                    "type": "watch",
                    "label": "Observe accumulation zone",
                    "price": round(price, 2),
                }
            ] if price is not None else [],
            "stop_loss": None,
            "targets": [],
            "position": {"units": None},
            "risk": None,
        }

    # =====================================================
    # 2️⃣ HOLD / OBSERVE → WATCH MODE
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
            for i, t in enumerate(raw_targets)
            if t is not None
        ]

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": entry_plan,
            "stop_loss": {"price": round(float(stop), 2)} if stop is not None else {"price": None},
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
    # 3️⃣ SELL
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
    # 4️⃣ BUY / NORMAL TRADE
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

    if live_price is not None:
        try:
            live_price = float(live_price)
            if action == "buy" and live_price > entry_price:
                entry_price = live_price
        except Exception:
            pass

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

    targets_plan = [
        {"label": f"TP{i+1}", "price": round(float(t), 2)}
        for i, t in enumerate(raw_targets)
        if t is not None
    ]

    reward_per_unit = None
    rr = None

    if raw_targets:
        try:
            first_target = float(raw_targets[0])
            reward_per_unit = abs(first_target - entry_price)

            if risk_per_unit > 0:
                rr = round(reward_per_unit / risk_per_unit, 2)
        except Exception:
            pass

    return {
        "symbol": symbol,
        "side": action,
        "entry_plan": entry_plan,
        "stop_loss": {"price": round(stop, 2)},
        "targets": targets_plan,
        "position": {"units": None},
        "risk": {
            "risk_per_unit": round(risk_per_unit, 2),
            "reward_per_unit": round(reward_per_unit, 2) if reward_per_unit else None,
            "risk_eur": None,
            "rr": rr,
            "regime": regime,
            "note": None,
        },
    }

def build_trade_plan(snapshot, brain, decision, bot):
    symbol = (decision.get("symbol") or "BTC").upper()
    action = (decision.get("action") or "hold").lower()
    strategy_type = (bot.get("strategy_type") or "").lower().strip()
    live_price = decision.get("live_price")

    # =====================================================
    # 🔧 Helpers
    # =====================================================
    def safe_float(v):
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

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
    # 1️⃣ DCA (altijd eigen gedrag)
    # =====================================================
    if strategy_type.startswith("dca"):
        price = safe_float(entry)

        if price is None:
            price = safe_float(live_price)

        entry_plan = []
        if price:
            entry_plan.append({
                "type": "watch",
                "label": "Observe accumulation zone",
                "price": round(price, 2),
            })

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": entry_plan,
            "stop_loss": None,
            "targets": [],
            "position": {"units": None},
            "risk": None,
        }

    # =====================================================
    # 2️⃣ HOLD / OBSERVE → 🔥 PRO WATCH MODE
    # =====================================================
    if action in ("hold", "observe"):

        entry_plan = []

        entry_price = safe_float(entry)
        breakout = safe_float(raw_targets[0]) if raw_targets else None

        # 🔥 Pullback zone
        if entry_price:
            entry_plan.append({
                "type": "watch",
                "label": "Watch pullback zone",
                "price": round(entry_price, 2),
            })

        # 🔥 Breakout level
        if breakout:
            entry_plan.append({
                "type": "watch",
                "label": "Watch breakout",
                "price": round(breakout, 2),
            })

        # fallback → live price
        if not entry_plan and live_price:
            lp = safe_float(live_price)
            if lp:
                entry_plan.append({
                    "type": "watch",
                    "label": "Watch current price",
                    "price": round(lp, 2),
                })

        targets = [
            {"label": f"TP{i+1}", "price": round(float(t), 2)}
            for i, t in enumerate(raw_targets)
            if safe_float(t) is not None
        ]

        return {
            "symbol": symbol,
            "side": action,
            "entry_plan": entry_plan,
            "stop_loss": {"price": safe_float(stop)},
            "targets": targets,
            "position": {"units": None},
            "risk": {
                "risk_per_unit": None,
                "reward_per_unit": None,
                "risk_eur": None,
                "rr": None,
                "regime": brain.get("regime"),
                "note": "waiting_for_setup",
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
    # 4️⃣ BUY (echte trade)
    # =====================================================
    if entry is None or stop is None:
        return empty_plan("missing_entry_or_stop")

    if not isinstance(entry, list):
        entry = [entry]

    try:
        entry = [float(p) for p in entry if safe_float(p) is not None]
        stop = float(stop)
    except Exception:
        return empty_plan("invalid_price_format")

    if not entry:
        return empty_plan("empty_entry")

    entry_price = sum(entry) / len(entry)

    # 🔥 live price override (realistisch gedrag)
    if live_price is not None:
        lp = safe_float(live_price)
        if lp and action == "buy" and lp > entry_price:
            entry_price = lp

    regime = brain.get("regime") or "neutral"

    # =====================================================
    # Risk aanpassen op regime
    # =====================================================
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
        if safe_float(t) is not None
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

def run_setup_agent(asset="BTC", user_id: int = None):
    if user_id is None:
        raise ValueError("❌ Setup-Agent vereist een user_id")

    logger.info(f"🤖 Setup-Agent gestart (user_id={user_id}, asset={asset})")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return {"active_setup": None, "all_setups": []}

    try:
        # ---------------------------------------------------------------
        # 1️⃣ DAGELIJKSE SCORES (PER USER)
        # ---------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, market_score
                FROM daily_scores
                WHERE report_date = CURRENT_DATE
                  AND user_id = %s
                LIMIT 1;
            """, (user_id,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ Geen daily_scores voor user_id={user_id}")
            return {"active_setup": None, "all_setups": []}

        macro_score, technical_score, market_score = map(to_float, row)

        # ---------------------------------------------------------------
        # 2️⃣ SETUPS (PER USER)
        # ---------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id, name, symbol,
                    min_macro_score, max_macro_score,
                    min_technical_score, max_technical_score,
                    min_market_score, max_market_score,
                    strategy_type, created_at
                FROM setups
                WHERE symbol = %s
                  AND user_id = %s
                ORDER BY created_at DESC;
            """, (asset, user_id))
            setups = cur.fetchall()

        if not setups:
            logger.info(f"ℹ️ Geen setups voor user_id={user_id}")
            return {"active_setup": None, "all_setups": []}

        results = []
        best_setup = None
        best_match_score = -1

        # ---------------------------------------------------------------
        # 3️⃣ MATCH LOGICA
        # ---------------------------------------------------------------
        for (
            setup_id, name, symbol,
            min_macro, max_macro,
            min_tech, max_tech,
            min_market, max_market,
            strategy_type, _
        ) in setups:

            macro_match = score_overlap(macro_score, min_macro, max_macro)
            tech_match = score_overlap(technical_score, min_tech, max_tech)
            market_match = score_overlap(market_score, min_market, max_market)

            total_match = round((macro_match + tech_match + market_match) / 3)
            active = macro_match > 0 and tech_match > 0 and market_match > 0

            if total_match > best_match_score:
                best_match_score = total_match
                best_setup = {
                    "setup_id": setup_id,
                    "name": name,
                    "symbol": symbol,
                    "total_match": total_match,
                    "active": active,
                    "strategy_type": strategy_type,
                }

            ai_comment = ask_gpt_text(f"""
MARKT SCORES:
Macro {macro_score}
Technical {technical_score}
Market {market_score}

Setup '{name}' ranges:
Macro {min_macro}-{max_macro}
Technical {min_tech}-{max_tech}
Market {min_market}-{max_market}

Geef één korte zin waarom deze setup {total_match}/100 scoort.
""")

            results.append({
                "setup_id": setup_id,
                "name": name,
                "match_score": total_match,
                "active": active,
                "macro_match": macro_match,
                "technical_match": tech_match,
                "market_match": market_match,
                "ai_comment": ai_comment,
                "best_of_day": False,
            })

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores
                        (setup_id, user_id, report_date, score, is_active, explanation, breakdown)
                    VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (setup_id, user_id, report_date)
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation,
                        breakdown = EXCLUDED.breakdown,
                        created_at = NOW();
                """, (
                    setup_id,
                    user_id,
                    total_match,
                    active,
                    ai_comment,
                    json.dumps({
                        "macro": macro_match,
                        "technical": tech_match,
                        "market": market_match
                    })
                ))

        # ---------------------------------------------------------------
        # 4️⃣ BEST SETUP MARKEREN
        # ---------------------------------------------------------------
        if best_setup:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE daily_setup_scores
                    SET is_best = TRUE
                    WHERE setup_id = %s
                      AND user_id = %s
                      AND report_date = CURRENT_DATE;
                """, (best_setup["setup_id"], user_id))

            for r in results:
                if r["setup_id"] == best_setup["setup_id"]:
                    r["best_of_day"] = True

        # ---------------------------------------------------------------
        # 5️⃣ AI SAMENVATTING (SETUP CATEGORY)
        # ---------------------------------------------------------------
        avg_score = round(sum(r["match_score"] for r in results) / len(results), 2)
        active_count = sum(1 for r in results if r["active"])

        summary = (
            f"Beste setup vandaag: {best_setup['name']} ({best_match_score}/100). "
            f"{active_count}/{len(results)} setups actief."
            if best_setup else
            "Geen actieve setups vandaag."
        )

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('setup', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, user_id, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                avg_score,
                "Match analyse",
                "Actief" if active_count else "Afwachten",
                "Laag" if market_score >= 60 else "Hoog",
                summary,
                json.dumps(results[:3])
            ))

        conn.commit()
        logger.info(f"✅ Setup-Agent klaar voor user_id={user_id}")
        return {"active_setup": best_setup, "all_setups": results}

    except Exception:
        conn.rollback()
        logger.error("❌ Setup-Agent crash", exc_info=True)
        return {"active_setup": None, "all_setups": []}

    finally:
        conn.close()

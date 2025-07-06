@router.post("/save")
async def save_market_data():
    logger.info("📡 [save] Ophalen van marktdata via CoinGecko...")
    crypto_data = {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for symbol, coingecko_id in ASSETS.items():
                url = COINGECKO_URL.format(id=coingecko_id)
                response = await client.get(url)
                response.raise_for_status()
                ohlc = response.json()

                if not ohlc:
                    logger.warning(f"⚠️ Geen OHLC-data voor {symbol}")
                    continue  # skip deze asset

                latest = ohlc[-1]
                open_, high, low, close = map(float, latest[1:5])
                change = ((close - open_) / open_) * 100

                vol_response = await client.get(VOLUME_URL.format(coingecko_id))
                vol_response.raise_for_status()
                market_data = vol_response.json()
                volume = market_data.get("market_data", {}).get("total_volume", {}).get("usd", None)

                crypto_data[symbol] = {
                    "price": close,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "change_24h": round(change, 2),
                    "volume": float(volume) if volume else None
                }

    except Exception as e:
        logger.error(f"❌ [save] Fout bij ophalen van CoinGecko-data: {e}")
        raise HTTPException(status_code=500, detail="❌ Fout bij ophalen van marktdata.")

    if not crypto_data:
        logger.warning("⚠️ Geen geldige crypto-data ontvangen, niets opgeslagen.")
        return {"message": "⚠️ Geen marktdata opgeslagen (lege respons van CoinGecko)."}

    conn, cur = get_db_cursor()
    try:
        for symbol, data in crypto_data.items():
            cur.execute("""
                INSERT INTO market_data (symbol, price, open, high, low, change_24h, volume, timestamp, is_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), TRUE)
            """, (
                symbol,
                data["price"],
                data["open"],
                data["high"],
                data["low"],
                data["change_24h"],
                data["volume"]
            ))
        conn.commit()
        logger.info("✅ [save] Marktdata succesvol opgeslagen.")
        return {"message": "✅ Marktdata opgeslagen"}
    except Exception as e:
        logger.error(f"❌ [save] DB-fout: {e}")
        raise HTTPException(status_code=500, detail="❌ DB-fout bij opslaan.")
    finally:
        conn.close()

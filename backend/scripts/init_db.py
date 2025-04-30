import logging
from scripts.database import get_db_connection  # ⬅️ Aangepaste import

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Rest van de code blijft gelijk...

def create_setups_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS setups (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                setup_name TEXT NOT NULL,
                conditions JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("✅ Tabel 'setups' succesvol aangemaakt.")

def create_market_data_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                price NUMERIC,
                volume NUMERIC,
                change_24h NUMERIC,
                is_updated BOOLEAN DEFAULT TRUE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("✅ Tabel 'market_data' succesvol aangemaakt.")

def create_technical_data_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS technical_data (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                rsi NUMERIC,
                volume NUMERIC,
                ma_200 NUMERIC,
                price NUMERIC,
                is_updated BOOLEAN DEFAULT TRUE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("✅ Tabel 'technical_data' succesvol aangemaakt.")

def create_macro_data_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS macro_data (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value NUMERIC,
                trend TEXT,
                interpretation TEXT,
                action TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("✅ Tabel 'macro_data' succesvol aangemaakt.")

def run_all():
    conn = get_db_connection()
    if not conn:
        logging.error("❌ Kan geen verbinding maken met de database.")
        return

    try:
        create_setups_table(conn)
        create_market_data_table(conn)
        create_technical_data_table(conn)
        create_macro_data_table(conn)
        conn.commit()
        logging.info("✅ Alle tabellen succesvol gecreëerd of gecontroleerd.")
    except Exception as e:
        logging.error(f"❌ Fout bij aanmaken tabellen: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_all()

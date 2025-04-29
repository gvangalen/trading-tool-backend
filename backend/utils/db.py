import psycopg2
import os
import logging

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    """Maakt een verbinding met de database."""
    db_config = {
        "host": os.getenv("DB_HOST", "market_dashboard_db"),
        "database": os.getenv("DB_NAME", "market_dashboard"),
        "user": os.getenv("DB_USER", "dashboard_user"),
        "password": os.getenv("DB_PASS", "password"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    try:
        conn = psycopg2.connect(**db_config)
        logging.info("✅ Databaseverbinding succesvol opgezet")
        return conn
    except psycopg2.Error as e:
        logging.error(f"❌ Databasefout: {e}")
    return None

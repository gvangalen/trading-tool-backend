import psycopg2
import os
import logging
from dotenv import load_dotenv  # ✅ Zorg dat .env automatisch geladen wordt

# ✅ .env-bestand laden (alleen nodig als dit bestand los wordt aangeroepen)
load_dotenv()

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    """Maakt een verbinding met de PostgreSQL database op basis van omgevingsvariabelen."""
    db_config = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),  # ✅ fallback = localhost
        "database": os.getenv("DB_NAME", "market_dashboard"),
        "user": os.getenv("DB_USER", "dashboard_user"),
        "password": os.getenv("DB_PASS", "password"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    try:
        conn = psycopg2.connect(**db_config)
        logging.info(f"✅ Verbonden met database {db_config['database']} op {db_config['host']}:{db_config['port']}")
        return conn
    except psycopg2.Error as e:
        logging.error(f"❌ Databasefout: {e}")
        return None

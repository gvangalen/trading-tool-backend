import os
import psycopg2
import logging
from dotenv import load_dotenv

# ✅ Laad omgevingsvariabelen
load_dotenv()

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_db_connection():
    """Maakt een verbinding met de PostgreSQL database."""
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "market_dashboard"),
        "user": os.getenv("DB_USER", "dashboard_user"),
        "password": os.getenv("DB_PASS", "password"),
        "port": int(os.getenv("DB_PORT", 5432)),
    }

    try:
        conn = psycopg2.connect(**db_config)
        logger.info("✅ Verbonden met de database.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Databaseverbinding mislukt: {e}")
        return None

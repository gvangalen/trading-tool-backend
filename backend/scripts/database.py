import os
import psycopg2
import logging
from dotenv import load_dotenv

# ✅ Laad .env-variabelen
load_dotenv()

# ✅ Logging configureren
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

    # 🔒 Beveiligingscheck op standaardwaarden
    if db_config["host"] in ["localhost", "127.0.0.1"] or db_config["password"] in ["password", "", "admin"]:
        logger.warning("⚠️ Waarschuwing: Mogelijk onveilige of standaard databaseconfiguratie gebruikt.")

    try:
        conn = psycopg2.connect(**db_config)
        logger.info(f"✅ Verbonden met de database ({db_config['host']}:{db_config['port']})")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Databaseverbinding mislukt: {e}")
        return None

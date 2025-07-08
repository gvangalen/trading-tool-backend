import os
import psycopg2
import logging
from dotenv import load_dotenv

# ✅ Laad .env-variabelen
load_dotenv()

# ✅ Logging instellen
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Vereiste variabelen
REQUIRED_VARS = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS"]

# ✅ Check of vereiste env-vars bestaan
missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing:
    logger.error(f"❌ Ontbrekende omgevingsvariabelen in .env: {', '.join(missing)}")
    raise EnvironmentError(f"Vereiste .env-variabelen ontbreken: {', '.join(missing)}")

def get_db_connection():
    """Maakt een veilige verbinding met de PostgreSQL database."""
    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
    }

    # 🔒 Beveiligingscheck op zwakke wachtwoorden
    if db_config["password"] in ["password", "", "admin"]:
        logger.warning("⚠️ Zwak wachtwoord gedetecteerd in DB_PASS.")

    try:
        conn = psycopg2.connect(**db_config)
        logger.info(f"✅ Verbonden met database {db_config['database']} op {db_config['host']}:{db_config['port']}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Databaseverbinding mislukt: {e}")
        return None

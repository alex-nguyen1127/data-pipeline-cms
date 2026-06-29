import os, logging, psycopg2
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

print(find_dotenv())
load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "cms_warehouse"),
    "user":     os.getenv("DB_USER", "cms_user"),
    "password": os.getenv("DB_PASSWORD", "cms_password"),
}


SCHEMA_FILE = Path(__file__).parent.parent / "sql"/ "schema.sql"

def create_schema() -> None:
    """ Connect to PostgreSQL, execute SQL code, and close connection"""
    log.info("Connecting to PostgreSQL...")
    log.info(f"  Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    log.info(f"  Database: {DB_CONFIG['dbname']}")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            log.info(f"Reading schema from: {SCHEMA_FILE}")
            sql = SCHEMA_FILE.read_text()
            cur.execute(sql)
            log.info("Schema created successfully")

            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]
            log.info(f"Tables in database: {tables}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    create_schema()
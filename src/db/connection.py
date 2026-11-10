import psycopg2
from src.config.settings import settings


def get_conn() -> "psycopg2.extensions.connection":
    return psycopg2.connect(settings.database_url)


def check_db() -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True
    except Exception:
        return False
    finally:
        if conn is not None:
            conn.close()
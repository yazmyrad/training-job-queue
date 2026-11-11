import psycopg2
from src.config.settings import settings


def get_db():
    conn = psycopg2.connect(settings.database_url)
    try:
        yield conn
    finally:
        conn.close()

def get_conn()-> "psycopg2.extensions.connection":
    return psycopg2.connect(settings.database_url)

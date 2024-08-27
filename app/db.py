import psycopg2
from app.config import settings

def get_db_connection():
    try:
        conn = psycopg2.connect(
            user=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            database=settings.db_name,
            port=settings.db_port
        )
        return conn
    except Exception as e:
        raise SystemExit(f"Ошибка подключения к базе данных: {e}")

def create_codes_table(conn):
    with conn.cursor() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS codes (
                        id SERIAL PRIMARY KEY,
                        code TEXT NOT NULL,
                        app_id TEXT NOT NULL,
                        request_time TIMESTAMP NOT NULL,
                        expire_time TIMESTAMP NOT NULL,
                        user_phone TEXT,
                        used INTEGER DEFAULT 0
                    )''')
        conn.commit()

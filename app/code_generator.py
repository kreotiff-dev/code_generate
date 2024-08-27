import random
import string
from datetime import datetime, timedelta

def generate_code(length=5):
    return ''.join(random.choices(string.digits, k=length))

def save_code(conn, code, app_id, user_phone):
    try:
        with conn.cursor() as c:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            expire_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            c.execute("INSERT INTO codes (code, app_id, request_time, expire_time, user_phone) VALUES (%s, %s, %s, %s, %s)",
                      (code, app_id, timestamp, expire_time, user_phone))
            conn.commit()
    except Exception as e:
        raise SystemExit(f"Ошибка при сохранении кода: {e}")

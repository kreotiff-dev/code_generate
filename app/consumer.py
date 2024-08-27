import json
import time
from app.code_generator import generate_code, save_code
from app.rabbitmq import send_code
from app.db import get_db_connection

def process_message(ch, method, properties, body):
    time.sleep(10)
    try:
        body_dict = json.loads(body)
        app_id = body_dict.get('app_id')
        user_phone = body_dict.get('user_phone')

        if app_id and user_phone:
            code = generate_code()
            conn = get_db_connection()
            save_code(conn, code, app_id, user_phone)
            send_code(ch, code, properties.correlation_id, properties.reply_to)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise SystemExit(f"Ошибка при обработке сообщения: {e}")

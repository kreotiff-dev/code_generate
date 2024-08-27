import pika
import logging
from app.config import settings

def get_rabbitmq_connection():
    try:
        return pika.BlockingConnection(pika.ConnectionParameters(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            virtual_host='gbank',
            credentials=pika.PlainCredentials(
                settings.rabbitmq_username,
                settings.rabbitmq_password
            )
        ))
    except Exception as e:
        raise SystemExit(f"Ошибка подключения к RabbitMQ: {e}")

def send_code(channel, code, correlation_id, reply_to):
    logging.info(f"Отправка кода: {code}, correlation_id: {correlation_id}, reply_to: {reply_to}")
    channel.basic_publish(
        exchange='gbank_exchange',
        routing_key='code.responses',
        body=code,
        properties=pika.BasicProperties(
            app_id='1234',
            correlation_id=str(correlation_id),
            reply_to=str(reply_to)
        )
    )
    logging.info(f"Сообщение отправлено с routing key 'code.responses': {code}")

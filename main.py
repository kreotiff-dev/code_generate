import logging
from app.db import get_db_connection, create_codes_table
from app.rabbitmq import get_rabbitmq_connection
from app.consumer import process_message

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    conn = get_db_connection()
    create_codes_table(conn)
    rabbitmq_connection = get_rabbitmq_connection()

    try:
        channel = rabbitmq_connection.channel()
        channel.queue_declare(queue='verification_code_requests', durable=True)
        channel.queue_bind(exchange='gbank_exchange', queue='verification_code_requests', routing_key='code.requests')
        channel.basic_consume(queue='verification_code_requests', on_message_callback=process_message)
        logging.info("Сервис запущен. Ожидание сообщений...")
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Ошибка во время обработки сообщений: {e}")
    finally:
        if conn:
            conn.close()
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.close()


if __name__ == '__main__':
    main()

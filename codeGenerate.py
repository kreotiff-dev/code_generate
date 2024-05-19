import pika
import logging
import os
import random
import string
import psycopg2
import json
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к базе данных PostgreSQL
conn = psycopg2.connect(
    user='postgres',
    password='',
    host='localhost',
    database='postgres',
    port=5432
)
print("Успешное подключение к базе данных")
c = conn.cursor()

# Создание таблицы codes, если она еще не существует
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

# Функция для генерации уникального кода
def generate_code():
    return ''.join(random.choices(string.digits, k=5))

# Cохранение кода и информации о запросе в базе данных
def save_code(code, app_id, user_phone):
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        expire_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        print(
            f"Сохранение кода в базе данных - Код: {code}, App ID: {app_id}, Request Time: {timestamp}, Expire Time: {expire_time}, User Phone: {user_phone}")
        c.execute("INSERT INTO codes (code, app_id, request_time, expire_time, user_phone) VALUES (%s, %s, %s, %s, %s)",
                  (code, app_id, timestamp, expire_time, user_phone))
        conn.commit()  # Коммитим транзакцию
    except Exception as e:
        logging.error("Произошла ошибка при сохранении кода: %s", str(e))


# Функция для обновления confirmcode и вывода SQL-запроса в консоль
def update_user_confirm_code(code, user_phone):
    try:
        sql_query = "UPDATE users SET confirmcode = %s WHERE phone = '%s'" % (code, user_phone)
        print("SQL-запрос для обновления confirmcode:", sql_query)

        # Выполнение SQL-запроса
        c.execute(sql_query)
        conn.commit()
    except Exception as e:
        logging.error("Произошла ошибка при обновлении кода: %s", str(e))

# Отправка через RabbitMQ
def send_code(code, correlation_id, reply_to):
    print(f"Отправка кода через RabbitMQ - Код: {code}, Correlation ID: {correlation_id}, Reply To: {reply_to}")
    credentials = pika.PlainCredentials('admin', '123456')
    parameters = pika.ConnectionParameters('37.46.129.245', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(
        exchange='sms.code',
        routing_key='response',
        body=code,
        properties=pika.BasicProperties(
            app_id='1234',
            correlation_id=str(correlation_id),
            reply_to=str(reply_to)
        )
    )

    print("Код успешно отправлен")

    connection.close()


# Обновляем статус использования кода в базе данных
def update_code_usage(code_id):
    print(f"Обновление статуса использования кода - ID: {code_id}")
    c.execute("UPDATE codes SET used = 1 WHERE id = ?", (code_id,))
    conn.commit()


# Обработчик сообщений из очереди запросов
def callback(ch, method, properties, body):
    app_id = properties.headers.get('app_id', None) if properties is not None and properties.headers is not None else None

    if app_id is not None:
        print(f"Получено сообщение из очереди - App ID: {app_id}")
    else:
        print("App ID не найден в заголовках сообщения")

    # Генерируем код
    code = generate_code()
    print("Сгенерирован код:", code)

    try:
        # Декодируем JSON-строку в словарь
        body_dict = json.loads(body)
        user_phone = body_dict.get('phone', None)

        if user_phone is not None:

            # Сохраняем код в базе данных
            save_code(code, app_id, user_phone)
            update_user_confirm_code(code, user_phone)

            # Отправляем код в ответ в очередь ответов
            send_code(code, properties.correlation_id, properties.reply_to)

            # Подтверждаем получение сообщения
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print("Номер телефона не найден в сообщении")
    except Exception as e:
        print("Ошибка при обработке сообщения:", str(e))


# Подключение к RabbitMQ
credentials = pika.PlainCredentials('admin', '123456')
parameters = pika.ConnectionParameters('37.46.129.245', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Объявляем очередь запросов
channel.queue_declare(queue='code_requests', durable=True)

# Подписываемся на очередь запросов
channel.basic_consume(queue='code_requests', on_message_callback=callback)

print('Сервис генерации кодов запущен. Ожидание запросов...')

# Запуск бесконечного цикла ожидания сообщений
channel.start_consuming()



# Отправка через RabbitMQ
def send_code(code, correlation_id, reply_to):
    print(f"Отправка кода через RabbitMQ - Код: {code}, Correlation ID: {correlation_id}, Reply To: {reply_to}")
    credentials = pika.PlainCredentials('admin', '123456')
    parameters = pika.ConnectionParameters('37.46.129.245', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(
        exchange='sms.code',
        routing_key='response',
        body=code,
        properties=pika.BasicProperties(
            app_id='1234',
            correlation_id=str(correlation_id),
            reply_to=str(reply_to)
        )
    )

    print("Код успешно отправлен")

    connection.close()


# Обновляем статус использования кода в базе данных
def update_code_usage(code_id):
    print(f"Обновление статуса использования кода - ID: {code_id}")
    c.execute("UPDATE codes SET used = 1 WHERE id = ?", (code_id,))
    conn.commit()


# Обработчик сообщений из очереди запросов
def callback(ch, method, properties, body):
    app_id = None
    if properties is not None and properties.headers is not None:
        app_id = properties.headers.get('app_id')

    if app_id is not None:
        print(f"Получено сообщение из очереди - App ID: {app_id}")
    else:
        print("App ID не найден в заголовках сообщения")

    # Генерируем код
    code = generate_code()
    print("Сгенерирован код:", code)

    # Сохраняем код в базе данных
    save_code(code, app_id)

    # Отправляем код в ответ в очередь ответов
    send_code(code, properties.correlation_id, properties.reply_to)

    # Подтверждаем получение сообщения
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Подключение к RabbitMQ
credentials = pika.PlainCredentials('admin', '123456')
parameters = pika.ConnectionParameters('37.46.129.245', credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Объявляем очередь запросов
channel.queue_declare(queue='code_requests', durable=True)

# Подписываемся на очередь запросов
channel.basic_consume(queue='code_requests', on_message_callback=callback)

print('Сервис генерации кодов запущен. Ожидание запросов...')

# Запуск бесконечного цикла ожидания сообщений
channel.start_consuming()

# Закрытие соединения с базой данных
c.close()
conn.close()
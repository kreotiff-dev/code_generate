import pika
import random
import string
import sqlite3
from datetime import datetime, timedelta

# Подключение к базе данных SQLite
conn = sqlite3.connect('codes_history.db')
c = conn.cursor()

# Создание таблицы, если она еще не существует
c.execute('''CREATE TABLE IF NOT EXISTS codes (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                app_id TEXT NOT NULL,
                request_time TEXT NOT NULL,
                expire_time TEXT NOT NULL,
                used INTEGER DEFAULT 0
            )''')
conn.commit()

# Функция для генерации уникального кода
def generate_code():
    return ''.join(random.choices(string.digits, k=5))

# Функция для сохранения кода и информации о запросе в базу данных
def save_code(code, app_id):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    expire_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')  # Устанавливаем срок действия кода в 1 минуту
    c.execute("INSERT INTO codes (code, app_id, request_time, expire_time) VALUES (?, ?, ?, ?)", (code, app_id, timestamp, expire_time))
    conn.commit()

# Функция для отправки кода через RabbitMQ
def send_code(code, correlation_id, reply_to):
    credentials = pika.PlainCredentials('admin', '123456')
    parameters = pika.ConnectionParameters('37.46.129.245', credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(
        exchange='',
        routing_key=reply_to,
        properties=pika.BasicProperties(correlation_id=correlation_id),
        body=code
    )
    connection.close()

# Функция для обновления статуса использования кода в базе данных
def update_code_usage(code_id):
    c.execute("UPDATE codes SET used = 1 WHERE id = ?", (code_id,))
    conn.commit()

# Функция-обработчик сообщений из очереди запросов
def callback(ch, method, properties, body):
    app_id = properties.app_id
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

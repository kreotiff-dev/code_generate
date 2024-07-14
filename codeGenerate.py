import logging
from pathlib import Path
import pika
import os
import random
import string
import psycopg2
import json
import signal
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    filename='app.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Пути к файлам .env
development_env_path = Path('.env.development')
production_env_path = Path('.env.production')

# Определение среды и загрузка соответствующего файла .env
if development_env_path.exists():
    load_dotenv(dotenv_path='.env.development')
    ENV = 'development'
    logging.info("Загружен файл окружения: .env.development")
elif production_env_path.exists():
    load_dotenv(dotenv_path='.env.production')
    ENV = 'production'
    logging.info("Загружен файл окружения: .env.production")
else:
    logging.error("Не найден ни один файл окружения (.env.development или .env.production)")
    raise SystemExit("Ошибка: отсутствуют файлы окружения")

# Проверка наличия переменных окружения
required_env_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME', 'DB_PORT']
missing_vars = [var for var in required_env_vars if os.getenv(var) is None]

# Проверка на пустые значения
empty_vars = [var for var in required_env_vars if os.getenv(var) == '']

# Исключение для пустого DB_PASSWORD в development
if ENV == 'development' and 'DB_PASSWORD' in empty_vars:
    empty_vars.remove('DB_PASSWORD')

# Логирование для диагностики
logging.debug(f"Текущая среда: {ENV}")
logging.debug(f"Отсутствующие переменные окружения: {missing_vars}")
logging.debug(f"Пустые переменные окружения: {empty_vars}")

if missing_vars:
    logging.error(f"Одна или несколько переменных окружения для базы данных отсутствуют: {', '.join(missing_vars)}")
    raise SystemExit(f"Ошибка: отсутствуют переменные окружения для базы данных: {', '.join(missing_vars)}")

if empty_vars:
    logging.error(f"Одна или несколько переменных окружения для базы данных пустые: {', '.join(empty_vars)}")
    raise SystemExit(f"Ошибка: переменные окружения для базы данных пустые: {', '.join(empty_vars)}")

logging.info("Все необходимые переменные окружения присутствуют и заполнены")
print("Все необходимые переменные окружения присутствуют и заполнены")


# Обработчик сигналов для корректного завершения работы скрипта
def signal_handler(sig, frame):
    logging.info("Получен сигнал прерывания. Завершение работы...")
    if conn:
        conn.close()
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        rabbitmq_connection.close()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Подключение к базе данных PostgreSQL
try:
    conn = psycopg2.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        port=os.getenv('DB_PORT')
    )
    c = conn.cursor()
    logging.info("Успешное подключение к базе данных")
    print("Успешное подключение к базе данных")
except Exception as e:
    logging.error(f"Ошибка подключения к базе данных: {e}")
    raise

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


# Сохранение кода и информации о запросе в базе данных
def save_code(code, app_id, user_phone):
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        expire_time = (datetime.now() + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Сохранение кода в базе данных - Код: {code}, App ID: {app_id}, Request Time: {timestamp}, "
                     f"Expire Time: {expire_time}, User Phone: {user_phone}")
        print(f"Сохранение кода в базе данных - Код: {code}, App ID: {app_id}, Request Time: {timestamp}, Expire Time: {expire_time}, User Phone: {user_phone}")
        c.execute("INSERT INTO codes (code, app_id, request_time, expire_time, user_phone) VALUES (%s, %s, %s, %s, %s)",
                  (code, app_id, timestamp, expire_time, user_phone))
        conn.commit()  # Коммитим транзакцию
        # Проверка, что данные записались в базу
        c.execute("SELECT * FROM codes WHERE code = %s AND user_phone = %s", (code, user_phone))
        result = c.fetchone()
        if result:
            logging.info("Данные успешно записаны в базу данных")
        else:
            raise Exception("Данные не записались в базу данных")

    except Exception as e:
        logging.error("Произошла ошибка при сохранении кода: %s", str(e))
        print(f"Произошла ошибка при сохранении кода: {str(e)}")


# Функция для обновления confirmcode и вывода SQL-запроса в консоль
def update_user_confirm_code(code, user_phone):
    try:
        sql_query = "UPDATE users SET confirm_code = %s WHERE phone = '%s'" % (code, user_phone)
        logging.info(f"SQL-запрос для обновления confirmcode: {sql_query}")
        print(f"SQL-запрос для обновления confirmcode: {sql_query}")

        # Выполнение SQL-запроса
        c.execute(sql_query)
        conn.commit()
    except Exception as e:
        logging.error("Произошла ошибка при обновлении кода: %s", str(e))


# Отправка через RabbitMQ
def send_code(code, correlation_id, reply_to):
    logging.info(f"Отправка кода через RabbitMQ - Код: {code}, Correlation ID: {correlation_id}, Reply To: {reply_to}")
    print(f"Отправка кода через RabbitMQ - Код: {code}, Correlation ID: {correlation_id}, Reply To: {reply_to}")
    credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USERNAME'), os.getenv('RABBITMQ_PASSWORD'))
    parameters = pika.ConnectionParameters(os.getenv('RABBITMQ_HOST'), int(os.getenv('RABBITMQ_PORT')), '/', credentials)
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

    logging.info("Код успешно отправлен")
    print("Код успешно отправлен")

    connection.close()


# Обновляем статус использования кода в базе данных
def update_code_usage(code_id):
    logging.info(f"Обновление статуса использования кода - ID: {code_id}")
    print(f"Обновление статуса использования кода - ID: {code_id}")
    c.execute("UPDATE codes SET used = 1 WHERE id = %s", (code_id,))
    conn.commit()


# Обработчик сообщений из очереди запросов
def callback(ch, method, properties, body):
    try:
        # Декодируем JSON-строку в словарь
        body_dict = json.loads(body)
        app_id = body_dict.get('app_id', None)
        user_phone = body_dict.get('user_phone', None)

        if app_id is not None:
            logging.info(f"Получено сообщение из очереди - App ID: {app_id}")
            print(f"Получено сообщение из очереди - App ID: {app_id}")
        else:
            logging.info("App ID не найден в сообщении")
            print("App ID не найден в сообщении")

        if user_phone is not None:
            # Генерируем код
            code = generate_code()
            logging.info(f"Сгенерирован код: {code}")
            print(f"Сгенерирован код: {code}")

            # Сохраняем код в базе данных
            save_code(code, app_id, user_phone)
            update_user_confirm_code(code, user_phone)

            # Отправляем код в ответ в очередь ответов
            send_code(code, properties.correlation_id, properties.reply_to)

            # Подтверждаем получение сообщения
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logging.info("Номер телефона не найден в сообщении")
            print("Номер телефона не найден в сообщении")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Подтверждаем сообщение, чтобы оно не оставалось в очереди
    except Exception as e:
        logging.error("Ошибка при обработке сообщения: %s", str(e))
        print("Ошибка при обработке сообщения:", str(e))
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Подтверждаем сообщение, чтобы оно не оставалось в очереди


# Подключение к RabbitMQ
try:
    rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('RABBITMQ_HOST'),
        port=int(os.getenv('RABBITMQ_PORT')),
        credentials=pika.PlainCredentials(
            os.getenv('RABBITMQ_USERNAME'),
            os.getenv('RABBITMQ_PASSWORD')
        )
    ))
    channel = rabbitmq_connection.channel()
    logging.info("Успешное подключение к RabbitMQ")
    print("Успешное подключение к RabbitMQ")
except Exception as e:
    logging.error(f"Ошибка подключения к RabbitMQ: {e}")
    raise

try:
    # Объявляем очередь запросов
    channel.queue_declare(queue='code_requests', durable=True)

    # Подписываемся на очередь запросов
    channel.basic_consume(queue='code_requests', on_message_callback=callback)
    logging.info("Сервис успешно запущен. Ожидание запросов...")
    print("Сервис успешно запущен. Ожидание запросов...")
    channel.start_consuming()
except Exception as e:
    logging.error(f"Ошибка во время обработки сообщений: {e}")
    print(f"Ошибка во время обработки сообщений: {e}")
finally:
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        rabbitmq_connection.close()
    if conn:
        conn.close()

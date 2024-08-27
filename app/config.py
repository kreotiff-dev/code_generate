from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path

development_env_path = Path('.env.development')
production_env_path = Path('.env.production')

if development_env_path.exists():
    load_dotenv(dotenv_path='.env.development')
    ENV = 'development'
elif production_env_path.exists():
    load_dotenv(dotenv_path='.env.production')
    ENV = 'production'
else:
    raise SystemExit("Ошибка: файл окружения (.env.development или .env.production) не найден")

class Settings(BaseSettings):
    env: str = "development"
    db_user: str
    db_password: str
    db_host: str
    db_name: str
    db_port: int
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_username: str
    rabbitmq_password: str

    class Config:
        env_file = ".env.development" if ENV == 'development' else ".env.production"

settings = Settings()

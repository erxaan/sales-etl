"""Конфигурация проекта: пути и настройки БД."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Базовые пути
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SQL_DIR = BASE_DIR

# Загрузка переменных окружения из .env
load_dotenv(BASE_DIR / ".env")

# Настройки подключения к PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sales_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")


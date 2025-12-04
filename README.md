# Sales ETL

Читаем `data/sales.csv` и `data/customers.csv` через pandas, считаем `total_price`, сводку по категориям и топ‑5 товаров, валидируем email, чистим данные и грузим в PostgreSQL. Логи, truncate перед загрузкой, UPSERT для клиентов, Docker-оркестрация.

## Быстрый старт (Docker)
```bash
docker compose up --build etl
```
- поднимает Postgres 15 (healthcheck ждёт готовности);
- запускает ETL (`python main.py` внутри контейнера);
- создаёт/очищает таблицы и загружает данные.
Логи: `docker compose logs -f etl`. Остановка: `docker compose down`.

Проверка в DBeaver (после запуска): 
- Host: `localhost`
- Port: `5432`
- DB: `sales_db`
- User: `postgres`
- Password: `password`
- Таблицы: `sales`, `customers`, `sales_summary`, `product_ranking`

## Локальный запуск
```bash
cp env.example .env  
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```
`.env` нужен только локально

## Структура
```
etl/extract.py    — чтение CSV + проверка колонок
etl/transform.py  — очистка, агрегаты, топ-5, валидация email
etl/load.py       — truncate, UPSERT для customers, загрузка в БД
main.py           — оркестрация + ожидание готовности БД
db.sql            — схема таблиц
Dockerfile, docker-compose.yml
tests/test_transform.py
```

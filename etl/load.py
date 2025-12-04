"""Модуль Load: загрузка подготовленных данных в PostgreSQL."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SQL_DIR

logger = logging.getLogger(__name__)


def get_connection() -> PgConnection:
    """Возвращает подключение к PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def create_tables(conn: PgConnection, schema_path: Path | None = None) -> None:
    """Создаёт таблицы по скрипту db.sql."""
    path = schema_path or (SQL_DIR / "db.sql")
    schema_sql = path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    logger.info("Схема БД актуализирована по %s", path)


def truncate_tables(conn: PgConnection) -> None:
    """Очищает целевые таблицы перед перезагрузкой данных."""
    logger.info("Очищаем таблицы перед загрузкой данных")
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                sales,
                customers,
                sales_summary,
                product_ranking
            RESTART IDENTITY;
            """
        )
    conn.commit()
    logger.info("Таблицы очищены")


def load_sales(conn: PgConnection, df_sales: pd.DataFrame) -> None:
    sql = (
        "INSERT INTO sales (order_id, customer_id, product_id, product_name, quantity, "
        "unit_price, total_price, order_date, category, month) VALUES (" + ", ".join(["%s"] * 10) + ")"
    )
    rows = [
        (
            int(row.order_id),
            row.customer_id,
            row.product_id,
            row.product_name,
            int(row.quantity),
            float(row.unit_price),
            float(row.total_price),
            _to_date(row.order_date),
            row.category,
            row.month,
        )
        for row in df_sales.itertuples(index=False)
    ]
    _executemany(conn, sql, rows, "sales")


def load_customers(conn: PgConnection, df_customers: pd.DataFrame) -> None:
    sql = """
    INSERT INTO customers (
        customer_id,
        customer_name,
        email,
        registration_date,
        region,
        customer_days
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO UPDATE SET
        customer_name = EXCLUDED.customer_name,
        email = EXCLUDED.email,
        registration_date = EXCLUDED.registration_date,
        region = EXCLUDED.region,
        customer_days = EXCLUDED.customer_days;
    """
    rows = [
        (
            row.customer_id,
            row.customer_name,
            row.email,
            _to_date(row.registration_date),
            row.region,
            int(row.customer_days) if pd.notna(row.customer_days) else None,
        )
        for row in df_customers.itertuples(index=False)
    ]
    _executemany(conn, sql, rows, "customers")


def load_sales_summary(conn: PgConnection, df_summary: pd.DataFrame) -> None:
    sql = (
        "INSERT INTO sales_summary (category, total_sales, total_quantity, average_order_value, period_date) "
        "VALUES (" + ", ".join(["%s"] * 5) + ")"
    )
    rows = [
        (
            row.category,
            float(row.total_sales),
            int(row.total_quantity),
            float(row.average_order_value),
            _to_date(row.period_date),
        )
        for row in df_summary.itertuples(index=False)
    ]
    _executemany(conn, sql, rows, "sales_summary")


def load_product_ranking(conn: PgConnection, df_ranking: pd.DataFrame) -> None:
    sql = (
        "INSERT INTO product_ranking (product_id, product_name, total_sold, total_revenue, rank_position) "
        "VALUES (" + ", ".join(["%s"] * 5) + ")"
    )
    rows = [
        (
            row.product_id,
            row.product_name,
            int(row.total_sold),
            float(row.total_revenue),
            int(row.rank_position),
        )
        for row in df_ranking.itertuples(index=False)
    ]
    _executemany(conn, sql, rows, "product_ranking")


def _executemany(conn: PgConnection, sql: str, rows: Sequence[Sequence], entity: str) -> None:
    if not rows:
        logger.info("Нет данных для загрузки в %s", entity)
        return

    logger.info("Загружаем %d строк в %s", len(rows), entity)
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    logger.info("Загрузка в %s завершена", entity)


def _to_date(value) -> object:
    """Преобразует pandas.Timestamp / datetime в date."""
    if isinstance(value, pd.Timestamp):
        return value.date()
    if hasattr(value, "date"):
        return value.date()
    return value

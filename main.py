"""Точка входа ETL-процесса."""

from __future__ import annotations

import logging
import sys
import time

import psycopg2

from config import DATA_DIR
from etl.extract import read_sales_csv, read_customers_csv
from etl.transform import (
    transform_sales,
    transform_customers,
    create_sales_summary,
    create_product_ranking,
    create_avg_check_by_region,
)
from etl.load import (
    get_connection,
    create_tables,
    truncate_tables,
    load_sales,
    load_customers,
    load_sales_summary,
    load_product_ranking,
)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("etl.log", encoding="utf-8"),
        ],
    )


def wait_for_db(max_retries: int = 10, delay: float = 2.0) -> None:
    """Ожидает, пока PostgreSQL начнёт принимать подключения."""
    logger = logging.getLogger("etl")
    for attempt in range(1, max_retries + 1):
        try:
            with get_connection():
                logger.info("Подключение к БД установлено (попытка %d)", attempt)
                return
        except psycopg2.OperationalError as exc:
            logger.warning(
                "БД ещё не готова (попытка %d/%d): %s",
                attempt,
                max_retries,
                exc,
            )
            time.sleep(delay)
    raise RuntimeError("Не удалось дождаться готовности БД")


def main() -> None:
    setup_logging()
    logger = logging.getLogger("etl")

    try:
        # Extract
        sales_df = read_sales_csv(DATA_DIR / "sales.csv")
        customers_df = read_customers_csv(DATA_DIR / "customers.csv")

        # Transform
        sales_df = transform_sales(sales_df)
        customers_df = transform_customers(customers_df)
        sales_summary_df = create_sales_summary(sales_df)
        product_ranking_df = create_product_ranking(sales_df)
        avg_check_by_region_df = create_avg_check_by_region(sales_df, customers_df)
        logger.info("Средний чек по регионам:\n%s", avg_check_by_region_df.to_string(index=False))

        wait_for_db()

        # Load
        with get_connection() as conn:
            create_tables(conn)
            truncate_tables(conn)
            load_sales(conn, sales_df)
            load_customers(conn, customers_df)
            load_sales_summary(conn, sales_summary_df)
            load_product_ranking(conn, product_ranking_df)

        logger.info("ETL-процесс успешно завершён")

    except Exception:
        logger.exception("ETL-процесс завершился с ошибкой")
        sys.exit(1)


if __name__ == "__main__":
    main()

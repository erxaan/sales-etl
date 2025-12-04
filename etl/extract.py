"""Модуль Extract: чтение исходных CSV-файлов."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

SALES_REQUIRED_COLUMNS: Sequence[str] = (
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "order_date",
    "category",
)

CUSTOMERS_REQUIRED_COLUMNS: Sequence[str] = (
    "customer_id",
    "customer_name",
    "email",
    "registration_date",
    "region",
)


def _read_csv(
    path: Path,
    *,
    parse_dates: Optional[Iterable[str]] = None,
    required_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Общий помощник чтения CSV с обработкой ошибок и валидацией структуры."""
    logger.info("Читаем файл %s", path)
    try:
        df = pd.read_csv(path, parse_dates=list(parse_dates or []))
    except FileNotFoundError:
        logger.error("Файл %s не найден", path)
        raise
    except pd.errors.EmptyDataError:
        logger.error("Файл %s пустой", path)
        raise
    except Exception:
        logger.exception("Не удалось прочитать CSV %s", path)
        raise

    if required_columns:
        missing = sorted(set(required_columns) - set(df.columns))
        if missing:
            logger.error(
                "Файл %s не содержит обязательные колонки: %s",
                path,
                ", ".join(missing),
            )
            raise ValueError(f"Missing required columns in {path}: {missing}")

    logger.info("Файл %s успешно прочитан (%d строк, %d колонок)", path, len(df), len(df.columns))
    return df


def read_sales_csv(path: Path) -> pd.DataFrame:
    """Читает sales.csv и приводит дату заказа."""
    return _read_csv(
        path,
        parse_dates=["order_date"],
        required_columns=SALES_REQUIRED_COLUMNS,
    )


def read_customers_csv(path: Path) -> pd.DataFrame:
    """Читает customers.csv и приводит дату регистрации."""
    return _read_csv(
        path,
        parse_dates=["registration_date"],
        required_columns=CUSTOMERS_REQUIRED_COLUMNS,
    )

"""Модуль Transform: очистка данных и расчёт агрегатов."""

from __future__ import annotations

import logging
import re
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def transform_sales(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Очищает и обогащает данные о продажах."""
    logger.info("Начинаем обработку продаж (%d строк)", len(df_sales))
    df = df_sales.copy()

    # Приведение даты
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    invalid_order_dates = int(df["order_date"].isna().sum())
    if invalid_order_dates:
        logger.warning("Не удалось распарсить order_date у %d строк", invalid_order_dates)

    # Вычисление производных колонок
    df["total_price"] = df["quantity"] * df["unit_price"]
    df["month"] = df["order_date"].dt.to_period("M").astype(str)

    # Поиск дубликатов
    dedup_subset: List[str] = ["order_id", "product_id", "quantity", "unit_price"]
    duplicates_mask = df.duplicated(subset=dedup_subset, keep=False)
    duplicates_count = int(duplicates_mask.sum())
    if duplicates_count:
        logger.warning("Обнаружено %d дубликатов заказов, удаляем их", duplicates_count)
        df = df.drop_duplicates(subset=dedup_subset, keep="first")

    # Удаление строк с пропусками в критических полях
    required_cols = ["order_id", "customer_id", "order_date", "quantity", "unit_price"]
    missing_mask = df[required_cols].isna().any(axis=1)
    missing_count = int(missing_mask.sum())
    if missing_count:
        logger.warning("Удаляем %d строк с пропусками в критических полях", missing_count)
        df = df[~missing_mask].copy()

    # Заполнение категории значением по умолчанию
    category_missing = int(df["category"].isna().sum())
    if category_missing:
        logger.warning("Подставляем 'Unknown' для %d строк без категории", category_missing)
        df["category"] = df["category"].fillna("Unknown")

    logger.info("Обработка продаж завершена (%d строк)", len(df))
    return df


def transform_customers(
    df_customers: pd.DataFrame,
    snapshot_date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Очищает и расширяет данные о клиентах."""
    logger.info("Начинаем обработку клиентов (%d строк)", len(df_customers))
    df = df_customers.copy()

    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    invalid_registration_dates = int(df["registration_date"].isna().sum())
    if invalid_registration_dates:
        logger.warning(
            "Не удалось распарсить registration_date у %d клиентов",
            invalid_registration_dates,
        )

    # Удаление записей без идентификатора клиента
    missing_ids = df["customer_id"].isna()
    missing_id_count = int(missing_ids.sum())
    if missing_id_count:
        logger.warning("Удаляем %d записей без customer_id", missing_id_count)
        df = df[~missing_ids].copy()

    # Валидация email
    email_pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    emails = df["email"].fillna("")
    df["is_email_valid"] = emails.apply(lambda value: bool(email_pattern.match(value)))
    invalid_emails = int((~df["is_email_valid"]).sum())
    if invalid_emails:
        logger.warning("Найдено %d невалидных email", invalid_emails)

    # Заполнение региона
    region_missing = int(df["region"].isna().sum())
    if region_missing:
        logger.warning("Подставляем 'Unknown' для %d клиентов без региона", region_missing)
        df["region"] = df["region"].fillna("Unknown")

    # Срок отношений с клиентом
    reference_date = (snapshot_date or pd.Timestamp.today()).normalize()
    df["customer_days"] = (reference_date - df["registration_date"]).dt.days

    logger.info("Обработка клиентов завершена (%d строк)", len(df))
    return df


def create_sales_summary(df_sales: pd.DataFrame) -> pd.DataFrame:
    """Возвращает агрегацию продаж по категориям и месяцам."""
    group = df_sales.groupby(["category", "month"], dropna=False)
    summary = (
        group.agg(total_sales=("total_price", "sum"), total_quantity=("quantity", "sum"))
        .reset_index()
    )

    order_counts = group["order_id"].nunique().reset_index(name="order_count")
    summary = summary.merge(order_counts, on=["category", "month"], how="left")

    summary["average_order_value"] = summary["total_sales"] / summary["order_count"].replace({0: pd.NA})
    summary["average_order_value"] = summary["average_order_value"].fillna(0)
    summary["period_date"] = pd.to_datetime(summary["month"] + "-01")
    summary = summary.drop(columns=["order_count"])

    logger.info("Сводная таблица продаж сформирована (%d строк)", len(summary))
    return summary


def create_avg_check_by_region(df_sales: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """Рассчитывает средний чек по регионам."""
    order_totals = (
        df_sales.groupby(["order_id", "customer_id"], as_index=False)["total_price"]
        .sum()
        .rename(columns={"total_price": "order_total"})
    )

    enriched = order_totals.merge(
        df_customers[["customer_id", "region"]],
        on="customer_id",
        how="left",
    )
    enriched["region"] = enriched["region"].fillna("Unknown")

    result = (
        enriched.groupby("region", as_index=False)
        .agg(avg_check=("order_total", "mean"), orders_count=("order_id", "nunique"))
        .sort_values("avg_check", ascending=False)
    )

    logger.info("Средний чек по регионам рассчитан (%d регионов)", len(result))
    return result


def create_product_ranking(df_sales: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """Возвращает рейтинг самых продаваемых товаров."""
    ranking = (
        df_sales.groupby(["product_id", "product_name"], as_index=False)
        .agg(total_sold=("quantity", "sum"), total_revenue=("total_price", "sum"))
        .sort_values(["total_sold", "total_revenue"], ascending=False)
    )

    ranking = ranking.head(top_n).reset_index(drop=True)
    ranking["rank_position"] = range(1, len(ranking) + 1)

    logger.info("Сформирован рейтинг товаров (топ %d)", len(ranking))
    return ranking

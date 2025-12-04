import pandas as pd

from etl.transform import (
    create_product_ranking,
    create_sales_summary,
    transform_customers,
    transform_sales,
)


def test_transform_sales_deduplicates_and_enriches():
    source = pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": "C1",
                "product_id": "P1",
                "product_name": "Prod1",
                "quantity": 1,
                "unit_price": 100.0,
                "order_date": "2024-01-01",
                "category": "Tech",
            },
            {
                "order_id": 1,
                "customer_id": "C1",
                "product_id": "P1",
                "product_name": "Prod1",
                "quantity": 1,
                "unit_price": 100.0,
                "order_date": "2024-01-01",
                "category": "Tech",
            },
            {
                "order_id": 2,
                "customer_id": None,
                "product_id": "P2",
                "product_name": "Prod2",
                "quantity": 2,
                "unit_price": 50.0,
                "order_date": "2024-01-03",
                "category": "Books",
            },
            {
                "order_id": 3,
                "customer_id": "C3",
                "product_id": "P3",
                "product_name": "Prod3",
                "quantity": 2,
                "unit_price": 50.0,
                "order_date": "2024-02-10",
                "category": None,
            },
        ]
    )

    result = transform_sales(source)

    assert len(result) == 2  # дубликат и строка с пропуском customer_id удалены
    first_row = result[result["order_id"] == 1].iloc[0]
    assert first_row["total_price"] == 100.0
    assert first_row["month"] == "2024-01"

    third_row = result[result["order_id"] == 3].iloc[0]
    assert third_row["category"] == "Unknown"
    assert third_row["total_price"] == 100.0


def test_transform_customers_validates_email_and_computes_days():
    snapshot = pd.Timestamp("2024-02-01")
    source = pd.DataFrame(
        [
            {
                "customer_id": "C1",
                "customer_name": "Ivan",
                "email": "ivan@example.com",
                "registration_date": "2024-01-01",
                "region": "Москва",
            },
            {
                "customer_id": "C2",
                "customer_name": "Anna",
                "email": "invalid-email",
                "registration_date": "2024-01-15",
                "region": None,
            },
        ]
    )

    result = transform_customers(source, snapshot_date=snapshot)

    assert result.loc[result["customer_id"] == "C1", "customer_days"].iloc[0] == 31
    assert (
        result.loc[result["customer_id"] == "C2", "is_email_valid"].iloc[0] is False
    )
    assert result.loc[result["customer_id"] == "C2", "region"].iloc[0] == "Unknown"


def test_create_sales_summary_aggregates_correctly():
    df_sales = pd.DataFrame(
        [
            {"order_id": 1, "category": "Tech", "quantity": 1, "total_price": 100.0, "month": "2024-01"},
            {"order_id": 1, "category": "Tech", "quantity": 2, "total_price": 200.0, "month": "2024-01"},
            {"order_id": 2, "category": "Books", "quantity": 1, "total_price": 50.0, "month": "2024-02"},
        ]
    )

    summary = create_sales_summary(df_sales)

    tech_row = summary[summary["category"] == "Tech"].iloc[0]
    assert tech_row["total_sales"] == 300.0
    assert tech_row["total_quantity"] == 3
    assert tech_row["average_order_value"] == 300.0
    assert tech_row["period_date"] == pd.Timestamp("2024-01-01")


def test_create_product_ranking_limits_top_n():
    df_sales = pd.DataFrame(
        [
            {"product_id": "P1", "product_name": "Prod1", "quantity": 5, "total_price": 500.0},
            {"product_id": "P2", "product_name": "Prod2", "quantity": 3, "total_price": 150.0},
            {"product_id": "P3", "product_name": "Prod3", "quantity": 7, "total_price": 210.0},
        ]
    )

    ranking = create_product_ranking(df_sales, top_n=2)

    assert len(ranking) == 2
    assert ranking.iloc[0]["product_id"] == "P3"
    assert list(ranking["rank_position"]) == [1, 2]


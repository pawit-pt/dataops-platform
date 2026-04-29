"""
DAG: gold_aggregation
Schedule: Hourly (@hourly)
Purpose: Populate gold.trips_hourly from silver.trips via Trino INSERT.

Data Quality Gate:
  Before Gold aggregation runs, 3 quality checks must pass:
  1. Freshness  — Silver has data in the last 2 hours
  2. Validity   — Invalid-record ratio stays below 10%
  3. Null check — No nulls in critical fields (trip_id, route_name, base_fare)

  If ANY check fails → Gold INSERT is blocked → users never see bad data.

Connection: trino_default (user=airflow -> adhoc resource group)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------------------------
# DATA QUALITY CHECKS (must all pass before Gold INSERT)
# ----------------------------------------------------------------

# DQ-1: Freshness — Silver table must have recent data (last 2 hours)
# Returns TRUE when fresh data exists → task succeeds
# Returns FALSE when no recent data  → task fails, Gold is blocked
CHECK_FRESHNESS_SQL = """
SELECT COUNT(*) > 0
FROM iceberg.silver.trips
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
"""

# DQ-2: Validity — Less than 10% of today's records should be invalid
# Returns TRUE when invalid ratio < 10% → task succeeds
# Returns FALSE when too many bad records → task fails, Gold is blocked
CHECK_INVALID_RATIO_SQL = """
SELECT
  CAST(COUNT(*) FILTER (WHERE is_valid = false) AS DOUBLE)
    / NULLIF(COUNT(*), 0) < 0.1
FROM iceberg.silver.trips
WHERE event_date >= CURRENT_DATE
"""

# DQ-3: Null check — Critical fields must not be null in recent data
# Returns TRUE when zero nulls found → task succeeds
# Returns FALSE when nulls exist     → task fails, Gold is blocked
CHECK_NULL_CRITICAL_SQL = """
SELECT COUNT(*) = 0
FROM iceberg.silver.trips
WHERE (trip_id IS NULL OR route_name IS NULL OR base_fare IS NULL)
  AND event_date >= CURRENT_DATE
"""

# ----------------------------------------------------------------
# GOLD AGGREGATION: Silver -> Gold (only runs after DQ passes)
# Source: trino_queries.sql (Gold Population section)
# ----------------------------------------------------------------
GOLD_INSERT_SQL = """
INSERT INTO iceberg.gold.trips_hourly
SELECT
  date_trunc('hour', created_at)          AS report_hour,
  route_name,
  COUNT(*)                                AS trip_count,
  SUM(base_fare)                          AS total_revenue,
  SUM(base_fare) / COUNT(*)              AS avg_fare,
  COUNT(DISTINCT driver_id)               AS unique_drivers,
  current_timestamp                       AS updated_at,
  CAST(date_trunc('hour', created_at) AS DATE) AS report_date
FROM iceberg.silver.trips
WHERE is_valid = true
  AND event_date >= CURRENT_DATE - INTERVAL '1' DAY
GROUP BY date_trunc('hour', created_at), route_name
"""

with DAG(
    dag_id="gold_aggregation",
    default_args=default_args,
    description="Hourly: DQ checks -> aggregate silver.trips -> gold.trips_hourly",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "aggregation", "trino", "data-quality"],
) as dag:

    # ---- Data Quality Gate (all 3 run in parallel) ----

    check_freshness = SQLCheckOperator(
        task_id="dq_check_freshness",
        conn_id="trino_default",
        sql=CHECK_FRESHNESS_SQL,
    )

    check_invalid_ratio = SQLCheckOperator(
        task_id="dq_check_invalid_ratio",
        conn_id="trino_default",
        sql=CHECK_INVALID_RATIO_SQL,
    )

    check_null_fields = SQLCheckOperator(
        task_id="dq_check_null_critical_fields",
        conn_id="trino_default",
        sql=CHECK_NULL_CRITICAL_SQL,
    )

    # ---- Gold Aggregation (only runs if ALL checks pass) ----

    populate_gold = TrinoOperator(
        task_id="insert_gold_trips_hourly",
        trino_conn_id="trino_default",
        sql=GOLD_INSERT_SQL,
    )

    # DQ checks run in parallel → all must pass → then Gold INSERT
    [check_freshness, check_invalid_ratio, check_null_fields] >> populate_gold

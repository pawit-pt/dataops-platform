"""
DAG: test_dq_and_batch
Schedule: Every 5 minutes (*/5 * * * *)
Purpose: Quick-cycle test DAG to verify DQ gates and batch Gold aggregation.

Runs the same 3 DQ checks as gold_aggregation, then a Gold INSERT.
Use this to validate the full pipeline without waiting an hour.

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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ----------------------------------------------------------------
# DATA QUALITY CHECKS (same rules as gold_aggregation)
# ----------------------------------------------------------------

CHECK_FRESHNESS_SQL = """
SELECT COUNT(*) > 0
FROM iceberg.silver.trips
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
"""

CHECK_INVALID_RATIO_SQL = """
SELECT
  CAST(COUNT(*) FILTER (WHERE is_valid = false) AS DOUBLE)
    / NULLIF(COUNT(*), 0) < 0.1
FROM iceberg.silver.trips
WHERE event_date >= CURRENT_DATE
"""

CHECK_NULL_CRITICAL_SQL = """
SELECT COUNT(*) = 0
FROM iceberg.silver.trips
WHERE (trip_id IS NULL OR route_name IS NULL OR base_fare IS NULL)
  AND event_date >= CURRENT_DATE
"""

# ----------------------------------------------------------------
# GOLD AGGREGATION: Silver -> Gold (same as gold_aggregation)
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
    dag_id="test_dq_and_batch",
    default_args=default_args,
    description="Every 5 min: DQ checks -> aggregate silver.trips -> gold.trips_hourly (test DAG)",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "gold", "data-quality", "batch"],
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

    # ---- Gold Aggregation (only runs if ALL DQ checks pass) ----

    populate_gold = TrinoOperator(
        task_id="insert_gold_trips_hourly",
        trino_conn_id="trino_default",
        sql=GOLD_INSERT_SQL,
    )

    # DQ checks run in parallel -> all must pass -> then Gold INSERT
    [check_freshness, check_invalid_ratio, check_null_fields] >> populate_gold

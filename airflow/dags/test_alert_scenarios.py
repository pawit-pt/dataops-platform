"""
DAG: test_alert_scenarios
Schedule: Manual trigger only (no schedule)
Purpose: Intentionally trigger alert failures to validate Prometheus alert rules.

This DAG simulates REAL failure scenarios that map to alert-rules.yml:

  Scenario 1: DQ Freshness FAIL → triggers DQGateFreshnessFailed alert
  Scenario 2: DQ Validity  FAIL → triggers DQGateValidityFailed alert
  Scenario 3: DQ Null      FAIL → triggers DQGateNullCheckFailed alert
  Scenario 4: Gold INSERT  FAIL → triggers GoldAggregationFailed alert

How it works:
  Each DQ check uses SQL that is DESIGNED TO RETURN FALSE, causing
  SQLCheckOperator to mark the task as FAILED. Airflow emits a StatsD
  counter (state="failed") which Prometheus scrapes via statsd-exporter.
  The alert rules detect the increase in failed counter and FIRE.

Usage:
  # Trigger from CLI:
  docker exec -it airflow-webserver airflow dags trigger test_alert_scenarios

  # Or trigger from Airflow UI: http://localhost:8085

After triggering:
  1. Airflow UI  → shows tasks as FAILED (red)
  2. Prometheus  → http://localhost:9090/alerts shows FIRING alerts
  3. Grafana     → DQ Gate panels show FAIL status

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
    "retries": 0,          # NO retries — we want immediate failure
    "retry_delay": timedelta(minutes=1),
}

# ----------------------------------------------------------------
# SCENARIO 1: DQ Freshness FAIL
# This query checks for data in the FUTURE (year 2099), which will
# never exist. COUNT(*) = 0, so "COUNT(*) > 0" returns FALSE.
# SQLCheckOperator treats FALSE as failure → task state = "failed"
# Triggers: DQGateFreshnessFailed alert
# ----------------------------------------------------------------
FAIL_FRESHNESS_SQL = """
SELECT COUNT(*) > 0
FROM iceberg.silver.trips
WHERE created_at >= TIMESTAMP '2099-01-01 00:00:00'
"""

# ----------------------------------------------------------------
# SCENARIO 2: DQ Validity FAIL
# This query forces the invalid ratio to appear as 100% by checking
# an impossible condition. The expression evaluates to 1.0 < 0.1
# which is FALSE → task fails.
# Triggers: DQGateValidityFailed alert
# ----------------------------------------------------------------
FAIL_INVALID_RATIO_SQL = """
SELECT 1.0 < 0.1
"""

# ----------------------------------------------------------------
# SCENARIO 3: DQ Null Check FAIL
# This query checks if "there are zero nulls" but we force it to
# find nulls by using a UNION with a NULL row. The COUNT > 0, so
# "COUNT(*) = 0" returns FALSE → task fails.
# Triggers: DQGateNullCheckFailed alert
# ----------------------------------------------------------------
FAIL_NULL_CHECK_SQL = """
SELECT COALESCE(
    (SELECT COUNT(*) = 0
     FROM (
         SELECT CAST(NULL AS VARCHAR) AS trip_id
     ) t
     WHERE trip_id IS NULL),
    false
)
"""

# ----------------------------------------------------------------
# SCENARIO 4: Gold INSERT FAIL
# This INSERT references a non-existent table (silver.trips_does_not_exist)
# which causes Trino to throw a TABLE_NOT_FOUND error → task fails.
# Triggers: GoldAggregationFailed alert
# ----------------------------------------------------------------
FAIL_GOLD_INSERT_SQL = """
INSERT INTO iceberg.gold.trips_hourly
SELECT
  date_trunc('hour', CURRENT_TIMESTAMP)     AS report_hour,
  'ALERT_TEST_ROUTE'                         AS route_name,
  0                                          AS trip_count,
  0.0                                        AS total_revenue,
  0.0                                        AS avg_fare,
  0                                          AS unique_drivers,
  CURRENT_TIMESTAMP                          AS updated_at,
  CURRENT_DATE                               AS report_date
FROM iceberg.silver.trips_does_not_exist
"""

with DAG(
    dag_id="test_alert_scenarios",
    default_args=default_args,
    description="Manual: trigger intentional failures to test Prometheus alert rules",
    schedule_interval=None,       # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "alerts", "data-quality", "scenarios"],
) as dag:

    # ---- Scenario 1: Freshness check designed to FAIL ----
    fail_freshness = SQLCheckOperator(
        task_id="dq_check_freshness",
        conn_id="trino_default",
        sql=FAIL_FRESHNESS_SQL,
    )

    # ---- Scenario 2: Validity check designed to FAIL ----
    fail_validity = SQLCheckOperator(
        task_id="dq_check_invalid_ratio",
        conn_id="trino_default",
        sql=FAIL_INVALID_RATIO_SQL,
    )

    # ---- Scenario 3: Null check designed to FAIL ----
    fail_null = SQLCheckOperator(
        task_id="dq_check_null_critical_fields",
        conn_id="trino_default",
        sql=FAIL_NULL_CHECK_SQL,
    )

    # ---- Scenario 4: Gold INSERT designed to FAIL ----
    fail_gold_insert = TrinoOperator(
        task_id="insert_gold_trips_hourly",
        trino_conn_id="trino_default",
        sql=FAIL_GOLD_INSERT_SQL,
    )

    # All 4 scenarios run in parallel (each independently fails)
    # This mirrors the gold_aggregation DAG structure but ALL tasks fail
    [fail_freshness, fail_validity, fail_null] >> fail_gold_insert

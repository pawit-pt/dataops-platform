"""
DAG: team_b_hourly_monitoring
Schedule: Hourly (@hourly)
Purpose: Execute Team B monitoring queries against Trino.

Connection: trino_team_b (user=team_b_airflow)
  -> matches resource group selector: team_b.*
  -> routed to: global.team_b_monitoring
  -> limits: 50 concurrent queries, 30% memory, schedulingWeight=50

Team B runs fast, frequent monitoring queries on Gold tables.
Tasks run sequentially: check performance first, then detect anomalies.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    "owner": "team_b",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------------------------
# Query 1: Current hour performance
# Source: trino_queries.sql (Team B section)
# Fast scan on gold.trips_hourly for current hour
# ----------------------------------------------------------------
CURRENT_HOUR_PERFORMANCE_SQL = """
SELECT
  route_name,
  SUM(trip_count)      AS trips_this_hour,
  SUM(total_revenue)   AS revenue_this_hour
FROM iceberg.gold.trips_hourly
WHERE report_hour >= DATE_TRUNC('hour', NOW())
GROUP BY route_name
ORDER BY revenue_this_hour DESC
"""

# ----------------------------------------------------------------
# Query 2: Revenue drop alert (>20% drop vs previous hour)
# Source: trino_queries.sql (Team B section)
# Compares current hour vs last hour by route
# ----------------------------------------------------------------
REVENUE_DROP_ALERT_SQL = """
WITH current_hour AS (
  SELECT route_name, SUM(total_revenue) AS revenue
  FROM iceberg.gold.trips_hourly
  WHERE report_hour >= DATE_TRUNC('hour', NOW())
  GROUP BY route_name
),
last_hour AS (
  SELECT route_name, SUM(total_revenue) AS revenue
  FROM iceberg.gold.trips_hourly
  WHERE report_hour >= DATE_TRUNC('hour', NOW()) - INTERVAL '1' HOUR
    AND report_hour <  DATE_TRUNC('hour', NOW())
  GROUP BY route_name
)
SELECT
  c.route_name,
  l.revenue                                                        AS last_hour_revenue,
  c.revenue                                                        AS current_hour_revenue,
  ROUND((c.revenue - l.revenue) / NULLIF(l.revenue, 0) * 100, 1) AS pct_change
FROM current_hour c
JOIN last_hour l ON c.route_name = l.route_name
WHERE c.revenue < l.revenue * 0.8
ORDER BY pct_change ASC
"""

with DAG(
    dag_id="team_b_hourly_monitoring",
    default_args=default_args,
    description="Team B: hourly monitoring (performance + revenue drop alerts)",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["team_b", "monitoring", "hourly"],
) as dag:

    current_performance = TrinoOperator(
        task_id="current_hour_performance",
        trino_conn_id="trino_team_b",
        sql=CURRENT_HOUR_PERFORMANCE_SQL,
    )

    revenue_alerts = TrinoOperator(
        task_id="revenue_drop_alerts",
        trino_conn_id="trino_team_b",
        sql=REVENUE_DROP_ALERT_SQL,
    )

    # Sequential: check performance first, then detect anomalies
    current_performance >> revenue_alerts

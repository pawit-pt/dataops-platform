"""
DAG: team_a_daily_reports
Schedule: Daily at 10:00 AM (0 10 * * *)
Purpose: Execute Team A report queries against Trino.

Connection: trino_team_a (user=team_a_airflow)
  -> matches resource group selector: team_a.*
  -> routed to: global.team_a_reports
  -> limits: 20 concurrent queries, 40% memory, schedulingWeight=30

Team A runs large daily report scans on Gold + Silver tables.
Both queries execute in parallel since they are independent reads.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    "owner": "team_a",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ----------------------------------------------------------------
# Query 1: Daily revenue by route
# Source: trino_queries.sql (Team A section)
# Scans gold.trips_hourly for yesterday
# ----------------------------------------------------------------
DAILY_REVENUE_BY_ROUTE_SQL = """
SELECT
  report_date,
  route_name,
  SUM(trip_count)      AS total_trips,
  SUM(total_revenue)   AS total_revenue,
  AVG(avg_fare)        AS avg_fare
FROM iceberg.gold.trips_hourly
WHERE report_date = CURRENT_DATE - INTERVAL '1' DAY
GROUP BY report_date, route_name
ORDER BY total_revenue DESC
"""

# ----------------------------------------------------------------
# Query 2: Weekly top routes
# Source: trino_queries.sql (Team A section)
# Scans gold.trips_hourly for last 7 days
# ----------------------------------------------------------------
WEEKLY_TOP_ROUTES_SQL = """
SELECT
  route_name,
  SUM(trip_count)      AS total_trips,
  SUM(total_revenue)   AS total_revenue
FROM iceberg.gold.trips_hourly
WHERE report_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY route_name
ORDER BY total_revenue DESC
"""

with DAG(
    dag_id="team_a_daily_reports",
    default_args=default_args,
    description="Team A: daily 10am reports (revenue by route + top routes)",
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["team_a", "reports", "daily"],
) as dag:

    daily_revenue = TrinoOperator(
        task_id="daily_revenue_by_route",
        trino_conn_id="trino_team_a",
        sql=DAILY_REVENUE_BY_ROUTE_SQL,
    )

    weekly_routes = TrinoOperator(
        task_id="weekly_top_routes",
        trino_conn_id="trino_team_a",
        sql=WEEKLY_TOP_ROUTES_SQL,
    )

    # Both queries run in parallel -- independent reads from gold layer
    [daily_revenue, weekly_routes]

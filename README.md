# Data Platform Architecture

> Real-time streaming data platform with batch scheduling, data quality gates, and workload isolation.

```
External Data  →  Kafka  →  Flink (Stream ETL)  →  Iceberg (Bronze + Silver)
                                                          ↓
                                            Airflow DQ Gate (Freshness / Validity / Nulls)
                                                          ↓  PASS
                                                   Iceberg (Gold)
                                                          ↓
                                                  Trino (SQL Engine)
                                                   ↙     ↓     ↘
                                              Team A   Team B   BI Tools
```

---

## Table of Contents

1. [Architecture Decision & Rationale](#architecture-decision--rationale)
2. [Tech Stack](#tech-stack)
3. [Setup & Deployment Guide](#setup--deployment-guide)
4. [ETL Pipeline Guidelines](#etl-pipeline-guidelines)
5. [Connecting & Querying Data](#connecting--querying-data)
6. [Monitoring & Observability](#monitoring--observability)
7. [Security & Governance](#security--governance)
8. [Containerization & Kubernetes Path](#containerization--kubernetes-path)
9. [Cost Awareness & Justification](#cost-awareness--justification)
10. [Platform Improvement Ideas](#platform-improvement-ideas)
11. [Fault Tolerance](#fault-tolerance)
12. [Maintenance](#maintenance)
13. [Project Structure](#project-structure)

---

## Architecture Decision & Rationale

### Why Lakehouse over Data Warehouse?

| Criteria | Traditional DW (Snowflake/BigQuery) | Lakehouse (This Platform) | Reason for Choice |
|----------|-------------------------------------|---------------------------|-------------------|
| **Vendor Lock-in** | High (proprietary formats) | None (open formats: Parquet + Iceberg) | Portability between clouds |
| **Real-time Support** | Limited (micro-batch) | Native (Flink streaming) | Sub-second ingestion needed |
| **Cost Model** | Pay-per-query (unpredictable) | Fixed infra cost (predictable) | Budget control |
| **Storage Format** | Proprietary | Open (Parquet + Iceberg) | Any engine can read the data |
| **Compute/Storage** | Coupled or semi-separated | Fully separated (Trino + MinIO) | Scale independently |

### Why Each Technology Was Chosen

| Component | Chosen | Common Considered | Decision Rationale |
|-----------|--------|------------------------|-------------------|
| **Message Broker** | Apache Kafka | RabbitMQ, Pulsar | Kafka excels at high-throughput ordered event streaming with replay capability. Flink has native Kafka connector. Industry standard for event-driven architectures. |
| **Stream Processing** | Apache Flink | Spark Structured Streaming, Kafka Streams | Flink provides true record-at-a-time processing with EXACTLY_ONCE semantics. Lower latency than Spark micro-batch. SQL-first API reduces development time. |
| **Table Format** | Apache Iceberg | Delta Lake, Apache Hudi | Iceberg is engine-agnostic (works with Flink, Trino, Spark). ACID transactions, time travel, hidden partitioning. No vendor lock-in (unlike Delta Lake tied to Databricks). |
| **Object Storage** | MinIO | AWS S3, HDFS | S3-compatible API means zero code changes when migrating to cloud. Runs locally for development. Production would swap to AWS S3 or GCS. |
| **Query Engine** | Trino | Spark SQL, Presto, Dremio | Trino is purpose-built for interactive analytics. Resource groups enable workload isolation. JDBC-compatible for any BI tool. |
| **Orchestrator** | Apache Airflow | Dagster, Prefect | Airflow is the industry standard. Rich operator ecosystem. TrinoOperator for SQL-based DQ gates. Mature monitoring and alerting. |
| **Monitoring** | Prometheus + Grafana | Datadog, ELK Stack | Open-source, zero license cost. Native exporters for Kafka, Flink, MinIO. PromQL for flexible alerting rules. |

### Why Medallion Architecture (Bronze → Silver → Gold)?

```
┌──────────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│      BRONZE          │     │       SILVER         │     │        GOLD          │
│  (Raw / Append-Only) │ ──→ │  (Cleaned / Valid)   │ ──→ │  (Aggregated / DQ)   │
│                      │     │                      │     │                      │
│  • No transformation │     │  • is_valid flag     │     │  • Hourly rollups    │
│  • Full fidelity     │     │  • Null handling     │     │  • DQ-gated writes   │
│  • Replayable        │     │  • Type enforcement  │     │  • Business-ready    │
└──────────────────────┘     └──────────────────────┘     └──────────────────────┘
```

- **Bronze**: Raw data exactly as received. Acts as the "source of truth" and enables replay.
- **Silver**: Cleaned and validated. Filters out invalid records (`base_fare > 0`, `customer_id > 0`, `trip_id IS NOT NULL`).
- **Gold**: Business-aggregated. Only populated after Data Quality gates pass. Consumed by teams and BI tools.

This layering ensures data quality improves at each stage while preserving the ability to reprocess from raw data.

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Message Broker** | Apache Kafka | 7.5.0 (Confluent) | Real-time event streaming with 7-day retention |
| **Stream Processing** | Apache Flink | 1.18 | EXACTLY_ONCE stream ETL (Bronze + Silver layers) |
| **Table Format** | Apache Iceberg | 1.5.2 | ACID transactions, time travel, partition pruning |
| **Object Storage** | MinIO | latest | S3-compatible lakehouse storage (Parquet + Snappy) |
| **Catalog** | Iceberg REST Catalog | 0.6.0 (tabulario) | Centralized table metadata management |
| **Scheduler** | Apache Airflow | 2.8.1 | DAG orchestration, DQ gates, scheduled queries |
| **Query Engine** | Trino | 435 | Distributed SQL with resource group isolation |
| **Metadata DB** | PostgreSQL | 15 | Hive Metastore + Airflow backend |
| **Coordination** | ZooKeeper | 7.5.0 (Confluent) | Kafka broker coordination |
| **Monitoring** | Prometheus | latest | Metrics collection (Kafka, Flink, Airflow, MinIO) |
| **Visualization** | Grafana | latest | Metrics dashboards & alerting |
| **Container** | Docker Compose | - | 18 services (16 long-running + 2 init) on shared `platform` bridge network |

---

## Setup & Deployment Guide

### Prerequisites

- **Docker Desktop** (v4.0+) with at least **8 GB RAM** allocated
- **Docker Compose** v2+
- **Ports available:** 2181, 3000, 5432, 8080, 8081, 8082, 8085, 8181, 9000, 9001, 9090, 9092, 9101

```bash
# Verify Docker is running
docker --version
docker compose version
docker login
```

### Step 1. Clone and navigate to the project

```bash
cd problem_2_answer
```

### Step 2. Pre-pull base Docker images

Pull the large base images first to avoid timeouts during the build step:

```bash
docker pull apache/airflow:2.8.1-python3.11
docker pull apache/flink:1.18-scala_2.12-java11
docker pull python:3.11-slim
```

### Step 3. Download Flink connector JARs

```bash
chmod +x download-jars.sh
./download-jars.sh
```

This downloads 9 JAR files into `flink-libs/`:
- Kafka SQL connector, Iceberg Flink runtime, S3 Hadoop filesystem
- Hadoop client libraries, AWS bundle, XML parsers
- Flink Prometheus metrics reporter

### Step 4. Build custom Docker images

```bash
docker compose build
```

This builds 3 custom images:
- **Flink** — Flink 1.18 with Iceberg/Kafka/S3 connectors + Prometheus reporter
- **Airflow** — Airflow 2.8.1 with `apache-airflow-providers-trino`
- **Data Generator** — Python ride-sharing trip event simulator

### Step 5. Start the platform

```bash
docker compose up -d
```

### Step 6. Wait for services to be healthy (~90 seconds)

```bash
# Check all containers are running
docker compose ps

# Watch logs for startup progress
docker compose logs -f --tail=20
```

**Startup order** (handled automatically by `depends_on` + health checks):

```
ZooKeeper → Kafka → Kafka UI + Kafka Exporter
PostgreSQL → Iceberg REST → MinIO → Flink → Trino
PostgreSQL → Airflow Init → Airflow Webserver + Scheduler
Data Generator (starts after Kafka is ready)
StatsD Exporter → Prometheus → Grafana
```

### Step 7. Submit the Flink ETL pipeline

```bash
# Submit the full ETL pipeline (creates tables + starts streaming jobs)
docker exec -it flink-jobmanager bash -c \
  "./bin/sql-client.sh -f /opt/flink/jobs/etl_pipeline.sql"
```

This creates:
- Bronze table: `iceberg.bronze.trips` (raw append)
- Silver table: `iceberg.silver.trips` (cleaned + validated)
- Gold table: `iceberg.gold.trips_hourly` (schema only, populated by Airflow)
- One combined streaming INSERT job (STATEMENT SET): Kafka → Bronze + Silver simultaneously

### Step 8. Verify the pipeline is working

```bash
# Check Flink jobs are running
docker exec -it flink-jobmanager bash -c \
  "./bin/flink list"

# Query Silver layer via Trino (wait ~2 min for data to accumulate)
docker exec -it trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.trips"

# Check Airflow DAGs are loaded
docker exec -it airflow-webserver airflow dags list
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | none |
| **Flink Web UI** | http://localhost:8081 | none |
| **Trino Web UI** | http://localhost:8082 | user: any, password: empty |
| **Airflow Web UI** | http://localhost:8085 | admin / admin |
| **MinIO Console** | http://localhost:9001 | admin / password123 |
| **Iceberg REST** | http://localhost:8181 | none |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | none |

---

## ETL Pipeline Guidelines

### Data Flow Overview

```
  ┌──────────────┐    ┌─────────┐    ┌─────────────────────────┐    ┌───────────────┐
  │ Data Generator│───→│  Kafka  │───→│    Flink (Streaming)    │───→│ MinIO (S3)    │
  │ 5 events/sec  │    │  Topic: │    │                         │    │               │
  │ (Python)      │    │  raw.   │    │  Combined STATEMENT SET  │    │  warehouse/   │
  └──────────────┘    │  trip.  │    │  → Bronze + Silver      │    │  ├── bronze/  │
                       │  events │    │  (validates + cleans)   │    │  ├── silver/  │
                       └─────────┘    └─────────────────────────┘    │  └── gold/    │
                                                                      └───────────────┘
                                                                             │
                       ┌──────────────────────────────────────────┐          │
                       │         Airflow (Batch, @hourly)          │          │
                       │                                            │          │
                       │  1. DQ Freshness  ──┐                     │          │
                       │  2. DQ Validity   ──┼── ALL PASS? ──→ Gold INSERT    │
                       │  3. DQ Null Check ──┘      │              │          │
                       │                          FAIL → Block     │          │
                       └──────────────────────────────────────────┘          │
                                                                              │
                       ┌──────────────────────────────────────────┐          │
                       │         Trino (Query Engine)              │◄─────────┘
                       │  Team A (40% mem) │ Team B (30% mem)     │
                       │  BI Tools         │ Adhoc queries (20%)  │
                       └──────────────────────────────────────────┘
```

### Stage 1: Data Ingestion (Streaming)

**Producer** — Python generator (`data-generator/generator.py`) simulates 5 ride-sharing trip events per second, each containing:
- Trip metadata (trip_id, driver_id, customer_id)
- Route data (route_name, distance_km, coordinates)
- Pricing (base_fare, surge_multiplier, toll_charges)
- Transaction info (payment_type, status)

Events are published to Kafka topic `raw.trip.events` as JSON.

### Stage 2: Stream ETL (Flink SQL)

Flink runs **1 combined INSERT job** defined in `flink-jobs/etl_pipeline.sql` using a `STATEMENT SET` — both Bronze and Silver INSERTs execute as a single Flink job for atomicity:

**Bronze INSERT — Kafka → Bronze (raw append, no transformation)**
```
INSERT INTO iceberg.bronze.trips
SELECT * FROM kafka_raw_trips   -- full fidelity, exactly as received
```

**Silver INSERT — Kafka → Silver (cleaned + validated, same job)**
```
INSERT INTO iceberg.silver.trips
SELECT *,
  CASE WHEN base_fare > 0 AND customer_id > 0 AND trip_id IS NOT NULL
       THEN TRUE ELSE FALSE END AS is_valid
FROM kafka_raw_trips             -- adds validation flag
```

Key configuration:
- **Checkpointing**: Every 60 seconds with EXACTLY_ONCE semantics
- **State Backend**: RocksDB with S3-based checkpoints (`s3://checkpoints/flink`)
- **Output Format**: Parquet with Snappy compression, 128 MB target file size
- **Partitioning**: By `event_date` for efficient pruning

### Stage 3: Data Quality Gates (Airflow)

Before Gold layer is populated, 3 SQL checks run in parallel via `gold_aggregation` DAG:

| Check | SQL Logic | Threshold | Fail Action |
|-------|-----------|-----------|-------------|
| **Freshness** | `COUNT(*) WHERE created_at >= now() - 2h` | > 0 rows | Block Gold INSERT |
| **Validity** | `invalid_count / total_count` | < 10% | Block Gold INSERT |
| **Null Check** | `COUNT(*) WHERE trip_id IS NULL OR route_name IS NULL OR base_fare IS NULL` | = 0 rows | Block Gold INSERT |

Only when ALL 3 checks pass, the `insert_gold_trips_hourly` task runs.

### Stage 4: Gold Aggregation (Batch)

```sql
INSERT INTO gold.trips_hourly
SELECT
  date_trunc('hour', created_at)   AS report_hour,
  route_name,
  COUNT(*)                          AS trip_count,
  SUM(base_fare)                    AS total_revenue,
  AVG(base_fare)                    AS avg_fare,
  COUNT(DISTINCT driver_id)         AS unique_drivers,
  CURRENT_TIMESTAMP                 AS updated_at,
  CAST(date_trunc('hour', created_at) AS DATE) AS report_date
FROM silver.trips
WHERE is_valid = true
GROUP BY date_trunc('hour', created_at), route_name
```

### Stage 5: Team Consumption

| DAG | Schedule | Team | Queries |
|-----|----------|------|---------|
| `gold_aggregation` | `@hourly` | Platform | DQ gates → Gold INSERT |
| `team_a_daily_reports` | `0 10 * * *` | Team A | Daily revenue by route, weekly top routes |
| `team_b_hourly_monitoring` | `@hourly` | Team B | Hourly performance, revenue drop alerts (>20% drop) |

Each team's DAG connects via its own Trino user, automatically routing to the correct resource group.

---

## Connecting & Querying Data

### Option 1: Trino CLI (Docker)

```bash
# As Team A user (routes to team_a_reports resource group: 40% memory, 20 concurrent)
docker exec -it trino trino \
  --server localhost:8080 \
  --catalog iceberg \
  --schema gold \
  --user team_a_user

# As Team B user (routes to team_b_monitoring resource group: 30% memory, 50 concurrent)
docker exec -it trino trino \
  --server localhost:8080 \
  --catalog iceberg \
  --schema gold \
  --user team_b_user
```

### Option 2: JDBC (DBeaver, Tableau, Power BI, etc.)

| Setting | Value |
|---------|-------|
| **Driver** | Trino JDBC |
| **JDBC URL** | `jdbc:trino://localhost:8082/iceberg` |
| **Username** | `team_a_user` or `team_b_user` (routes to correct resource group) |
| **Password** | (leave empty) |
| **Default Schema** | `gold` (or `silver`, `bronze`) |

### Option 3: Python (PyTrino)

```python
from trino.dbapi import connect

conn = connect(
    host="localhost", port=8082,
    user="team_a_user", catalog="iceberg", schema="gold"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM trips_hourly ORDER BY report_hour DESC LIMIT 10")
rows = cursor.fetchall()
```

### Example Queries

**Gold Layer — Hourly Revenue**
```sql
SELECT report_hour, route_name, total_revenue, trip_count
FROM iceberg.gold.trips_hourly
ORDER BY report_hour DESC LIMIT 20;
```

**Silver Layer — Data Quality Check**
```sql
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE is_valid = false) AS invalid,
  ROUND(CAST(COUNT(*) FILTER (WHERE is_valid = false) AS DOUBLE) / COUNT(*) * 100, 2) AS invalid_pct
FROM iceberg.silver.trips
WHERE event_date = CURRENT_DATE;
```

**Iceberg Time Travel — Query Historical Snapshots**
```sql
-- View table snapshots
SELECT snapshot_id, committed_at, operation
FROM iceberg.silver."trips$snapshots"
ORDER BY committed_at DESC;

-- Read data as of a specific snapshot
SELECT COUNT(*) FROM iceberg.silver.trips
FOR VERSION AS OF <snapshot_id>;
```

### Workload Isolation (Resource Groups)

Trino routes each user to an isolated resource group:

| Group | Max Concurrent | Memory | Scheduling Weight | User Pattern |
|-------|---------------|--------|-------------------|--------------|
| **global** | 100 (hard limit) | - | - | - |
| team_a_reports | 20 | 40% | 30 | `team_a.*` |
| team_b_monitoring | 50 | 30% | 50 | `team_b.*` |
| adhoc | 20 | 20% | 20 | `.*` (fallback) |

This ensures Team A's heavy daily reports cannot starve Team B's real-time monitoring.

---

## Monitoring & Observability

### Architecture

```
  Kafka ──→ kafka-exporter ──→ Prometheus ──→ Grafana
  Flink ──→ PrometheusReporter ──→ Prometheus ──→ Grafana
  Airflow ──→ StatsD ──→ statsd-exporter ──→ Prometheus ──→ Grafana
  MinIO ──→ /metrics endpoint ──→ Prometheus ──→ Grafana
```

### Prometheus Scrape Targets (7 targets)

| Target | Endpoint | Metrics |
|--------|----------|---------|
| Prometheus | `localhost:9090` | Self-monitoring |
| Kafka Exporter | `kafka-exporter:9308` | Topic offsets, consumer lag, partition count |
| Flink JobManager | `flink-jobmanager:9249` | Job status, checkpoints, restarts |
| Flink TaskManager | `flink-taskmanager:9249` | CPU, memory, records in/out, GC |
| Airflow (StatsD) | `statsd-exporter:9102` | DAG runs, task finish, DQ gate status |
| MinIO Cluster | `minio:9000/minio/v2/metrics/cluster` | Capacity, drive health, process CPU/memory |
| MinIO Bucket | `minio:9000/minio/v2/metrics/bucket` | Per-bucket size, object count |

### Grafana Dashboards

**Dashboard 1 — Platform Overview** (`platform-overview.json`)
- DAG run success/fail rate over time
- DQ check average duration
- Flink streaming metrics (jobs running, restarts, throughput)
- Kafka metrics (messages in/out, consumer lag)
- MinIO per-bucket storage (Bronze/Silver/Gold sizes, object counts, S3 request rates)

**Dashboard 2 — Service Resources** (`service-resources.json`)
- Flink JobManager: CPU load, heap/non-heap memory, GC count/duration, threads, classloader
- Flink TaskManager: CPU load, heap memory, records throughput (in/out per sec), network IO
- MinIO: CPU usage, resident/virtual memory, disk IO read/write, file descriptors, Go routines, S3 request rates
- Kafka: CPU, memory, topic throughput (msgs/sec), consumer group lag, file descriptors
- Airflow: StatsD exporter CPU/memory, DAG task completion rate
- Prometheus: Self-monitoring CPU/memory, scrape stats, active time series

**Dashboard 3 — Error Monitoring** (`error-monitoring.json`)
- Platform Health Overview: UP/DOWN status for all 6 services (Flink JM/TM, Kafka, Airflow, MinIO, Prometheus)
- Airflow DQ Gate & Task Errors: success/fail rate over time, failed tasks by DAG, DQ failure counts by check type
- Flink Streaming Errors: running jobs, restarts, failed checkpoints, record errors, Kafka consumer errors & lag
- Kafka Consumer Lag: total lag stat with color thresholds (0 / 1,000 / 10,000), lag by group/topic, per partition
- MinIO S3 Errors: total error rate, 4xx errors, auth rejected, invalid requests, notification/audit errors
- Prometheus Scrape Health: target status over time, scrape duration, samples scraped, rule evaluation failures

### Alert Rules (`alert-rules.yml`)

| Alert | Severity | Condition |
|-------|----------|-----------|
| `DQGateFreshnessFailed` | critical | No Silver data in 2 hours |
| `DQGateValidityFailed` | critical | Invalid ratio > 10% |
| `DQGateNullCheckFailed` | critical | NULLs in critical fields |
| `FlinkJobNotRunning` | critical | 0 running Flink jobs for 2 min (Bronze + Silver run as 1 combined job) |
| `FlinkJobRestarting` | warning | > 3 job restarts in 10 min |
| `KafkaConsumerLagHigh` | warning | Consumer lag > 10,000 messages |
| `GoldAggregationFailed` | critical | Gold INSERT task failed |

### Alert Failure Testing (`test_alert_scenarios` DAG)

A dedicated test DAG (`test_alert_scenarios.py`) and test script (`test-alerts.sh`) are included to **intentionally trigger all alert rules** and validate that the monitoring pipeline works end-to-end.

**How to run:**

```bash
# Option 1: Automated test script (tests all 7 alerts with status checks)
chmod +x scripts/test-alerts.sh
./scripts/test-alerts.sh              # Run all 7 scenarios
./scripts/test-alerts.sh --dq-only    # Run only DQ gate scenarios (1-4)
./scripts/test-alerts.sh --flink-only # Run only Flink scenarios (5,7)
./scripts/test-alerts.sh --summary    # Check current alert status only

# Option 2: Manual — trigger just the DQ failure DAG
docker exec -it airflow-webserver airflow dags trigger test_alert_scenarios
```

**What happens after trigger:**

| Step | Where to Check | Expected Result |
|------|---------------|----------------|
| 1. Tasks FAIL | Airflow UI → http://localhost:8085 | All 4 tasks show RED (failed) |
| 2. StatsD emits | statsd-exporter | `airflow_task_finish_total{state="failed"}` counter increases |
| 3. Prometheus scrapes | http://localhost:9090/alerts | 4 alerts change to FIRING |
| 4. Grafana shows | http://localhost:3000 | DQ Gate panels show FAIL |

**Failure scenarios in the DAG:**

| Scenario | Task ID | How It Fails | Alert Triggered |
|----------|---------|-------------|-----------------|
| 1. Freshness | `dq_check_freshness` | Queries for data in year 2099 (no rows exist) → returns FALSE | `DQGateFreshnessFailed` |
| 2. Validity | `dq_check_invalid_ratio` | SQL `SELECT 1.0 < 0.1` → returns FALSE | `DQGateValidityFailed` |
| 3. Null Check | `dq_check_null_critical_fields` | Forces a NULL row to exist → `COUNT(*) = 0` returns FALSE | `DQGateNullCheckFailed` |
| 4. Gold INSERT | `insert_gold_trips_hourly` | References non-existent table → Trino TABLE_NOT_FOUND error | `GoldAggregationFailed` |

**Testing Flink & Kafka alerts (infrastructure-level):**

```bash
# Scenario 5: FlinkJobNotRunning — stop Flink TaskManager
# Alert fires after 2 minutes when running jobs drop to 0 (combined Bronze+Silver job stops)
docker stop flink-taskmanager
# Check: http://localhost:9090/alerts → FlinkJobNotRunning = FIRING
# Restore:
docker start flink-taskmanager

# Scenario 6: KafkaConsumerLagHigh — pause Flink to build up lag
# Stop consumer (Flink) while producer keeps sending events
docker stop flink-taskmanager
# Wait ~5 minutes for lag to exceed 10,000 messages
# Check: http://localhost:9090/alerts → KafkaConsumerLagHigh = FIRING
# Restore:
docker start flink-taskmanager
# Flink replays from offset, lag recovers

# Scenario 7: FlinkJobRestarting — cause repeated job failures
# Cancel and resubmit jobs rapidly to simulate crash loop
docker exec -it flink-jobmanager bash -c "./bin/flink cancel <job-id>"
# Repeat 3+ times in 10 minutes
# Check: http://localhost:9090/alerts → FlinkJobRestarting = FIRING
```

**Full alert validation checklist:**

| # | Alert | Trigger Method | Severity | Recovery |
|---|-------|---------------|----------|----------|
| 1 | `DQGateFreshnessFailed` | Run `test_alert_scenarios` DAG | critical | Re-run `gold_aggregation` (normal DQ passes) |
| 2 | `DQGateValidityFailed` | Run `test_alert_scenarios` DAG | critical | Re-run `gold_aggregation` |
| 3 | `DQGateNullCheckFailed` | Run `test_alert_scenarios` DAG | critical | Re-run `gold_aggregation` |
| 4 | `GoldAggregationFailed` | Run `test_alert_scenarios` DAG | critical | Re-run `gold_aggregation` |
| 5 | `FlinkJobNotRunning` | `docker stop flink-taskmanager` | critical | `docker start flink-taskmanager` |
| 6 | `KafkaConsumerLagHigh` | Stop Flink for 5+ min | warning | `docker start flink-taskmanager` |
| 7 | `FlinkJobRestarting` | Cancel jobs 3+ times in 10 min | warning | Jobs auto-stabilize |

---

## Security & Governance

### Authentication & Authorization

| Layer | Mechanism | Implementation |
|-------|-----------|---------------|
| **Trino Query Engine** | User-based routing | Resource groups map `team_a.*` → 40% memory, `team_b.*` → 30%, others → 20% adhoc |
| **Airflow Web UI** | Username + password | RBAC-enabled, admin/admin (local dev) |
| **MinIO Console** | Access key + secret | admin / password123 (local dev) |
| **Grafana** | Username + password | admin / admin with role-based access |
| **Kafka** | Network isolation | Internal only (`kafka:29092`), no external auth (local dev) |

### Network Security

All services run on a shared Docker bridge network (`platform`). In this local deployment, inter-service communication is isolated from the host:

```
External Access (host ports)          Internal Network (platform bridge)
─────────────────────────────         ──────────────────────────────────
localhost:8082 → Trino                kafka:29092 (internal broker)
localhost:8085 → Airflow              postgres:5432 (DB)
localhost:3000 → Grafana              minio:9000 (S3 API)
localhost:9001 → MinIO Console        iceberg-rest:8181 (catalog)
```

Production recommendation: use Kubernetes NetworkPolicies to restrict pod-to-pod traffic.

### Data Governance with Iceberg

| Feature | How It Helps | Example |
|---------|-------------|---------|
| **Schema Evolution** | Add/rename/drop columns without rewriting data | `ALTER TABLE silver.trips ADD COLUMN new_field VARCHAR` |
| **Time Travel** | Query data at any point in time | `SELECT * FROM silver.trips FOR VERSION AS OF <snapshot_id>` |
| **Snapshot Audit** | Track all write operations with timestamps | `SELECT * FROM silver."trips$snapshots"` |
| **ACID Transactions** | No partial writes, readers never see dirty data | Guaranteed by Iceberg's optimistic concurrency |
| **Partition Pruning** | Date-based partitions prevent full table scans | `WHERE event_date = '2026-02-28'` only reads 1 partition |
| **Rollback** | Revert table to a previous snapshot | `CALL iceberg.system.rollback_to_snapshot('silver', 'trips', <snapshot_id>)` |

### Data Quality as Governance

The DQ gate pattern ensures **only validated data enters the Gold layer**:

1. **Freshness gate** — ensures no stale data is served to business users
2. **Validity gate** — invalid ratio must stay below 10%
3. **Null gate** — critical business fields (`trip_id`, `route_name`, `base_fare`) must never be NULL
4. **Audit trail** — Airflow logs every DQ check result (pass/fail with timestamp)
5. **Grafana visibility** — DQ gate PASS/FAIL status displayed in real-time on dashboard

### Production Security Recommendations

| Area | Recommendation |
|------|----------------|
| **Secrets** | Use Docker secrets or Vault instead of environment variables |
| **Kafka** | Enable SASL/SSL authentication + ACLs per topic |
| **Trino** | Enable HTTPS + LDAP/OAuth2 authentication + system access control |
| **MinIO** | Enable TLS, IAM policies per bucket, disable public Prometheus |
| **Airflow** | Enable OAuth2/LDAP, use `fernet_key` for connection encryption |
| **Network** | Kubernetes NetworkPolicies to restrict ingress/egress per service |
| **Data Encryption** | Enable SSE-S3 server-side encryption on MinIO buckets |
| **Audit Logging** | Enable MinIO audit log, Trino query log, Kafka broker log |

---

## Containerization & Kubernetes Path

### Current State: Docker Compose (16 containers)

This platform is **fully containerized** using Docker Compose with:
- **3 custom-built images** (Flink, Airflow, Data Generator) via Dockerfiles
- **13 official images** (Kafka, Trino, MinIO, PostgreSQL, Grafana, Prometheus, etc.)
- **Health checks** on all critical services with `depends_on: condition: service_healthy`
- **4 persistent volumes** (minio-data, postgres-data, zookeeper-data, kafka-data)
- **Bridge network** (`platform`) for all inter-service communication

### Kubernetes Migration Path

The Docker Compose setup maps directly to Kubernetes resources:

| Docker Compose | Kubernetes Equivalent | Notes |
|----------------|----------------------|-------|
| `services:` | `Deployment` + `Service` | One Deployment per service |
| `volumes:` | `PersistentVolumeClaim` | Use StorageClass for dynamic provisioning |
| `depends_on:` | `initContainers` + readiness probes | K8s handles dependency via health checks |
| `networks:` | `NetworkPolicy` | K8s has built-in DNS and network isolation |
| `environment:` | `ConfigMap` + `Secret` | Secrets for credentials, ConfigMaps for configs |
| `ports:` | `Service` (ClusterIP/LoadBalancer) | Ingress controller for external access |
| `healthcheck:` | `livenessProbe` + `readinessProbe` | K8s native health management |

### Recommended Kubernetes Architecture

```
┌─── Namespace: data-platform ──────────────────────────────────────────────┐
│                                                                            │
│  ┌─ StatefulSet ──┐  ┌─ StatefulSet ──┐  ┌─ Deployment ──────────────┐   │
│  │ Kafka (3x)     │  │ ZooKeeper (3x) │  │ Flink JobManager (1x)    │   │
│  │ PVC per broker │  │ PVC per node   │  │ + TaskManager (N, HPA)   │   │
│  └────────────────┘  └────────────────┘  └───────────────────────────┘   │
│                                                                            │
│  ┌─ Deployment ───┐  ┌─ StatefulSet ──┐  ┌─ Deployment ──────────────┐   │
│  │ Trino Coord(1) │  │ PostgreSQL(1)  │  │ Airflow Webserver(1)     │   │
│  │ + Workers (N)  │  │ PVC 10Gi       │  │ + Scheduler(1)           │   │
│  │ HPA on CPU     │  └────────────────┘  │ + KubernetesExecutor     │   │
│  └────────────────┘                       └───────────────────────────┘   │
│                                                                            │
│  ┌─ Deployment ───┐  ┌─ Deployment ───┐  ┌─ Namespace: monitoring ───┐   │
│  │ MinIO (or S3)  │  │ Data Generator │  │ Prometheus + Grafana     │   │
│  │ PVC 100Gi      │  │ Replicas: 1    │  │ AlertManager             │   │
│  └────────────────┘  └────────────────┘  └───────────────────────────┘   │
│                                                                            │
│  ┌─ Ingress Controller ──────────────────────────────────────────────┐   │
│  │ /airflow → airflow-webserver:8085                                  │   │
│  │ /trino   → trino-coordinator:8082                                  │   │
│  │ /grafana → grafana:3000                                            │   │
│  │ /minio   → minio-console:9001                                      │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

Key benefits of K8s migration:
- **Horizontal Pod Autoscaler (HPA)** — Scale Flink TaskManagers and Trino Workers based on CPU/memory
- **Pod Disruption Budgets** — Ensure rolling updates don't break streaming jobs
- **Helm Charts** — Most components (Kafka, Flink, Trino, Airflow) have official Helm charts
- **Namespace isolation** — Separate `data-platform` and `monitoring` namespaces
- **RBAC** — Kubernetes RBAC for cluster access control

---

## Cost Awareness & Justification

### License Cost: $0

Every component in this platform is **100% open-source**. No vendor license fees.

| Component | License | AWS Managed Equivalent | Estimated Annual Cost (avoided) |
|-----------|---------|------------------------|-------------------------------|
| Apache Kafka | Apache 2.0 | Amazon MSK Serverless | $12,000–50,000/yr |
| Apache Flink | Apache 2.0 | Amazon Managed Flink (KDA) | $8,000–30,000/yr |
| Apache Iceberg | Apache 2.0 | AWS Glue Data Catalog + Lake Formation | $15,000–60,000/yr |
| Trino | Apache 2.0 | Amazon Athena | $20,000–100,000/yr |
| MinIO | AGPL v3 | Amazon S3 | $2,000–10,000/yr |
| Apache Airflow | Apache 2.0 | Amazon MWAA | $5,000–20,000/yr |
| Prometheus + Grafana | Apache 2.0 | Amazon CloudWatch + Managed Grafana | $5,000–30,000/yr |

### Production Cloud Cost Estimate (AWS)

| Resource | Specification | Monthly Cost |
|----------|--------------|-------------|
| EKS Cluster | 1 control plane | ~$73 |
| Kafka (MSK) | 3x `kafka.m5.large` | ~$500 |
| Flink (EMR) | 1 JM + 2 TM `m5.xlarge` | ~$400 |
| Trino (EC2) | 1 Coord + 2 Workers `r5.xlarge` | ~$600 |
| S3 Storage | 500 GB (Parquet, compressed) | ~$12 |
| RDS PostgreSQL | `db.t3.medium` | ~$65 |
| **Total** | | **~$1,650/month** |

Compare to fully managed : Snowflake ($5,000+/mo), Databricks ($3,000+/mo).

---

## Platform Improvement Ideas

### 1. Schema Registry 
Add a schema registry for Kafka to enforce schema evolution rules (backward/forward compatibility). Prevents producer-side changes from breaking Flink consumers.

### 2. Data Catalog (Apache Atlas / DataHub)
Add a metadata catalog to provide:
- Automated data lineage tracking (Kafka → Bronze → Silver → Gold)
- Column-level descriptions and tags
- Data discovery for analysts

### 3. CDC Integration (Debezium)
Replace the Python data generator with Debezium CDC connector to capture real changes from operational databases. Enables real-time synchronization from MySQL/PostgreSQL source systems.

### 4. Data Contracts
Formalize the interface between producer (data generator) and consumer (Flink) with a data contract specifying:
- Required fields, types, and allowed values
- SLA (freshness, completeness)
- Breaking change notification process

### 5. Cost Monitoring with OpenCost
Add OpenCost to Kubernetes to track per-team compute costs. Enables chargeback reporting (e.g., Team A used 40% of Trino resources = $X/month).

### 6. Streaming Gold Layer
Replace Airflow batch Gold aggregation with a Flink streaming aggregation using windowed SQL. Reduces Gold layer latency from hourly to near real-time (minutes).

### 7. Data Lakehouse Query Acceleration
Add Apache Parquet bloom filters and Iceberg metadata statistics to accelerate point lookups on high-cardinality columns like `trip_id` or `driver_id`.

### 8. Disaster Recovery
- Cross-region S3 replication for MinIO data
- Kafka MirrorMaker 2 for topic replication
- PostgreSQL streaming replication for metadata

---

## Fault Tolerance

```bash
# Test 1: Kill Flink TaskManager — job auto-restarts from last checkpoint
docker stop flink-taskmanager
# Wait 30s, then check Flink UI: job restarts from checkpoint
docker start flink-taskmanager

# Test 2: Restart Kafka — Flink reconnects and replays from stored offset
docker restart kafka

# Test 3: Kill Airflow scheduler — webserver still shows status, restart resumes
docker restart airflow-scheduler

# Verify: no data loss or duplicates after recovery
docker exec -it trino trino --execute \
  "SELECT COUNT(*), COUNT(DISTINCT trip_id) FROM iceberg.silver.trips"
```

### How Each Component Recovers

| Component | Failure Mode | Recovery Mechanism |
|-----------|-------------|-------------------|
| **Flink** | TaskManager crash | Auto-restart from last checkpoint (60s interval, EXACTLY_ONCE) |
| **Kafka** | Broker restart | Consumer resumes from committed offset (group: `flink-etl-pipeline`) |
| **Airflow** | Scheduler crash | Webserver stays up; scheduler resumes DAG runs on restart |
| **MinIO** | Container restart | Data persisted on Docker volume (`minio-data`) |
| **PostgreSQL** | Container restart | Data persisted on Docker volume (`postgres-data`) |
| **Trino** | Worker crash | Query retried by coordinator; no persistent state needed |

---

## Maintenance

### Airflow DAG Management

```bash
# Trigger Gold aggregation manually
docker exec -it airflow-webserver airflow dags trigger gold_aggregation

# Trigger Team A reports manually
docker exec -it airflow-webserver airflow dags trigger team_a_daily_reports

# Check DAG run status
docker exec -it airflow-webserver airflow dags list-runs -d gold_aggregation
```

Or use the **Airflow Web UI** at http://localhost:8085 (admin / admin).

### Iceberg Compaction (reduce small files)

```sql
-- Run in Trino (schedule as daily Airflow DAG)
ALTER TABLE iceberg.silver.trips EXECUTE optimize;
ALTER TABLE iceberg.gold.trips_hourly EXECUTE optimize;
```

### Cleanup Old Snapshots

```sql
-- Remove snapshots older than 7 days
ALTER TABLE iceberg.silver.trips EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.gold.trips_hourly EXECUTE expire_snapshots(retention_threshold => '7d');
```

### Stop the Platform

```bash
# Stop all services (preserves data volumes)
docker compose down

# Stop and remove all data (fresh start)
docker compose down -v
```

---

## Project Structure

```
problem_2_answer/
├── docker-compose.yml                    # 18 services (16 long-running + 2 init containers)
├── download-jars.sh                      # Downloads 9 Flink connector JARs
│
├── airflow/
│   ├── Dockerfile                        # Custom Airflow + Trino provider
│   ├── init.sh                           # DB creation + migration + admin user
│   └── dags/
│       ├── gold_aggregation.py           # @hourly: DQ Gate → Silver → Gold
│       ├── team_a_daily_reports.py       # 10am: daily revenue + top routes
│       ├── team_b_hourly_monitoring.py   # @hourly: performance + revenue alerts
│       ├── test_dq_and_batch.py          # */5min: test DQ gate + batch INSERT
│       └── test_alert_scenarios.py       # Manual: trigger all 7 alert failures
│
├── data-generator/
│   ├── Dockerfile                        # Python 3.11 + kafka-python
│   └── generator.py                      # Simulates 5 trip events/sec
│
├── flink-custom/
│   └── Dockerfile                        # Flink 1.18 + connector JARs
│
├── flink-jobs/
│   └── etl_pipeline.sql                  # Flink SQL: Kafka → Bronze → Silver
│
├── flink-libs/                           # Pre-downloaded connector JARs (9 total)
│   ├── flink-sql-connector-kafka-3.1.0-1.18.jar
│   ├── iceberg-flink-runtime-1.18-1.5.2.jar
│   ├── flink-s3-fs-hadoop-1.18.1.jar
│   ├── flink-metrics-prometheus-1.18.1.jar
│   └── ... (+ hadoop, aws-bundle, xml parsers)
│
├── trino/
│   ├── catalog/
│   │   └── iceberg.properties            # Trino → Iceberg REST + MinIO (S3)
│   ├── resource-groups/
│   │   └── resource-groups.json          # Workload isolation: 3 groups
│   └── trino_queries.sql                 # Reference queries (Gold, Team A/B)
│
├── scripts/
│   ├── init-postgres.sql                 # Creates airflow database on startup
│   ├── prometheus.yml                    # 7 scrape targets (Kafka, Flink, Airflow, MinIO)
│   ├── alert-rules.yml                   # 7 Prometheus alert rules (DQ gate + pipeline)
│   ├── statsd-mapping.yml                # Airflow StatsD → Prometheus metric mapping
│   ├── test-alerts.sh                    # Automated alert testing script (all 7 rules)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/prometheus.yml  # Auto-configured Prometheus datasource
│       │   ├── dashboards/dashboards.yml   # Dashboard auto-load config
│       │   └── alerting/alerts.yml         # 7 Grafana alert rules + contact points
│       └── dashboards/
│           ├── platform-overview.json      # DQ Gate, Flink, Kafka, MinIO panels
│           ├── service-resources.json      # CPU, RAM, IO per service dashboard
│           └── error-monitoring.json       # Service health, errors, consumer lag, scrape health
│
├── Architecture_answer_2.jpg             # Architecture diagram (image)
└── README.md                             
```

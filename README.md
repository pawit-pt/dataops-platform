# DataOps Platform

Real-time streaming data platform with batch scheduling, data quality gates, and workload isolation — built entirely on open-source technologies.

```
Data Generator  →  Kafka  →  Flink (Stream ETL)  →  Iceberg (Bronze / Silver)
                                                            ↓
                                              Airflow DQ Gates (Freshness / Validity / Nulls)
                                                            ↓  PASS
                                                     Iceberg (Gold)
                                                            ↓
                                                    Trino (SQL Engine)
                                                     ↙     ↓     ↘
                                                Team A   Team B   BI Tools
```

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [ETL Pipeline](#etl-pipeline)
- [Querying Data](#querying-data)
- [Monitoring & Alerting](#monitoring--alerting)
- [Security & Governance](#security--governance)
- [Fault Tolerance](#fault-tolerance)
- [Maintenance](#maintenance)
- [Kubernetes Migration Path](#kubernetes-migration-path)
- [Cost Analysis](#cost-analysis)
- [Future Improvements](#future-improvements)
- [Project Structure](#project-structure)

---

## Architecture

### Why Lakehouse?

| Criteria | Traditional DW | This Platform |
|----------|---------------|---------------|
| Vendor lock-in | High (proprietary formats) | None (Parquet + Iceberg) |
| Real-time support | Limited (micro-batch) | Native (Flink streaming) |
| Cost model | Pay-per-query (unpredictable) | Fixed infra (predictable) |
| Storage format | Proprietary | Open (Parquet + Iceberg) |
| Compute / Storage | Coupled | Fully separated (Trino + MinIO) |

### Technology Choices

| Component | Chosen | Rationale |
|-----------|--------|-----------|
| Message Broker | **Kafka** | High-throughput ordered event streaming with replay. Native Flink connector. |
| Stream Processing | **Flink** | True record-at-a-time with EXACTLY_ONCE semantics. Lower latency than Spark micro-batch. |
| Table Format | **Iceberg** | Engine-agnostic (Flink, Trino, Spark). ACID, time travel, hidden partitioning. No vendor lock-in. |
| Object Storage | **MinIO** | S3-compatible API — zero code changes when migrating to cloud S3/GCS. |
| Query Engine | **Trino** | Purpose-built for interactive analytics. Resource groups for workload isolation. JDBC-compatible. |
| Orchestrator | **Airflow** | Industry standard. Rich operator ecosystem. TrinoOperator for SQL-based DQ gates. |
| Monitoring | **Prometheus + Grafana** | Open-source, native exporters for all platform components. |

### Medallion Architecture

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

- **Bronze** — Raw data exactly as received. Source of truth, enables replay.
- **Silver** — Cleaned and validated. Filters invalid records via `is_valid` flag.
- **Gold** — Business-aggregated. Only populated after all DQ gates pass.

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| Message Broker | Apache Kafka | 7.5.0 (Confluent) | Event streaming, 7-day retention |
| Stream Processing | Apache Flink | 1.18 | EXACTLY_ONCE stream ETL (Bronze + Silver) |
| Table Format | Apache Iceberg | 1.5.2 | ACID transactions, time travel, partition pruning |
| Object Storage | MinIO | latest | S3-compatible lakehouse storage (Parquet + Snappy) |
| Catalog | Iceberg REST Catalog | 0.6.0 (tabulario) | Centralized table metadata |
| Scheduler | Apache Airflow | 2.8.1 | DAG orchestration, DQ gates, scheduled queries |
| Query Engine | Trino | 435 | Distributed SQL with resource group isolation |
| Metadata DB | PostgreSQL | 15 | Hive Metastore + Airflow backend |
| Coordination | ZooKeeper | 7.5.0 (Confluent) | Kafka broker coordination |
| Monitoring | Prometheus + Grafana | latest | Metrics collection, dashboards, alerting |
| Containers | Docker Compose | v3.8 | 18 services on shared `platform` bridge network |

---

## Getting Started

### Prerequisites

- **Docker Desktop** (v4.0+) with at least **8 GB RAM** allocated
- **Docker Compose** v2+
- Available ports: 2181, 3000, 5432, 8080–8082, 8085, 8181, 9000–9001, 9090, 9092, 9101

### 1. Pre-pull base images

```bash
docker pull apache/airflow:2.8.1-python3.11
docker pull apache/flink:1.18-scala_2.12-java11
docker pull python:3.11-slim
```

### 2. Download Flink connector JARs

```bash
chmod +x download-jars.sh
./download-jars.sh
```

Downloads 9 JARs into `flink-libs/` — Kafka SQL connector, Iceberg Flink runtime, S3 Hadoop filesystem, Hadoop client libraries, AWS bundle, XML parsers, and Prometheus metrics reporter.

### 3. Build and start

```bash
docker compose build
docker compose up -d
```

Three custom images are built: Flink (with connectors), Airflow (with Trino provider), and the data generator (Python trip simulator).

### 4. Wait for healthy state (~90 seconds)

```bash
docker compose ps
```

Startup order is handled automatically via `depends_on` + health checks:

```
ZooKeeper → Kafka → Kafka UI + Kafka Exporter
PostgreSQL → Iceberg REST → MinIO → Flink → Trino
PostgreSQL → Airflow Init → Airflow Webserver + Scheduler
Data Generator (starts after Kafka is ready)
StatsD Exporter → Prometheus → Grafana
```

### 5. Submit the Flink ETL pipeline

```bash
docker exec -it flink-jobmanager bash -c \
  "./bin/sql-client.sh -f /opt/flink/jobs/etl_pipeline.sql"
```

This creates Bronze, Silver, and Gold Iceberg tables and starts a combined streaming INSERT job (STATEMENT SET) that writes Kafka events to Bronze and Silver simultaneously.

### 6. Verify

```bash
docker exec -it flink-jobmanager bash -c "./bin/flink list"

docker exec -it trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.silver.trips"

docker exec -it airflow-webserver airflow dags list
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Flink | http://localhost:8081 | — |
| Trino | http://localhost:8082 | any user, no password |
| Airflow | http://localhost:8085 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Iceberg REST | http://localhost:8181 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## ETL Pipeline

### Data Flow

```
┌──────────────┐    ┌─────────┐    ┌──────────────────────┐    ┌──────────────┐
│Data Generator│───→│  Kafka  │───→│   Flink (Streaming)  │───→│ MinIO (S3)   │
│ 5 events/sec │    │  raw.   │    │                      │    │  warehouse/  │
│              │    │  trip.  │    │  STATEMENT SET        │    │  ├── bronze/ │
│              │    │  events │    │  → Bronze + Silver    │    │  ├── silver/ │
└──────────────┘    └─────────┘    └──────────────────────┘    │  └── gold/   │
                                                                └──────────────┘
                                                                       │
                    ┌─────────────────────────────────────┐            │
                    │     Airflow (Batch, @hourly)         │            │
                    │                                       │            │
                    │  DQ Freshness  ──┐                   │            │
                    │  DQ Validity   ──┼── ALL PASS? ─→ Gold INSERT     │
                    │  DQ Null Check ──┘      │            │            │
                    │                      FAIL → Block    │            │
                    └─────────────────────────────────────┘            │
                                                                       │
                    ┌─────────────────────────────────────┐            │
                    │       Trino (Query Engine)           │◄───────────┘
                    │  Team A (40%)  │  Team B (30%)       │
                    │  BI Tools      │  Adhoc (20%)        │
                    └─────────────────────────────────────┘
```

### Stage 1 — Ingestion

The data generator (`data-generator/generator.py`) publishes 5 ride-sharing trip events per second to Kafka topic `raw.trip.events` as JSON. Each event contains trip metadata, route data, pricing, and transaction info.

### Stage 2 — Stream ETL

Flink runs a single combined job defined in `flink-jobs/etl_pipeline.sql`:

- **Bronze** — Raw append, no transformation. Full fidelity.
- **Silver** — Adds an `is_valid` flag based on `base_fare > 0`, `customer_id > 0`, and `trip_id IS NOT NULL`.

Key config: 60-second checkpointing with EXACTLY_ONCE semantics, RocksDB state backend, Parquet output with Snappy compression, partitioned by `event_date`.

### Stage 3 — Data Quality Gates

Before Gold is populated, three checks run in parallel via the `gold_aggregation` DAG:

| Check | Logic | Threshold | On Fail |
|-------|-------|-----------|---------|
| Freshness | Recent data exists (within 2 hours) | > 0 rows | Block Gold |
| Validity | Invalid ratio | < 10% | Block Gold |
| Null Check | NULLs in `trip_id`, `route_name`, `base_fare` | 0 rows | Block Gold |

### Stage 4 — Gold Aggregation

When all DQ gates pass, Airflow runs an hourly aggregation from Silver into Gold:

```sql
INSERT INTO gold.trips_hourly
SELECT
  date_trunc('hour', created_at) AS report_hour,
  route_name,
  COUNT(*)                       AS trip_count,
  SUM(base_fare)                 AS total_revenue,
  AVG(base_fare)                 AS avg_fare,
  COUNT(DISTINCT driver_id)      AS unique_drivers,
  CURRENT_TIMESTAMP              AS updated_at
FROM silver.trips
WHERE is_valid = true
GROUP BY date_trunc('hour', created_at), route_name
```

### Stage 5 — Team Consumption

| DAG | Schedule | Team | Purpose |
|-----|----------|------|---------|
| `gold_aggregation` | `@hourly` | Platform | DQ gates + Gold INSERT |
| `team_a_daily_reports` | `0 10 * * *` | Team A | Daily revenue by route, weekly top routes |
| `team_b_hourly_monitoring` | `@hourly` | Team B | Hourly performance, revenue drop alerts (>20%) |

Each team's DAG connects via its own Trino user, automatically routing to the correct resource group.

---

## Querying Data

### Trino CLI

```bash
# Team A (40% memory, 20 concurrent queries)
docker exec -it trino trino --catalog iceberg --schema gold --user team_a_user

# Team B (30% memory, 50 concurrent queries)
docker exec -it trino trino --catalog iceberg --schema gold --user team_b_user
```

### JDBC (DBeaver, Tableau, Power BI)

| Setting | Value |
|---------|-------|
| Driver | Trino JDBC |
| JDBC URL | `jdbc:trino://localhost:8082/iceberg` |
| Username | `team_a_user` or `team_b_user` |
| Password | (empty) |
| Default Schema | `gold` |

### Python

```python
from trino.dbapi import connect

conn = connect(host="localhost", port=8082, user="team_a_user",
               catalog="iceberg", schema="gold")
cursor = conn.cursor()
cursor.execute("SELECT * FROM trips_hourly ORDER BY report_hour DESC LIMIT 10")
rows = cursor.fetchall()
```

### Example Queries

```sql
-- Gold: hourly revenue
SELECT report_hour, route_name, total_revenue, trip_count
FROM iceberg.gold.trips_hourly
ORDER BY report_hour DESC LIMIT 20;

-- Silver: data quality check
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE is_valid = false) AS invalid,
  ROUND(CAST(COUNT(*) FILTER (WHERE is_valid = false) AS DOUBLE) / COUNT(*) * 100, 2) AS invalid_pct
FROM iceberg.silver.trips
WHERE event_date = CURRENT_DATE;

-- Iceberg time travel
SELECT snapshot_id, committed_at, operation
FROM iceberg.silver."trips$snapshots"
ORDER BY committed_at DESC;

SELECT COUNT(*) FROM iceberg.silver.trips
FOR VERSION AS OF <snapshot_id>;
```

### Workload Isolation

Trino routes each user to an isolated resource group:

| Group | Max Concurrent | Memory | User Pattern |
|-------|---------------|--------|--------------|
| global | 100 (hard limit) | — | — |
| team_a_reports | 20 | 40% | `team_a.*` |
| team_b_monitoring | 50 | 30% | `team_b.*` |
| adhoc | 20 | 20% | `.*` (fallback) |

---

## Monitoring & Alerting

### Metrics Pipeline

```
Kafka       ──→  kafka-exporter     ──→  Prometheus  ──→  Grafana
Flink       ──→  PrometheusReporter ──→  Prometheus  ──→  Grafana
Airflow     ──→  StatsD             ──→  statsd-exporter ──→ Prometheus ──→ Grafana
MinIO       ──→  /metrics           ──→  Prometheus  ──→  Grafana
```

### Scrape Targets

| Target | Endpoint | Key Metrics |
|--------|----------|-------------|
| Kafka Exporter | `kafka-exporter:9308` | Topic offsets, consumer lag, partitions |
| Flink JobManager | `flink-jobmanager:9249` | Job status, checkpoints, restarts |
| Flink TaskManager | `flink-taskmanager:9249` | CPU, memory, records in/out, GC |
| Airflow (StatsD) | `statsd-exporter:9102` | DAG runs, task status, DQ gate results |
| MinIO Cluster | `minio:9000/minio/v2/metrics/cluster` | Capacity, drive health, CPU/memory |
| MinIO Bucket | `minio:9000/minio/v2/metrics/bucket` | Per-bucket size, object count |
| Prometheus | `localhost:9090` | Self-monitoring |

### Grafana Dashboards

- **Platform Overview** — DAG success/fail rate, DQ check duration, Flink throughput, Kafka lag, MinIO per-bucket storage
- **Service Resources** — CPU, heap memory, GC, network IO, disk IO per service (Flink, MinIO, Kafka, Airflow, Prometheus)
- **Error Monitoring** — UP/DOWN status for all services, DQ gate failures by type, Flink restarts/checkpoint failures, Kafka consumer lag with thresholds, MinIO S3 errors, Prometheus scrape health

### Alert Rules

| Alert | Severity | Condition |
|-------|----------|-----------|
| `DQGateFreshnessFailed` | critical | No Silver data in 2 hours |
| `DQGateValidityFailed` | critical | Invalid ratio > 10% |
| `DQGateNullCheckFailed` | critical | NULLs in critical fields |
| `FlinkJobNotRunning` | critical | 0 running jobs for 2 minutes |
| `FlinkJobRestarting` | warning | > 3 restarts in 10 minutes |
| `KafkaConsumerLagHigh` | warning | Consumer lag > 10,000 messages |
| `GoldAggregationFailed` | critical | Gold INSERT task failed |

### Testing Alerts

An automated test script and dedicated Airflow DAG validate all 7 alert rules end-to-end:

```bash
./scripts/test-alerts.sh              # All 7 scenarios
./scripts/test-alerts.sh --dq-only    # DQ gate scenarios only
./scripts/test-alerts.sh --flink-only # Flink scenarios only
./scripts/test-alerts.sh --summary    # Current alert status
```

Infrastructure-level alerts (Flink/Kafka) can be triggered manually:

```bash
# FlinkJobNotRunning — fires after 2 min
docker stop flink-taskmanager

# KafkaConsumerLagHigh — stop consumer, wait ~5 min for lag buildup
docker stop flink-taskmanager

# Restore
docker start flink-taskmanager
```

---

## Security & Governance

### Access Control

| Layer | Mechanism |
|-------|-----------|
| Trino | User-based routing to resource groups |
| Airflow | RBAC with username/password |
| MinIO | Access key + secret key |
| Grafana | Username/password with role-based access |
| Kafka | Network isolation (internal only) |

### Network Isolation

All services communicate on a Docker bridge network (`platform`). Only designated ports are exposed to the host.

### Iceberg Data Governance

| Feature | Benefit |
|---------|---------|
| Schema Evolution | Add/rename/drop columns without rewriting data |
| Time Travel | Query data at any historical snapshot |
| Snapshot Audit | Track all write operations with timestamps |
| ACID Transactions | No partial writes, no dirty reads |
| Partition Pruning | Date-based partitions prevent full table scans |
| Rollback | Revert to any previous snapshot |

### DQ as Governance

Only validated data enters the Gold layer. Every DQ check result is logged in Airflow and visible in Grafana. The three gates (freshness, validity, null check) act as automated data contracts.

### Production Recommendations

| Area | Recommendation |
|------|----------------|
| Secrets | Docker secrets or Vault |
| Kafka | SASL/SSL + ACLs per topic |
| Trino | HTTPS + LDAP/OAuth2 + system access control |
| MinIO | TLS, IAM policies per bucket |
| Airflow | OAuth2/LDAP, fernet_key encryption |
| Network | Kubernetes NetworkPolicies |
| Data Encryption | SSE-S3 server-side encryption |
| Audit Logging | MinIO audit log, Trino query log, Kafka broker log |

---

## Fault Tolerance

| Component | Failure Mode | Recovery |
|-----------|-------------|----------|
| Flink | TaskManager crash | Auto-restart from last checkpoint (60s interval, EXACTLY_ONCE) |
| Kafka | Broker restart | Consumer resumes from committed offset |
| Airflow | Scheduler crash | Webserver stays up; scheduler resumes on restart |
| MinIO | Container restart | Data persisted on Docker volume |
| PostgreSQL | Container restart | Data persisted on Docker volume |
| Trino | Worker crash | Query retried by coordinator |

### Testing Resilience

```bash
# Kill Flink TaskManager — job restarts from checkpoint
docker stop flink-taskmanager && sleep 30 && docker start flink-taskmanager

# Restart Kafka — Flink reconnects and replays from stored offset
docker restart kafka

# Restart Airflow scheduler — webserver stays up
docker restart airflow-scheduler

# Verify no data loss
docker exec -it trino trino --execute \
  "SELECT COUNT(*), COUNT(DISTINCT trip_id) FROM iceberg.silver.trips"
```

---

## Maintenance

### DAG Management

```bash
docker exec -it airflow-webserver airflow dags trigger gold_aggregation
docker exec -it airflow-webserver airflow dags trigger team_a_daily_reports
docker exec -it airflow-webserver airflow dags list-runs -d gold_aggregation
```

### Iceberg Table Maintenance

```sql
-- Compact small files (schedule as daily Airflow DAG)
ALTER TABLE iceberg.silver.trips EXECUTE optimize;
ALTER TABLE iceberg.gold.trips_hourly EXECUTE optimize;

-- Expire snapshots older than 7 days
ALTER TABLE iceberg.silver.trips EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.gold.trips_hourly EXECUTE expire_snapshots(retention_threshold => '7d');
```

### Stopping the Platform

```bash
docker compose down      # Preserves data volumes
docker compose down -v   # Removes all data (fresh start)
```

---

## Kubernetes Migration Path

The Docker Compose setup maps directly to Kubernetes resources:

| Docker Compose | Kubernetes |
|----------------|------------|
| `services:` | Deployment + Service |
| `volumes:` | PersistentVolumeClaim |
| `depends_on:` | initContainers + readiness probes |
| `networks:` | NetworkPolicy |
| `environment:` | ConfigMap + Secret |
| `ports:` | Service (ClusterIP / LoadBalancer) + Ingress |
| `healthcheck:` | livenessProbe + readinessProbe |

Key benefits: HPA for Flink TaskManagers and Trino workers, Pod Disruption Budgets for rolling updates, Helm charts for all major components, namespace isolation, and Kubernetes RBAC.

---

## Cost Analysis

### License Cost: $0

Every component is 100% open-source.

| Component | License | Managed Equivalent (avoided) |
|-----------|---------|------------------------------|
| Kafka | Apache 2.0 | Amazon MSK |
| Flink | Apache 2.0 | Amazon Managed Flink |
| Iceberg | Apache 2.0 | AWS Glue + Lake Formation |
| Trino | Apache 2.0 | Amazon Athena |
| MinIO | AGPL v3 | Amazon S3 |
| Airflow | Apache 2.0 | Amazon MWAA |
| Prometheus + Grafana | Apache 2.0 | CloudWatch + Managed Grafana |

### Estimated Production Cost (AWS)

| Resource | Spec | Monthly |
|----------|------|---------|
| EKS Cluster | 1 control plane | ~$73 |
| Kafka (MSK) | 3x `kafka.m5.large` | ~$500 |
| Flink (EMR) | 1 JM + 2 TM `m5.xlarge` | ~$400 |
| Trino (EC2) | 1 Coord + 2 Workers `r5.xlarge` | ~$600 |
| S3 Storage | 500 GB compressed | ~$12 |
| RDS PostgreSQL | `db.t3.medium` | ~$65 |
| **Total** | | **~$1,650/mo** |

Comparable managed platforms: Snowflake ~$5,000+/mo, Databricks ~$3,000+/mo.

---

## Future Improvements

1. **Schema Registry** — Enforce schema evolution rules on Kafka topics to prevent producer-side breaking changes.
2. **Data Catalog (Atlas / DataHub)** — Automated lineage tracking, column-level metadata, and data discovery.
3. **CDC Integration (Debezium)** — Replace the synthetic data generator with real change data capture from operational databases.
4. **Data Contracts** — Formalize producer/consumer interfaces with required fields, types, SLAs, and breaking change notifications.
5. **Cost Monitoring (OpenCost)** — Per-team compute cost tracking and chargeback reporting on Kubernetes.
6. **Streaming Gold Layer** — Replace hourly batch Gold aggregation with Flink windowed SQL for near real-time latency.
7. **Query Acceleration** — Parquet bloom filters and Iceberg metadata statistics for point lookups on high-cardinality columns.
8. **Disaster Recovery** — Cross-region S3 replication, Kafka MirrorMaker 2, and PostgreSQL streaming replication.

---

## Project Structure

```
DataOps-Platform/
├── docker-compose.yml              # 18 services (16 long-running + 2 init)
├── download-jars.sh                # Downloads 9 Flink connector JARs
├── README.md
│
├── airflow/
│   ├── Dockerfile                  # Airflow 2.8.1 + Trino provider
│   ├── init.sh                     # DB migration + admin user setup
│   └── dags/
│       ├── gold_aggregation.py     # @hourly: DQ gates → Gold INSERT
│       ├── team_a_daily_reports.py # Daily revenue + top routes
│       ├── team_b_hourly_monitoring.py  # Hourly performance + alerts
│       ├── test_dq_and_batch.py    # */5min: DQ gate + batch test
│       └── test_alert_scenarios.py # Manual: trigger all alert rules
│
├── data-generator/
│   ├── Dockerfile                  # Python 3.11 + kafka-python
│   └── generator.py               # 5 trip events/sec → Kafka
│
├── flink-custom/
│   └── Dockerfile                  # Flink 1.18 + connector JARs
│
├── flink-jobs/
│   └── etl_pipeline.sql           # Flink SQL: Kafka → Bronze + Silver
│
├── flink-libs/                     # Downloaded JARs (populated by download-jars.sh)
│
├── trino/
│   ├── catalog/
│   │   └── iceberg.properties     # Trino → Iceberg REST + MinIO
│   └── resource-groups/
│       └── resource-groups.json   # Workload isolation (3 groups)
│
└── scripts/
    ├── init-postgres.sql          # Creates airflow DB on startup
    ├── prometheus.yml             # 7 scrape targets
    ├── alert-rules.yml            # 7 Prometheus alert rules
    ├── statsd-mapping.yml         # Airflow StatsD → Prometheus mapping
    ├── test-alerts.sh             # Automated alert validation
    └── grafana/
        ├── provisioning/
        │   ├── datasources/       # Prometheus datasource
        │   ├── dashboards/        # Dashboard auto-load config
        │   └── alerting/          # Grafana alert rules + contact points
        └── dashboards/
            ├── platform-overview.json
            ├── service-resources.json
            └── error-monitoring.json
```

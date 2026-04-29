-- ================================================================
-- Flink SQL: ETL Pipeline
-- raw.trip.events (Kafka) -> Iceberg Bronze -> Silver -> Gold
--
-- Run this in Flink SQL Client:
--   docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/etl_pipeline.sql
-- ================================================================


-- ----------------------------------------------------------------
-- STEP 1: Configure checkpointing and parallelism
-- ----------------------------------------------------------------
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'parallelism.default' = '2';


-- ----------------------------------------------------------------
-- STEP 2: Kafka source table (in DEFAULT catalog)
-- Matches the PySpark schema: driver_ID, customer_id,
-- transactionRecord (nested), COORDINATES (3D array)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_raw_trips (
  driver_ID       STRING,
  customer_id     BIGINT,
  transactionRecord ROW<
    transaction_id  STRING,
    trip_info       ROW<
      trip_id       STRING,
      pricing       ROW<
        base_fare   DOUBLE,
        route_data  ROW<
          route_name STRING,
          segments   ARRAY<ROW<
            SEGMENT_ID STRING,
            `_Metrics`   ARRAY<DOUBLE>
          >>
        >
      >,
      createdAt     STRING
    >
  >,
  COORDINATES     ARRAY<ARRAY<ARRAY<DOUBLE>>>
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'raw.trip.events',
  'properties.bootstrap.servers'  = 'kafka:29092',
  'properties.group.id'           = 'flink-etl-pipeline',
  'scan.startup.mode'             = 'earliest-offset',
  'format'                        = 'json',
  'json.fail-on-missing-field'    = 'false',
  'json.ignore-parse-errors'      = 'true'
);


-- ----------------------------------------------------------------
-- STEP 3: Register Iceberg REST catalog
-- ----------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
  'type'                = 'iceberg',
  'catalog-type'        = 'rest',
  'uri'                 = 'http://iceberg-rest:8181',
  'io-impl'             = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'         = 'http://minio:9000',
  's3.path-style-access'= 'true',
  's3.access-key-id'    = 'admin',
  's3.secret-access-key'= 'password123',
  's3.region'           = 'us-east-1'
);

USE CATALOG iceberg_catalog;

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;


-- ----------------------------------------------------------------
-- STEP 4: Drop old tables that may carry stale properties, then recreate
-- (CREATE TABLE is not allowed inside EXECUTE STATEMENT SET)
-- ----------------------------------------------------------------

DROP TABLE IF EXISTS bronze.trips;
DROP TABLE IF EXISTS silver.trips;
DROP TABLE IF EXISTS gold.trips_hourly;

CREATE TABLE IF NOT EXISTS bronze.trips (
  driver_id         STRING,
  customer_id       BIGINT,
  transaction_id    STRING,
  trip_id           STRING,
  base_fare         DOUBLE,
  route_name        STRING,
  segment_count     INT,
  created_at        STRING,
  coordinates_raw   STRING,
  ingested_at       TIMESTAMP(3),
  event_date        DATE
) PARTITIONED BY (event_date)
WITH (
  'format-version'                  = '2',
  'write.format.default'            = 'parquet',
  'write.parquet.compression-codec' = 'snappy',
  'write.target-file-size-bytes'    = '134217728'
);

CREATE TABLE IF NOT EXISTS silver.trips (
  driver_id         STRING,
  customer_id       BIGINT,
  transaction_id    STRING,
  trip_id           STRING,
  base_fare         DOUBLE,
  route_name        STRING,
  segment_count     INT,
  created_at        TIMESTAMP(3),
  updated_at        TIMESTAMP(3),
  event_date        DATE,
  is_valid          BOOLEAN
) PARTITIONED BY (event_date)
WITH (
  'format-version'         = '2',
  'write.format.default'   = 'parquet'
);

CREATE TABLE IF NOT EXISTS gold.trips_hourly (
  report_hour       TIMESTAMP(3),
  route_name        STRING,
  trip_count        BIGINT,
  total_revenue     DOUBLE,
  avg_fare          DOUBLE,
  unique_drivers    BIGINT,
  updated_at        TIMESTAMP(3),
  report_date       DATE,
  PRIMARY KEY (report_hour, route_name) NOT ENFORCED
) PARTITIONED BY (report_date)
WITH (
  'write.format.default' = 'parquet'
);


-- ----------------------------------------------------------------
-- STEP 5: Stream Bronze + Silver from Kafka (append-only, no agg)
-- Gold is populated by Trino scheduled query via Airflow DAG.
-- ----------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

INSERT INTO bronze.trips
SELECT
  driver_ID                                                           AS driver_id,
  customer_id,
  transactionRecord.transaction_id                                    AS transaction_id,
  transactionRecord.trip_info.trip_id                                 AS trip_id,
  transactionRecord.trip_info.pricing.base_fare                       AS base_fare,
  transactionRecord.trip_info.pricing.route_data.route_name           AS route_name,
  CARDINALITY(transactionRecord.trip_info.pricing.route_data.segments) AS segment_count,
  transactionRecord.trip_info.createdAt                               AS created_at,
  CAST(COORDINATES AS STRING)                                         AS coordinates_raw,
  CURRENT_TIMESTAMP                                                   AS ingested_at,
  CAST(
    TO_TIMESTAMP(
      LEFT(transactionRecord.trip_info.createdAt, 23),
      'yyyy-MM-dd''T''HH:mm:ss.SSS'
    ) AS DATE
  )                                                                   AS event_date
FROM default_catalog.default_database.kafka_raw_trips
WHERE driver_ID IS NOT NULL
  AND transactionRecord.trip_info.trip_id IS NOT NULL;

INSERT INTO silver.trips
SELECT
  driver_ID                                                           AS driver_id,
  customer_id,
  transactionRecord.transaction_id                                    AS transaction_id,
  transactionRecord.trip_info.trip_id                                 AS trip_id,
  transactionRecord.trip_info.pricing.base_fare                       AS base_fare,
  transactionRecord.trip_info.pricing.route_data.route_name           AS route_name,
  CARDINALITY(transactionRecord.trip_info.pricing.route_data.segments) AS segment_count,
  TO_TIMESTAMP(
    LEFT(transactionRecord.trip_info.createdAt, 23),
    'yyyy-MM-dd''T''HH:mm:ss.SSS'
  )                                                                   AS created_at,
  CURRENT_TIMESTAMP                                                   AS updated_at,
  CAST(
    TO_TIMESTAMP(
      LEFT(transactionRecord.trip_info.createdAt, 23),
      'yyyy-MM-dd''T''HH:mm:ss.SSS'
    ) AS DATE
  )                                                                   AS event_date,
  CASE
    WHEN transactionRecord.trip_info.pricing.base_fare <= 0 THEN FALSE
    WHEN customer_id <= 0                                   THEN FALSE
    WHEN transactionRecord.trip_info.trip_id IS NULL         THEN FALSE
    ELSE TRUE
  END                                                                 AS is_valid
FROM default_catalog.default_database.kafka_raw_trips
WHERE transactionRecord.trip_info.trip_id IS NOT NULL;

END;

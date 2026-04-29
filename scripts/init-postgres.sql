-- Create the Airflow metadata database
-- This runs automatically on first PostgreSQL startup
-- (mounted into /docker-entrypoint-initdb.d/)
CREATE DATABASE airflow;

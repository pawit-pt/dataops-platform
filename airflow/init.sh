#!/bin/bash
set -e

# Ensure airflow binary is on PATH (installed in ~/.local/bin by pip)
export PATH="/home/airflow/.local/bin:${PATH}"

echo "=== Airflow Init ==="

# 1. Install psycopg2 for PostgreSQL connectivity
pip install psycopg2-binary -q

# 2. Create the 'airflow' database if it doesn't exist
python -c "
import psycopg2
conn = psycopg2.connect(
    host='postgres', port=5432,
    user='platform', password='platform',
    dbname='platform'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute(\"SELECT 1 FROM pg_database WHERE datname='airflow'\")
if not cur.fetchone():
    cur.execute('CREATE DATABASE airflow')
    print('Created airflow database')
else:
    print('airflow database already exists')
conn.close()
"

# 3. Run Airflow DB migrations
echo "Running Airflow DB migrations..."
if ! airflow db migrate; then
    echo "Airflow DB migrations failed"
    exit 1
fi

echo "Creating admin user..."
if ! airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email pawit@example.com; then
    echo "Admin user creation failed"
    exit 1
fi

echo "=== Airflow Init Complete ==="

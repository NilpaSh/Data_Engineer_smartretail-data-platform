#!/bin/bash
# Creates the Airflow metadata database alongside our retail_dw database
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL

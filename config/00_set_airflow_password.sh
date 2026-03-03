#!/bin/bash
# Set the Airflow DB user password from environment variable
# This runs before SQL init scripts (00_ prefix ensures ordering)
set -e

if [ -n "$AIRFLOW_DB_PASSWORD" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        DO \$\$
        BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
                ALTER USER airflow_user WITH PASSWORD '$AIRFLOW_DB_PASSWORD';
            END IF;
        END
        \$\$;
EOSQL
    echo "Airflow DB password set from environment variable"
fi

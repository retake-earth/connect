#!/bin/bash

# This script is executed at container initialization to configure
# it for ParadeDB workloads

echo "Configuring PostgreSQL search_path..."

# Add the `paradedb` schema to the user database, and default to public (by listing it first)
PGPASSWORD=$POSTGRESQL_PASSWORD psql -v ON_ERROR_STOP=1 --username "$POSTGRESQL_USERNAME" --dbname "$POSTGRESQL_DATABASE" <<-EOSQL
  ALTER DATABASE "$POSTGRES_DB" SET search_path TO public,paradedb;
EOSQL

# Add the `paradedb` schema to the template1 database, to have it inherited by all new databases
# created post-initialization, and default to public (by listing it first)
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "template1" <<-EOSQL
  ALTER DATABASE template1 SET search_path TO public,paradedb;
EOSQL

echo "Installing PostgreSQL extensions..."

# Pre-install all required PostgreSQL extensions to the user database via the `postgres` superuser
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d $DATABASE_NAME -c "CREATE EXTENSION IF NOT EXISTS pg_bm25 CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d $DATABASE_NAME -c "CREATE EXTENSION IF NOT EXISTS pg_analytics CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d $DATABASE_NAME -c "CREATE EXTENSION IF NOT EXISTS svector CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d $DATABASE_NAME -c "CREATE EXTENSION IF NOT EXISTS vector CASCADE;"

# Pre-install all required PostgreSQL extensions to the template1 database, to have them inherited by all new
# databases created post-initialization, via the `postgres` user
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d "template1" -c "CREATE EXTENSION IF NOT EXISTS pg_bm25 CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d "template1" -c "CREATE EXTENSION IF NOT EXISTS pg_analytics CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d "template1" -c "CREATE EXTENSION IF NOT EXISTS svector CASCADE;"
PGPASSWORD=$POSTGRESQL_POSTGRES_PASSWORD psql -U postgres -d "template1" -c "CREATE EXTENSION IF NOT EXISTS vector CASCADE;"

echo "Sending anonymous deployment telemetry (to turn off, unset TELEMETRY)..."

# We collect basic, anonymous telemetry to help us understand how many people are using
# the project. We only do this if TELEMETRY is set to "true"
if [[ ${TELEMETRY:-} == "true" ]]; then



  # TODO: We definitely need to update this path for bitnami
  # For privacy reasons, we generate an anonymous UUID for each new deployment
  UUID_FILE="/var/lib/postgresql/data/paradedb_uuid"
  if [ ! -f "$UUID_FILE" ]; then
    uuidgen > "$UUID_FILE"
  fi
  DISTINCT_ID=$(cat "$UUID_FILE")

  # Send the deployment event to PostHog
  curl -s -L --header "Content-Type: application/json" -d '{
    "api_key": "'"$POSTHOG_API_KEY"'",
    "event": "ParadeDB Deployment",
    "distinct_id": "'"$DISTINCT_ID"'",
    "properties": {
      "commit_sha": "'"${COMMIT_SHA:-}"'"
    }
  }' "$POSTHOG_HOST/capture/"

  # Mark telemetry as handled so we don't try to send it again when
  # initializing our PostgreSQL extensions. We use a file for IPC
  # between this script and our PostgreSQL extensions
  echo "true" > /tmp/telemetry
fi




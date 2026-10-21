#!/bin/sh
set -eu
# Executed by the official PostgreSQL image only when initializing an empty volume.
# Password is supplied as a psql variable, never interpolated into SQL by the shell.
psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=runtime_password="$TASKMESH_DB_PASSWORD" <<'SQL'
CREATE ROLE taskmesh_app LOGIN PASSWORD :'runtime_password'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
SQL

#!/bin/sh
# Restores ONLY to a newly created temporary database in this project's PostgreSQL.
set -eu
if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  printf 'Usage: sh scripts/verify-backup.sh backups/taskmesh-TIMESTAMP.dump\n' >&2
  exit 1
fi
target="taskmesh_restore_check_$(date -u +%Y%m%d%H%M%S)_$$"
created=0
cleanup() {
  if [ "$created" -eq 1 ]; then
    docker compose exec -T postgres dropdb -U taskmesh "$target"
  fi
}
trap cleanup EXIT HUP INT TERM
docker compose exec -T postgres createdb -U taskmesh "$target"
created=1
docker compose exec -T postgres pg_restore --exit-on-error --no-owner -U taskmesh -d "$target" < "$1"
docker compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U taskmesh -d "$target" -c \
  "SELECT status,count(*) FROM jobs GROUP BY status; SELECT count(*) AS attempts FROM job_attempts; SELECT version,checksum FROM schema_migrations;"
printf 'Archive restored and queried in an isolated temporary database.\n'

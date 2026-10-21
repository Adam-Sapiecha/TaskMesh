#!/bin/sh
# Run from the repository root on the Docker host.
set -eu
umask 077
mkdir -p backups
stamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="backups/taskmesh-$stamp.dump"
partial="$backup.partial"
trap 'rm -f "$partial"' EXIT HUP INT TERM
docker compose exec -T postgres pg_dump -U taskmesh -d taskmesh --format=custom > "$partial"
docker compose exec -T postgres pg_restore --list < "$partial" > /dev/null
mv "$partial" "$backup"
sha256sum "$backup" > "$backup.sha256"
printf 'Backup: %s\n' "$backup"

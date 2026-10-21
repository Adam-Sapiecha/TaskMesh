# Operation on luxonis

The initial deployment is described in [the server report](deployment-luxonis.md).
The procedures below apply to subsequent deployments and maintenance.

## Preconditions and read-only audit

Do not deploy before an explicit instruction in the project chat. First inspect
`ssh luxonis` without mutations: `uname -m`, `/etc/os-release`, `free -m`, `df -h`,
`ss -lntup`, `docker ps`, `docker network ls`, `docker volume ls`,
`docker stats --no-stream` and the current reverse-proxy/Tailscale routing.
Confirm CPU/RAM headroom and that loopback port 8084 is unused. Inspect only the
configuration relevant to routing; avoid printing secret environment variables.
Back up an existing proxy file before editing it. Preserve Nextcloud, MariaDB,
Portainer, dashboard, ChordLab, Luxonis/DepthAI services and all their data.

Never run global Docker cleanup. Do not touch `/usr/libexec/robothub`,
`/home/robothub`, `/home/pi/depthai-venv`, `/home/pi/oakd-env`,
`/home/pi/venv-oakv3` or the posture-analysis project.

## ARM64 and resource budget

The Go runtime is built with CGO disabled for the selected target architecture.
Both the official Go/Alpine builder and PostgreSQL/Alpine publish ARM64 images.
No GPU or native image-processing library is used. Before deployment verify the
exact current manifests with `docker buildx imagetools inspect golang:1.26-alpine`,
`alpine:3.23` and `postgres:17-alpine`; record resolved digests for a release.

Default stack: 3 workers x 2 slots, queue cap 8, 30-second leases, 5-second recovery
scan, payload 64 KiB, maximum 10 attempts, timeout 300 seconds, queue backlog 10000.
Reports accept <=1000 finite numbers with absolute value <=1e12, text <=32768
bytes, compute <=100000 hash rounds. No arbitrary code or URL fetching.

Limits are budgets, not measurements: PostgreSQL 384 MiB; each of five long-running
Go processes 160 MiB, GOMEMLIMIT 96 MiB, GOMAXPROCS 1; migration temporarily another
160 MiB. Nominal sum after migration: 1184 MiB. Cgroup overhead, filesystem cache,
Docker and other services consume additional memory. CPU caps: DB 1 core, each Go
process 0.5 core, so total caps can exceed physical availability during migration.
This does not establish suitability for ten simultaneous applications on an 8 GB Pi.

Measure main-flow RSS/CPU on the actual Pi using `docker stats` and disk growth
before increasing concurrency. PostgreSQL data, results, events and deduplication
keys grow without automatic retention. Monitor `df`, never fill the microSD, and
archive only intentionally selected historical data. No demo reset endpoint exists.

## HTTPS and access control

`TASKMESH_PUBLIC_URL=https://taskmesh.YOUR-DOMAIN.example` is an unconfigured
placeholder. A public domain, DNS, reachable HTTPS route and chosen publication
method are still required. The private Tailscale host is not a public recruiter URL.
No assumption is made about a public IP or port forwarding.

`deployments/Caddyfile.example` illustrates a host reverse proxy to loopback 8084.
The API does not derive authorization or tenant identity from forwarded headers and
does not trust arbitrary client IP headers. TLS terminates at the proxy. No SSE or
WebSocket is used in Phase 1. Do not insert the administrative API key into a public
website or frontend bundle. It gives access to every job in this single-project
instance. Public anonymous trials require a later isolated-session API and limits.

Environment: independent DB owner/runtime passwords and API token, no other service accounts
or third-party keys. Use URL-safe hex for the database password in DATABASE_URL.
Use URL-safe hex for both passwords. Keep `.env` outside source control, permissions 0600. Compose injects credentials
into containers; Docker administrators can inspect them. PostgreSQL has its own
dedicated instance and administrative role for this application; it shares no
account or data with Nextcloud. Only the migration process receives the DB owner
credential. API/workers/controller use `taskmesh_app`: schema usage, required table
reads/inserts/updates and event-sequence usage, without deletion, DDL or administrative
privileges. The init script creates this role on an empty volume; migration grants
access after creating tables. Existing volumes from custom native setups need this
role provisioned explicitly before using this Compose configuration. Changing an
environment password does not change a role stored in an existing volume: rotate
it deliberately in PostgreSQL, then update `.env` and recreate dependent services.
The API bearer token should be given only to the trusted owner.

Logs are JSON on stdout; application failure messages omit DB connection strings,
authorization headers, payloads and results. Docker rotates each service at 3 x 5 MiB.
PostgreSQL statement logging is disabled; database administrators must still treat
the DB log and backups as private because SQL errors can include data.

## Backups and verified restore

Run from the repo root on Linux:

```sh
sh scripts/backup.sh
sha256sum -c backups/taskmesh-TIMESTAMP.dump.sha256
sh scripts/verify-backup.sh backups/taskmesh-TIMESTAMP.dump
```

The dump is a consistent PostgreSQL custom-format snapshot. Verification creates
an isolated temporary database inside this stack, restores with exit-on-error,
queries jobs/attempts/migration metadata and drops **only that temporary database**.
It does not overwrite the live database. For full confidence, also retrieve known
results by job ID and compare with the source snapshot taken during a maintenance
window. Copy encrypted backups off the microSD and test them periodically; the
scripts do not configure remote storage or scheduled backups. Application data
currently lives entirely in PostgreSQL; there is no separate upload directory.

Disaster recovery:

1. Stop this stack's API, workers and controller to prevent writes.
2. Preserve the current volume and make a fresh dump if the DB is accessible.
3. On a **new dedicated TaskMesh PostgreSQL instance**, create an empty `taskmesh`
   database and restore the selected archive with `pg_restore --exit-on-error
   --no-owner -U taskmesh -d taskmesh`. Use `docker compose exec -T postgres` and
   redirect the file into stdin as in the verification script.
4. Run the matching release's `taskmesh migrate`; never edit applied migration files.
5. Start API, workers and controller. Restored RUNNING jobs have copied leases;
   the controller will recover them after expiry. Duplicate external effects remain
   possible for future integrations.
6. Check readiness, a known result, event/attempt history and a fresh submission.

There is no automatic reset or destructive down migration. `docker compose down`
keeps the named volume; **do not add `-v`** when preserving data.

## Updates and rollback

Take and restore-test a backup before updates. Record source revision, image tags
and digests. Use versioned `taskmesh:VERSION` images and `TASKMESH_VERSION`; for a
local Compose build, retain the previous tagged image. Stop/drain worker processes
before switching handler versions. Run `docker compose run --rm migrate` with the
new image and inspect its exit status before recreating long-lived services.
Do not assume a successful migration implies a healthy deployment.

The initial migration is transactional and protected by an advisory lock and stored
checksum. Re-running it is safe; modifying an applied file is rejected. Future
migrations must follow expand/contract changes and document compatibility. A code
rollback is safe only against a schema it understands. Otherwise restore a verified
pre-update backup into a new volume/instance and accept the explicitly assessed
loss of changes since that snapshot. Never automatically downgrade/reset production.

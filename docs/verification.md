> Historical Go deployment report. The backend has since migrated to Rust; see
> [current migration and verification](rust-migration.md). Results below describe
> the earlier implementation and are retained as historical evidence.

# Verification report — 2026-10-21

This is the **historical local baseline before deployment**. TaskMesh was
subsequently deployed to luxonis the same day; see
[current server deployment](deployment-luxonis.md) for ARM64, HTTPS and server evidence.

## Implemented

Phase 1 Go/PostgreSQL core, REST submission/read/list/cancel/history APIs, registered
bounded handlers, separate executable modes for API/worker/controller/migration,
three-worker Compose configuration, SQL schema and sqlc reads, durable results,
queue/worker capacity checks, leases and fencing, renewal, retry jitter, DLQ,
idempotency, cooperative cancellation, transactional migrations, instance token
authentication and restricted database runtime role. Includes backup/restore tools,
HTTPS proxy template, CI configuration and architecture/guarantee documentation.

The original full specification remains a roadmap. React dashboard, SSE,
Failure Lab, gRPC, SDK, multi-project RBAC, cron, isolated untrusted execution,
image storage/resizing and OpenTelemetry/Prometheus are not implemented.

## Locally verified

Environment: Windows AMD64, Go 1.27.3, native PostgreSQL 17.11 on a dedicated local
test cluster bound to loopback. No application server or existing service data was
used. PostgreSQL was installed as portable test tooling, without a Windows service.

- Unit tests: numeric reports, Unicode text statistics, SHA-256, input bounds,
  unknown fields/handlers and cooperative cancellation.
- Real PostgreSQL integration tests: 20 concurrent claimers, no duplicate active
  claims, queue and worker caps, delayed jobs, priority, handler filtering, retries,
  DLQ, terminal-state guards, timeout-capped renewal, expired/stale ACK and renewal
  rejection, recovery history, cancellation, and 40 concurrent identical submissions.
- Injected SQL failure: claim, attempt counter and transition event roll back together.
- Restricted runtime role: successful submit/claim/renew/finish/read, denial of DDL,
  DELETE and migration-metadata updates.
- HTTP authentication, malformed input, persistence across API/pool reopening and
  503 behavior with an unavailable (closed) connection pool. This last check is not
  a physical network-partition experiment.
- 1000 jobs through real HTTP and three actual worker **runtime instances in one
  test process**, each with two slots. All jobs succeeded once, with correct report
  results; all three worker identities processed work; a reopened API read results.
- Race detector executed with portable LLVM/MinGW; unit and integration binaries
  were also run directly from `bin/` because Windows App Control intermittently
  blocks executables in Go's temporary build directories.
- `pg_dump` custom archive restored into a newly created database using
  `pg_restore --exit-on-error --no-owner`; known result, migration checksum, attempt
  and event counts compared. Original data was not overwritten.
- PostgreSQL process restart test verifies a previously completed result survives
  restart and connection-pool recovery.
- `go vet ./...`; dependency vulnerability scan: **no vulnerabilities found** after
  upgrading `golang.org/x/text` to v0.41.0 (earlier scan found two fixed issues).
- Compose files parse with `docker compose config --quiet`; shell scripts pass
  `bash -n`. Linux Compose execution of these shell wrappers has not been verified.

The throughput from these functional smoke tests is not a production benchmark.
For example, one non-race run processed 1000 jobs in 2.483 seconds (402.8 jobs/s)
including submission, on this Windows workstation with a local DB and 10 HTTP
producers. Race-enabled runs are slower. No Raspberry Pi throughput, p95/p99,
CPU/RAM measurement or scaling claim is made. The native test DB used its defaults,
not the Compose resource limits. The local transcript is in ignored
`artifacts/integration-race.txt`.

## ARM64

Successfully cross-compiled `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`.
Registry manifests were inspected and contain Linux ARM64 variants:

| Image | Inspected ARM64 manifest |
| --- | --- |
| `postgres:17-alpine` | `sha256:0b2882c46a2b8d8e148431b333394348cf4d7f15aec6df7f17d51dc641e56cfa` |
| `golang:1.26-alpine` | `sha256:838da1d688161cd81dee9f6dcddc85fed0686a7bec6e9c0998a20d78bc4e394b` |
| `alpine:3.23` | `sha256:ae959799024a09da597284763c417a6e820e020fad0634c5267cf4f125e682bf` |

No ARM64 hardware runtime test or container-image build was performed. Digests are
observations at verification time; release images should pin reviewed digests.

## Unverified acceptance and environment blockers

The `acceptance` test starts three worker OS processes and an API, submits 1000
jobs and restarts the API. It is checked in and enabled in CI, but the local attempt
was blocked by **Windows Application Control when launching the newly built worker
executable**. It is not counted as a passing test. Security policies were not changed.
The ordinary three-runtime test does not replace independent-process acceptance.

Docker Desktop's Linux engine was unavailable: its startup reported a stale
`sailor-ingest.sock` listener error. Thus the Docker image build, initial-volume role
bootstrap, Compose startup/health/restart behavior and container-level persistence
remain unverified here. No factory reset, global cleanup or Docker data deletion
was attempted. CI is prepared but has not been run on a remote repository.

## Server and target HTTPS

Nothing was deployed to luxonis; no SSH changes were made. No ARM64 execution,
server resource measurement or target HTTPS check was performed. A public domain,
DNS, reachable HTTPS publication method and explicit deployment instruction are
still needed. The private Tailscale address has not been presented as a public URL.

## Reproduce optional operational tests

In addition to TEST_DATABASE_URL, set TASKMESH_PG_BIN to matching PostgreSQL tools
for the dump/restore test. For the restart test only, also set
TASKMESH_TEST_PG_DATA to the data directory of that **disposable native test cluster**.
The test deliberately restarts that cluster; never configure it against production.
The test login needs schema/role creation, and database creation for restore checks.
Use a dedicated local PostgreSQL instance, not a shared application database.

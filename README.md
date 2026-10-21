# TaskMesh

Durable, pull-based background jobs in Go and PostgreSQL. This repository implements
the durable core engine and a **public React execution console**, with lease fencing, renewal, retries and crash recovery
included so that independent workers can safely restart. It is a real execution
engine with persisted inputs, results and audit history.

[Try TaskMesh in your browser](https://luxonis.tail6ad453.ts.net:8443/taskmesh/) —
no account or Tailscale required. The visitor console runs against its own
PostgreSQL instance, isolated from the owner's authenticated API.
See [public deployment](docs/public-deployment.md) and
[private server deployment](docs/deployment-luxonis.md).

## Run with Docker Compose

Requirements: Docker Engine with Compose v2; Linux AMD64 or ARM64. No paid services,
API subscriptions, CUDA, Redis or external queue are needed.

1. Copy `.env.example` to `.env`.
2. Generate **three different** random values with `openssl rand -hex 32` and fill
   `POSTGRES_PASSWORD`, `TASKMESH_DB_PASSWORD` and `TASKMESH_API_KEY`.
   Keep the file private (`chmod 600 .env`).
3. Run `docker compose config --quiet`, then `docker compose up --build -d`.
4. Run `docker compose ps` and `curl http://127.0.0.1:8084/readyz`.

The stack creates its own `taskmesh` network and PostgreSQL volume. A one-shot,
transactional migration runs before the API, controller and three worker processes.
Port 8084 is used on luxonis; check availability before deploying on another host. PostgreSQL and
worker health endpoints are not published. Long-running services restart after
reboots; migrations must succeed before dependent services start.

Submit your own data (replace `YOUR_API_KEY`):

```sh
curl --fail-with-body http://127.0.0.1:8084/api/v1/jobs \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: first-report' \
  -d '{"type":"report.generate","queue":"reports","payload":{"values":[10,20,30]},"priority":7,"max_attempts":3,"timeout_seconds":120}'
```

Use the returned `Location` to read status and result with the same bearer header.
The report returns count, sum, mean, min and max; no simulated external service is
involved. Other registered types:

| Type | Payload | Result |
| --- | --- | --- |
| `text.analyze` | `{"text":"Your own text"}` | Unicode characters, words, lines |
| `report.generate` | `{"values":[1,2,3]}` | Numeric summary of up to 1000 values |
| `benchmark.compute` | `{"seed":"abc","rounds":1000}` | Repeated SHA-256, up to 100000 rounds |

`run_at` accepts an RFC3339 timestamp; it is the earliest eligible time. Priority is
0–9 (default 5). Defaults: queue `default`, 3 attempts, timeout 120 seconds.
Queues `default`, `reports` and `analytics` exist initially. Unknown task types,
unknown payload fields and invalid sizes are rejected. This release does not
implement `image.resize` or the example report-ID integration from the vision document.

## API

All `/api/` endpoints require the instance's bearer token; health checks are public.

| Method | Path | Behavior |
| --- | --- | --- |
| POST | `/api/v1/jobs` | Durable submission, 201; identical idempotent reuse, 200; conflict, 409 |
| GET | `/api/v1/jobs` | Latest 100 jobs |
| GET | `/api/v1/jobs/{id}` | Job, result and current lease |
| GET | `/api/v1/jobs/{id}/attempts` | Attempt history |
| GET | `/api/v1/jobs/{id}/events` | Durable ordered state changes |
| POST | `/api/v1/jobs/{id}/cancel` | Atomic cancellation, terminal jobs unchanged |
| GET | `/api/v1/queues` | Queue definitions and global concurrency limits |
| GET | `/api/v1/workers` | Latest 100 worker sessions and heartbeats |
| GET | `/healthz`, `/readyz` | Liveness / PostgreSQL and migration readiness |

The private REST API is a **single trusted project per stack**.
Anyone with its token can access all jobs. Use independent stacks and credentials
for independent owners. The public console instead uses HttpOnly visitor-session
cookies and only returns the visitor's own jobs. RBAC, gRPC, SSE, cron,
OpenTelemetry and Failure Lab remain roadmap items. The console uses 2.5-second
polling; it does not claim SSE transport or production fault-injection controls.

## Public visitor console

`compose.public.yaml` creates a separate `taskmesh-public` stack with its own DB,
network, volume and credentials. It runs `taskmesh public` rather than the owner API.
Copy `.env.public.example` to a **separate** `.env` file in your public deployment
directory, fill independent DB passwords and set the exact browser origin/path.
Use `docker compose -f compose.public.yaml up --build -d --wait`.

Each browser gets an HttpOnly, Secure (HTTPS), SameSite=Strict cookie. Session
secrets are stored only as SHA-256 hashes in PostgreSQL. Sessions expire in 24 hours.
Admission is transactional: 30 jobs/session, 200 jobs/fixed clock hour globally,
60 new sessions/hour, and 5000 retained jobs/sessions. Inputs are <=8 KiB, computation
is <=10000 SHA-256 rounds, timeout is 15 seconds, and arbitrary commands/network
integrations are absent. A periodic janitor deletes terminal jobs belonging only
to expired public sessions. No private NULL-scoped job can match retention cleanup.

Frontend development uses Node 24:

```sh
npm ci --prefix web
npm run build --prefix web
# Or npm run dev --prefix web with a local public API on port 8085.
```

The Dockerfile builds the React/TypeScript assets and serves them from Go; no
runtime Node process or administrator API key is shipped to the browser.

## Develop and test

Go 1.26+, PostgreSQL 17. Generated sqlc read queries are checked in. Transactional
claim, ACK and recovery SQL lives in `internal/storage/store.go` so transaction
boundaries and lock ordering remain explicit.

```sh
go mod download
go test ./...                # unit tests; integration tests skip without the URL
docker compose -f compose.test.yaml up -d --wait
export TEST_DATABASE_URL='postgres://taskmesh_test:taskmesh_test_local_only@127.0.0.1:55432/taskmesh_test?sslmode=disable'
go test -race -count=1 -timeout=180s ./...
go test -tags=acceptance -count=1 -timeout=180s ./tests/integration
go vet ./...
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0 generate -f db/sqlc.yaml
docker compose -f compose.test.yaml down
```

PowerShell: set `$env:TEST_DATABASE_URL='postgres://...'` instead of `export`.
Tests create and remove uniquely named schemas in the supplied **test database**;
never point this variable at production. CI requires the URL and never silently
skips integration tests. The additional `acceptance` process test builds the executable, starts
three actual workers plus an API, submits 1000 reports, checks results and restarts
the API. More tests cover concurrent claims, queue/worker limits, transaction
rollback, retries, cancellation, stale ACKs and concurrent idempotency requests.

Native operation:

```sh
go build -o bin/taskmesh ./cmd/taskmesh
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:5432/taskmesh?sslmode=disable'
export TASKMESH_API_KEY='YOUR_64_CHARACTER_RANDOM_HEX_KEY'
bin/taskmesh migrate
bin/taskmesh api
# Other terminals with DATABASE_URL: bin/taskmesh worker, bin/taskmesh controller
```

Worker configuration: `WORKER_NAME`, `WORKER_QUEUES` (comma separated),
`WORKER_CONCURRENCY` (1–16, default 2). `PORT` configures each process's health/HTTP
listener (default 8080); give native processes different ports. Every worker startup
creates a unique session UUID, even when a worker name is reused.

See [architecture](docs/architecture.md), [guarantees](docs/guarantees.md),
[operations and recovery](docs/operations.md), and [verification](docs/verification.md).

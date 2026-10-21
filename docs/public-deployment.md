> Historical Go deployment report. The backend has since migrated to Rust; see
> [current migration and verification](rust-migration.md). Results below describe
> the earlier implementation and are retained as historical evidence.

# Public TaskMesh — recruiter access, 2026-10-21

The running application is available at
**https://luxonis.tail6ad453.ts.net:8443/taskmesh/** without an account or Tailscale.
The portfolio links to it from the home-page button and
`https://adam-sapiecha.github.io/Portfolio-AdamSapiecha/projects.html#taskmesh`.
Both published pages were retrieved and the new links verified.

## What visitors can use

A React/TypeScript console submits actual jobs to the Go engine and displays
PostgreSQL-backed status, results, attempts and transition history. The three
supported tasks accept the visitor's own text, numeric input or hash seed. Runs can
submit 1, 5 or 10 jobs, with selectable priority. The worker view uses real heartbeats
and active lease counts. Polling is every 2.5 seconds. Reloading retains session
results; the console is not a static simulation.

## Isolation and resource bounds

Public stack: `taskmesh-public`, root `/home/pi/apps/taskmesh-public`, active release
`releases/20261021-142913`, image `taskmesh:public-20261021-142913` (native ARM64),
named volume `taskmesh-public_visitor-data`. This is a separate PostgreSQL instance
from the private `taskmesh` stack. Credentials were generated independently in
the public root's mode-0600 `.env`; it contains no private API key.

Only the public console's loopback port 8085 is published. Its executable mode has
no administrative REST endpoints. Existing private port 8084 / tailnet HTTPS 10000
and its 132 stored verification jobs were preserved. Each stack has its own
internal execution network and an API-only ingress network.

Visitor credentials are random 256-bit HttpOnly, Secure, SameSite=Strict cookies,
scoped to `/taskmesh/`; only their hashes are stored. Every public job read, event,
attempt and cancellation validates ownership. Mutation requests require the exact
configured Origin. The runtime has no Docker socket or arbitrary-code handler.

Admission locks and counters share the job transaction:

| Bound | Value |
| --- | --- |
| Session lifetime | 24 hours |
| Accepted jobs per session | 30 |
| Global jobs per fixed clock hour | 200 |
| New visitor sessions per fixed clock hour | 60 |
| Retained public jobs / sessions | 5000 each |
| JSON request body | 8 KiB |
| Hash computation | 10000 rounds |
| Attempt timeout | 15 seconds, cooperative |
| Worker slots | 1 each, three public workers |
| HTTP global rate | 40 requests/sec, burst 80 |
| Concurrent HTTP requests | 12 |

The minute-based retention task deletes only terminal jobs in expired visitor
sessions, along with their attempts/events/idempotency records; it never matches
private jobs. Active jobs are retained until completion/recovery. Limits return a
clear error rather than admitting more work. No paid or message-sending integration
is exposed. The existing host lacks memory-cgroup support, so Compose memory caps
remain advisory/unavailable; the bounded handlers, quotas and CPU/PID caps reduce
exposure. Go's memory target is not a hard memory guarantee.

## Public routing

The host already exposed the posture application using Tailscale Funnel on 8443.
A new `/taskmesh` handler forwards to `127.0.0.1:8085`; the existing `/` posture
handler and all private routes remain unchanged. The prior routing configuration
is saved at `backups/tailscale-serve-before-public.json` under the public root.
This is Funnel (public), not a private Serve-only link.

Public DNS returned Funnel relay IPs. HTTPS readiness was requested using curl
with `--resolve` to a public relay IP and normal certificate validation, bypassing
tailnet DNS/routing. It returned HTTP 200 and schema `002_public_sessions.sql`.
No custom domain, port forwarding, public IP on the Pi or changes to private
administrative services were required. Keep the trailing slash in portfolio links
so relative asset and API paths resolve under `/taskmesh/`.

## Verification

- Frontend TypeScript check and production build passed locally and on ARM64.
- Go vet and vulnerability analysis passed; npm production audit found no known
  vulnerabilities.
- Real PostgreSQL integration tests, including race detection on Windows, passed:
  cross-session read/event/attempt/cancel denial, scoped idempotency, concurrent
  30-job admission limit, global budget/rollback, expiry and retention that preserves
  private jobs. Existing engine/concurrency/recovery tests also passed.
- Public browser QA: submitted five reports with values `[2,4,8,16]`; all completed
  and showed the real result `sum=30`, `mean=7.5`, count 4 and one attempt per job.
  Transition history was visible. Reloading preserved all five jobs.
- The local browser version was checked at a 390px mobile viewport; the published
  desktop version was exercised over HTTPS. The deployed asset hashes match the
  local production build.
- An independent HTTP client verified Secure/HttpOnly/SameSite cookie flags,
  cross-visitor 404, administrative API 404 and invalid-Origin 403. It retained the
  cookie across a console-container restart and recovered the saved result.
- The original owner API and other services remained running. Host reboot, a large
  hostile traffic test and a custom-domain publication were not performed.
- The public PostgreSQL backup was restored into a separate temporary database,
  including both migration checksums and all seven synthetic verification jobs.
  The original private database still contained its 132 jobs. A process-PSS snapshot
  after the public smoke tests measured 77.85 MiB for the new public stack; it is
  an idle/small-workload observation, not a peak-memory guarantee.

## Operate

```sh
ssh luxonis
cd /home/pi/apps/taskmesh-public/current
sudo docker compose -f compose.public.yaml ps
sudo docker compose -f compose.public.yaml logs --tail 100 console controller
sudo docker compose -f compose.public.yaml up -d --no-build --wait
```

The public `.env` also sets `COMPOSE_FILE=compose.public.yaml`, allowing the existing
backup scripts to select the correct public stack from this directory. Backups
and credentials are outside releases. Backups may retain data beyond online
session expiry; restrict them and define retention before backing up real visitor
input. Do not run a cleanup against the private stack or remove its volume.

The TaskMesh repository remains private. Recruiters use the public application and
portfolio description; the page does not advertise an inaccessible source-code link.
The original local portfolio checkout had unrelated uncommitted edits, so the two
published HTML changes were committed in an isolated Git worktree and rebased on
the current upstream main. Those unrelated local edits were not published or reset.

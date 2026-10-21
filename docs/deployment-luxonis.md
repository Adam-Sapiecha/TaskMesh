> Historical Go deployment report. The backend has since migrated to Rust; see
> [current migration and verification](rust-migration.md). Results below describe
> the earlier implementation and are retained as historical evidence.

# TaskMesh on luxonis — deployed 2026-10-21

## Access and current release

- Private HTTPS readiness: <https://luxonis.tail6ad453.ts.net:10000/readyz>.
- API base: `https://luxonis.tail6ad453.ts.net:10000/api/v1` (requires Tailscale and
  `Authorization: Bearer ...`). This is the Phase 1 backend, not a browser dashboard.
- Server root: `/home/pi/apps/taskmesh`; active release: `current` symlink to
  `releases/20261021-135117`.
- Credentials: `/home/pi/apps/taskmesh/.env`, owned by pi, mode 0600. Three
  independent random 256-bit secrets were generated on the server; none are in the
  repository or report. Read the key over your administrative SSH session when
  configuring an API client; never place it in a public frontend.
- Image: `taskmesh:20261021-135117`, built natively on ARM64, image ID
  `sha256:9f38d09240990906b4cf3596a5e3abfc596712160042520121701a3ed9306d73`.
- PostgreSQL: `postgres:17-alpine`, resolved index digest
  `sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24`.

No public domain or public Internet route was configured. Tailscale Serve on
HTTPS port 10000 is **tailnet only**. Existing routes on ports 443/8443 and the
private administrative services were preserved. The previous Serve configuration
was saved in `backups/tailscale-serve-before-20261021-135117.json`.

## Running services and isolation

Stack `taskmesh`: PostgreSQL, API, controller and three independent workers.
The migration container exits successfully after transactional migration and
runtime-role grants. Each worker starts with concurrency **1** on this server.
Only the API is published, to **127.0.0.1:8084**. PostgreSQL/worker ports are not
published. Network `taskmesh_taskmesh` is internal; `taskmesh_ingress` attaches only
the API and enables loopback port publication. The second network was necessary
because the engine does not publish ports for an internal-only container.

Data is stored in `taskmesh_postgres-data`, independent of all other applications.
Long-lived containers use `unless-stopped`; Docker and tailscaled are enabled at
boot. No whole-server reboot was performed. Existing services and protected
DepthAI/Luxonis paths were not changed. Temporary test containers/network were
removed after validation; no global Docker cleanup was used.

## Verified on ARM64 and through HTTPS

1. Built the Docker image on the Pi and started the complete Compose stack.
   All six long-lived containers are healthy; migration exits 0.
2. Submitted 12 report jobs through the deployed API, checked all results and
   attempts, authentication rejection, invalid-handler rejection, idempotent reuse
   and conflict detection. Re-read results through HTTPS.
3. Created a PostgreSQL custom-format backup and restored it into a separate
   temporary database with the provided scripts. The 12 completed jobs, attempts
   and migration checksum were present. The temporary restored DB was then dropped.
4. Removed/recreated **only TaskMesh containers and networks**, retaining its named
   volume. All 12 original results survived, and the HTTPS check passed again.
5. Requested the HTTPS readiness URL from the Windows computer with normal TLS
   certificate verification: HTTP 200, `status=ready`, schema `001_core.sql`.
6. Ran `go test -tags=acceptance -count=1 -v -timeout=240s ./...` natively on ARM64,
   using a **separate disposable PostgreSQL test stack**. Unit/integration tests and
   the real three-process acceptance test passed. The latter processed 1000 reports
   with all three worker identities, checked results and restarted the API.
   Measured acceptance flow: 4.657 seconds / 214.7 jobs/s including submission.
   The test container had a 1.5-CPU cap and did not use production service quotas;
   this is a functional test observation, not a production throughput guarantee.
7. The optional native pg_ctl/dump tests were skipped in that Go test container;
   deployed backup/restore and PostgreSQL container recreation were verified
   separately as above. Windows baseline tests covered native pg_ctl restart.
8. Submitted another 120 report jobs through the deployed HTTPS API while sampling
   process memory. All results were correct. These are explicitly deployment-test
   inputs, not private user data; they remain in the database and no reset exists.

## Resource measurement and host limitations

Docker reports **no memory/swap limit support** on this kernel and ignores Compose
`mem_limit`. CPU and PID controllers are present. No boot/kernel configuration was
changed and no reboot was attempted to enable memory cgroups. Go's GOMEMLIMIT is
a runtime target, not a hard process/container memory cap.

Docker memory stats returned 0/0, so memory was measured from `/proc/*/smaps_rollup`
as summed proportional set size (PSS) for processes belonging to this stack. After
the initial smoke check the sum was 87.97 MiB. During the 120-job HTTPS scenario,
38 samples recorded a maximum **91.17 MiB PSS**, including PostgreSQL. The sampled
scenario took 25.158 seconds including HTTP validation/readback and sampling
overhead. This does not include all kernel/filesystem cache or Docker daemon memory
and does not predict peaks for other handlers or ten simultaneous applications.
The measurement is saved at `/home/pi/apps/taskmesh/resource-measurement.json`.

After tests: about 6050 MiB available RAM, 28 GiB free filesystem space, swap unused.
Concurrency remains 1 per worker. Hard memory isolation requires a separate host
configuration/maintenance change; it has not been silently assumed to work.

The ARM64 race-detector attempt could not run on this host: ThreadSanitizer reported
`unsupported VMA range`, `Found 47 - Supported 48`. Ordinary ARM64 tests passed;
race tests had previously passed on Windows. No kernel/address-space changes were
made to force the detector to run.

## Maintenance

```sh
ssh luxonis
cd /home/pi/apps/taskmesh/current
sudo docker compose ps
sudo docker compose logs --tail 100 api controller worker-1 worker-2 worker-3
python3 scripts/smoke.py --base-url https://luxonis.tail6ad453.ts.net:10000 \
  --snapshot /home/pi/apps/taskmesh/smoke-results.json --verify-only
sudo python3 scripts/resources.py
sudo sh scripts/backup.sh
sudo sh scripts/verify-backup.sh /home/pi/apps/taskmesh/backups/taskmesh-TIMESTAMP.dump
```

Backups are outside release folders, in `/home/pi/apps/taskmesh/backups`. The first
verified archive is `taskmesh-20261021T115558Z.dump`; timestamps in archive names are
UTC. Backups require off-server copying/retention to protect against microSD loss;
neither scheduled off-server backup nor public publication was configured.
The final deployment backup `taskmesh-20261021T115932Z.dump` was also restored
successfully and contains all 132 completed deployment-test jobs and attempts.

Normal shutdown: `sudo docker compose stop`. Restart: `sudo docker compose up -d
--no-build --wait`. Never add `-v` to `down` while preserving data. See
[operations](operations.md) for update/rollback constraints. This is the first
release on the server, so no earlier TaskMesh version exists for code rollback.

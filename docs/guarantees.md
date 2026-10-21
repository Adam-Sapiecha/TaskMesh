# Guarantees and limits

| Property | Implemented behavior |
| --- | --- |
| Durability | HTTP 201 only after PostgreSQL commit; ambiguous network failures require idempotent retry |
| Execution | At least once while DB, supported workers and recovery controller eventually remain available |
| Current owner | At most one valid lease per job; atomic claim |
| Stale writes | ACK and renewal require current owner, generation, RUNNING state and unexpired lease |
| Side effects | No exactly-once promise; handler integrations must provide their own idempotency/fencing |
| Submission deduplication | One instance-wide key and normalized request hash; different body produces 409 |
| Ordering | Priority then run_at then ID; no strict FIFO, no priority aging |
| Global limits | Queue-row and worker-row locking protect capacity checks |
| Cancellation | Terminal state immediately; cooperative execution cancellation |
| Persistence | Named PostgreSQL volume survives container replacement and normal restart |
| Isolation | One trusted project per dedicated stack; no RBAC or shared anonymous access |
| Availability | One PostgreSQL instance; no DB HA, multi-region or automatic backup service |

If PostgreSQL is unavailable, submissions fail with 503 and are never acknowledged
as accepted. Workers stop acquiring, cancel on renewal failure, and retry polling.
The controller resumes scans when the connection pool reconnects. Retry budgets
bound startup connection attempts; Compose restarts failed long-lived processes.

Priority starvation is possible under continuous high-priority load. Claims are
bounded by polling latency, process capacity and contention. An accepted job whose
handler has no compatible worker stays queued. Native worker code is trusted and
has direct database access; use gRPC plus authorization before permitting remote
untrusted workers. Idempotency keys have no TTL in this release; plan disk retention
with explicit archival. Application tokens authorize all jobs in their instance.

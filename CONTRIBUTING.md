# Contributing

Keep claim/ACK/recovery changes covered by real PostgreSQL integration tests.
Describe ownership, lock order, rollback behavior and safety/liveness implications.
Run formatting, vet, tests and the race detector as documented in README.
Regenerate sqlc queries when their SQL changes. Never modify an applied migration;
add a new compatible migration with a rollback and backup plan.

Handlers must validate bounded inputs, honor context cancellation, avoid logging
private data and explicitly describe side-effect idempotency. No arbitrary shell
execution is accepted in the trusted in-process runtime.

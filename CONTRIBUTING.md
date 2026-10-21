# Contributing

Keep claim/ACK/recovery changes covered by real PostgreSQL integration tests.
Describe ownership, lock order, rollback behavior and safety/liveness implications.
Run cargo fmt, clippy and the Rust tests as documented in README.
Keep SQLx transaction boundaries explicit. Never modify an applied migration;
add a new compatible migration with a rollback and backup plan.

Handlers must validate bounded inputs, yield for timeout/cancellation, avoid logging
private data and explicitly describe side-effect idempotency. No arbitrary shell
execution is accepted in the trusted in-process runtime.

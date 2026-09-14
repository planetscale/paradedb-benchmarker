# WAL per updated record

Phase-controlled workloads that enable updates report PostgreSQL WAL generated per
successfully updated document. The dashboard computes a cumulative running
average for each backend phase:

```text
(current WAL insert position - phase-start WAL insert position)
----------------------------------------------------------------
                 successfully completed updates
```

The phase-start position is captured after that backend's optional prewarm,
after every worker reaches the start barrier, and immediately before its
measured phase. The final position is captured after the phase updater and
query workers finish and before the next backend phase is released.
Intermediate positions and their paired completed-update counts are sampled by
the metrics collector, not by the update path.

The live ratio is sampled rather than transactionally captured, so an update
committing at the exact sampling instant can make one refresh briefly lag by a
record. The forced final sample runs after the updater stops and uses a stable
completed-update count.

This is intentionally a server-wide PostgreSQL WAL measurement. It includes
WAL produced by queries and foreground or background maintenance between the
two boundary samples. Background work that lands during the short finalization
boundary is consequently included; this is intentional system-level write
amplification rather than update-statement-only accounting.
Read-only workloads and backends without PostgreSQL WAL support omit the
metric entirely.

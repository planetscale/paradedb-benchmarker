# PostgreSQL index I/O statistics

## Goal

Show logical index bytes accessed per completed query for the search indexes
used by the ParadeDB, pg_textsearch, and native PostgreSQL FTS benchmark
backends without adding work to the timed query path.

## Behavior

- For managed containers, reset current-database statistics during k6
  initialization, before workload scenarios can run. A reset failure aborts
  the benchmark. Connections configured with `container: ""` instead subtract
  a client-side baseline without resetting the remote database's statistics.
- On the dedicated collector VU, query `pg_statio_user_indexes` once per second
  through that VU's PostgreSQL connection. Workload VUs use different driver
  instances and connections.
- Restrict the aggregate to each backend's search access method: `paradedb` for
  ParadeDB, `bm25` for pg_textsearch, and `gin` for `postgres`. Elasticsearch does not
  participate.
- Keep `idx_blks_read` and `idx_blks_hit` separate. Convert both to bytes using
  `block_size`, and return their display values from SQL with
  `pg_size_pretty()`.
- Store the latest successful cumulative snapshot by backend alias. A polling
  failure retains the last good value and does not fail an active workload.
- Schedule a one-iteration finalizer after all workload phases and force a last
  snapshot before k6 exits or a subsequent benchmark can start.
- Display `(read bytes + hit bytes) / completed queries` immediately to the
  right of `QUERIES` as `PER QUERY`. Preserve the underlying counters in
  JSON/standalone HTML exports. Elasticsearch participates in the workload but
  omits this card because it has no comparable per-index byte counters.

## Non-goals

- Elasticsearch I/O accounting.
- Per-query deltas or a historical I/O timeline.
- Counting heap, TOAST, temporary, WAL, extension-private, or operating-system
  cache I/O. These counters describe PostgreSQL relation-block activity for the
  selected index access method.

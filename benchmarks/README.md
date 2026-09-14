# Wikipedia and Stack Exchange benchmarks

Both datasets use the same k6 workload in `benchmarks/search.js`, backend
definitions in `benchmarks/compose.yml`, and setup recipes in
`benchmarks/Makefile.common`. The two Makefile entry points select the dataset
and its default ports. No dataset-specific Go code is needed.

## Create the databases

Place the dataset's `data.csv.gz` in `datasets/wikipedia/` or
`datasets/stackexchange/`. Compressed and decompressed CSVs are excluded from
Git; the corresponding `data-manifest.json`, `SHA256SUMS`, and `source.json`
describe the files. These are gzip level 9 archives of the existing source
CSVs, with filename and timestamp headers omitted.

```bash
make -f Makefile.wikipedia create
make -f Makefile.stackexchange create BACKENDS=paradedb,postgres,elasticsearch
```

`create` decompresses the CSV and verifies its SHA-256 checksum. It builds the
extension images, then loads and indexes the selected backends one at a time.
Each backend gets its own database volume and an independent import. The
existing loader parses CSV records, including embedded newlines, and uses
PostgreSQL binary COPY or Elasticsearch bulk requests. PostgreSQL's generated
`tsvector` is computed during import.

Images are pinned by the Docker configuration. Each container defaults to 8 CPUs,
with 64 GB of memory during `create` and 32 GB during `run`. Every PostgreSQL
backend uses `shared_buffers=24GB` and `maintenance_work_mem=24GB` for every
target. These are the tuning overrides supplied by the Makefiles, alongside
the required extension preloads. ParadeDB's image also writes tuning settings
during initialization, including `max_parallel_workers_per_gather` at half the
CPU count (4 with the default 8 CPUs); PostgreSQL and pg_textsearch use the
PostgreSQL default of 2 for that setting. The index definitions preserve the
existing plain backend configurations: ParadeDB's
default tokenizer with eight target segments, the PostgreSQL `simple` text
configuration for PostgreSQL and pg_textsearch, and Elasticsearch's `standard`
analyzer with eight shards. Elasticsearch uses zero replicas and disables its
query cache.

Each setup stage prints to the terminal. Per-backend load/index logs, including
PostgreSQL index creation time in milliseconds and index size in bytes, are
saved in `benchmarks/.state/<project>/logs/`. Successful PostgreSQL loads are
recorded separately from index completion: rerunning `create` after an index
failure preserves the loaded table and retries the index stage. An incomplete
Elasticsearch load is restarted. Completed backends are skipped. Keep the
state directory together with its Docker volumes.

Changing the data, schema, or index configuration of a completed backend
requires explicitly requesting `RECREATE=1`. This drops and reloads the selected
backend's table or Elasticsearch index:

```bash
make -f Makefile.wikipedia create BACKENDS=postgres RECREATE=1
```

Setup stops this project's containers before loading and stops each backend
afterward, including on failure. Other Compose projects are independent.

To drop the benchmark data for selected backends, use `clean`:

```bash
make -f Makefile.wikipedia clean BACKENDS=pg_textsearch
```

`clean` drops the PostgreSQL `documents` table and its indexes, or the
Elasticsearch `documents` index, and clears the selected backends' load/index
completion markers. It starts containers with existing data volumes as needed
and stops each selected container afterward. Docker volumes, source CSVs,
archives, and logs are retained.
Missing volumes are skipped; repeating `clean` is safe. The next `create` reloads
the cleaned backends. Omit `BACKENDS` to clean all four default backends.

## Run a benchmark

GNU Make accepts `NAME=value` arguments rather than `-D` parameters:

```bash
make -f Makefile.wikipedia run \
  WORKLOAD=topk QUERY_STYLE=disjunction \
  BACKENDS=paradedb,pg_textsearch,postgres,elasticsearch \
  VUS=8 DURATION=60s

make -f Makefile.stackexchange run \
  WORKLOAD=count QUERY_STYLE=mixed \
  BACKENDS=paradedb,postgres,elasticsearch \
  VUS=8 DURATION=10m
```

`run` uses already-created volumes. It starts the selected containers, runs
backends sequentially in the order supplied, and stops them after the run,
including on interruption. It does not reload or restore data between runs.
The Makefiles serialize `create`, `run`, and `clean` within each project.

| Variable               | Default                                  | Meaning                                                                            |
| ---------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------- |
| `WORKLOAD`             | `topk`                                   | `topk` or `count`                                                                  |
| `QUERY_STYLE`          | `disjunction`                            | `conjunction`, `disjunction`, `phrase`, or `mixed`                                 |
| `BACKENDS`             | All compatible backends                  | Comma-separated backend names in execution order                                   |
| `VUS`                  | `8`                                      | Concurrent query workers per backend                                               |
| `DURATION`             | `60s`                                    | Full measured duration per backend                                                 |
| `PREWARM`              | `10s`                                    | Unmeasured queries before each measured phase; `0s` disables warm-up               |
| `COOLDOWN`             | `0s`                                     | Host filesystem sync and idle time after stopping a backend, before the next phase |
| `TOP_K`                | `10`                                     | Number of hits requested for `topk`                                                |
| `SEED`                 | `1592614637`                             | Repeatable shuffle of query records/styles; unsigned 32-bit integer                |
| `QUERIES`              | Dataset's `queries.json`                 | Path to another compatible query file                                              |
| `OUTPUT`               | `live`                                   | `live`, `json`, `html`, or a comma-separated combination                           |
| `OUT_DIR`              | `out/<dataset>-<workload>-<query-style>` | Logs and timestamped dashboard exports                                             |
| `CPUS`                 | `8`                                      | Docker CPU limit per backend                                                       |
| `MEMORY`               | `64g` for `create`; `32g` otherwise      | Docker memory limit per backend; explicit overrides apply to either target         |
| `SHARED_BUFFERS`       | `24GB`                                   | PostgreSQL shared buffers during both creation and benchmark runs                  |
| `MAINTENANCE_WORK_MEM` | `24GB`                                   | PostgreSQL maintenance memory for every target                                     |
| `POSTGRES_SHM_SIZE`    | `16g`                                    | Docker `/dev/shm` capacity for PostgreSQL backends                                 |
| `WORKERS`              | `1`                                      | Parallel workers during CSV loading                                                |
| `BATCH_SIZE`           | `10000`                                  | Rows per load batch                                                                |
| `BUILD`                | `1`                                      | Set to `0` to reuse already-built images                                           |
| `PROJECT`              | `bench-datasets-<dataset>`               | Docker project and container prefix                                                |

pg_textsearch supports only `WORKLOAD=topk QUERY_STYLE=disjunction` in this
runner. Other selections default to ParadeDB, PostgreSQL, and Elasticsearch.
Explicitly selecting pg_textsearch with an unsupported combination fails before
container startup. `mixed` executes all three query styles with equal frequency
over a complete pass; no styles are silently substituted.

PostgreSQL backends first prewarm the search index, then all backends run the
normal query stream unmeasured for `PREWARM`. The measured phase continues that
stream. Query forms are read directly from JSON, and each engine parses them
using its configured analyzer. Query failures produce a failing k6 threshold;
an ordinary cancellation at the end of a measurement phase is excluded.

The default `OUTPUT=live` starts only the live dashboard. For files, use
`OUTPUT=json,html` or `OUTPUT=live,json,html`. Logs are written while running;
requested JSON and HTML dashboard files are written when k6 shuts down.
Interactive runs preserve k6's terminal progress bars while recording the
session to the log file, including ANSI escape sequences. Redirected or piped
runs produce plain progress output.

## Query files and data layout

Both CSVs have `id,body` headers and load into a `documents` table or index.
The source body text and IDs are preserved during archive creation.

- Wikipedia's `queries.json` contains all 302 existing queries.
- Stack Exchange's `queries.json` contains the existing 1,254 queries.
- Stack Exchange's `queries.shortened.json` preserves the 573-query subset and
  its recorded sampling seed and target counts by length.

```bash
make -f Makefile.stackexchange run \
  WORKLOAD=topk QUERY_STYLE=disjunction BACKENDS=paradedb,postgres \
  QUERIES="$PWD/datasets/stackexchange/queries.shortened.json"
```

The files contain engine forms for ParadeDB, pg_textsearch, PostgreSQL, and
Elasticsearch. Compression and backend filtering do not resample the queries
or change their text.

## Isolation and dependencies

| Dataset        | ParadeDB | PostgreSQL | pg_textsearch | Elasticsearch |
| -------------- | -------- | ---------- | ------------- | ------------- |
| Wikipedia      | 35432    | 35433      | 35435         | 39200         |
| Stack Exchange | 45432    | 45433      | 45435         | 49200         |

Ports are bound to loopback. Override `PARADEDB_PORT`, `POSTGRES_PORT`,
`PG_TEXTSEARCH_PORT`, or `ELASTICSEARCH_PORT` to change them. Containers are
named `<project>-<backend>` and use project-scoped named volumes. PostgreSQL
and Elasticsearch data survive `stop` and subsequent `run` invocations.

Required tools are GNU Make, Bash, Docker with Compose, gzip, sha256sum, flock,
util-linux's `script` for interactive runs, and Go when building the loader or
k6 extension. Node.js is used only by the JavaScript unit tests. No npm
dependencies are needed.

For local fixtures or externally stored data, `DATA_GZ`, `DATA_CSV`, and
`CHECKSUMS` can override the default file paths. `CHECKSUMS` must contain a
`data.csv` SHA-256 entry matching that CSV. `STATE_DIR` can also be overridden.
Use absolute paths for overrides. `MEMORY` and `CPUS` control Docker limits;
`SHARED_BUFFERS` and `MAINTENANCE_WORK_MEM` control PostgreSQL's buffer and
maintenance allocations independently.

```bash
make -f Makefile.wikipedia help
make -f Makefile.wikipedia stop
node --test benchmarks/queries.test.js
```

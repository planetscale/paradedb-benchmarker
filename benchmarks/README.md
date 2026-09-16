# Wikipedia and Stack Exchange benchmarks

Both datasets use the same k6 workload in `benchmarks/search.js`, backend
definitions in `benchmarks/compose.yml`, and setup recipes in
`benchmarks/Makefile.common`. The two Makefile entry points select the dataset
and its default ports. No dataset-specific Go code is needed.

See [SOURCES.md](../SOURCES.md) for the corpus and query origins, preparation,
licensing references, and checksums.

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
backend defaults to `shared_buffers=24GB`, `maintenance_work_mem=24GB`,
`max_parallel_maintenance_workers=8`, `max_parallel_workers=8`, and
`max_parallel_workers_per_gather=2`
for every target. These settings are passed
on the PostgreSQL command line, alongside the required extension preloads,
so the worker limits override ParadeDB's CPU-based bootstrap values. Other
tuning supplied by backend images still applies. The index definitions
preserve the existing plain backend configurations: ParadeDB's
default tokenizer with eight target segments, the PostgreSQL `simple` text
configuration for PostgreSQL and pg_textsearch, and Elasticsearch's `standard`
analyzer with eight shards. Elasticsearch uses zero replicas and disables its
query cache.

Each setup stage prints to the terminal. Per-backend load/index logs, including
PostgreSQL index creation time in milliseconds and index size in bytes, are
saved in `benchmarks/.state/<project>/logs/` for Docker's default storage, or
`benchmarks/.state/<project>/volumes/<root-id>/logs/` for a custom `VOLUME_ROOT`.
Successful PostgreSQL loads are
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

Setup stops this storage location's containers before loading and stops each
backend afterward, including on failure. Before starting a backend, the
Makefiles also stop its running copies at other storage locations for the same
logical `PROJECT`, since they share host ports. Other projects are independent.

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

## Choose database storage

Leave `VOLUME_ROOT` unset to keep Docker's default volume storage. To place
database files on another filesystem, pass an absolute directory on the local
Docker host. For a fresh import:

```bash
make -f Makefile.wikipedia create VOLUME_ROOT=/tin/benchmarker
```

Both dataset Makefiles support this parameter. Database directories are
`<VOLUME_ROOT>/<PROJECT>/<backend>`, for example
`/tin/benchmarker/bench-datasets-wikipedia/paradedb`. `create` creates the
directories; the backend containers set their data-directory ownership. The root must be
writable by the user running Make. Docker uses the host directories as backing
storage for named volumes through the
[local volume driver](https://docs.docker.com/reference/compose-file/volumes/#driver_opts).
Image layers, source CSVs, setup logs, and benchmark output keep their existing
filesystems.

To copy an already-created database, use `copy`. `VOLUME_ROOT` selects the
source (unset means Docker's default storage); `TARGET_VOLUME_ROOT` is required:

```bash
make -f Makefile.wikipedia copy BACKENDS=paradedb TARGET_VOLUME_ROOT=/tin/benchmarker
make -f Makefile.wikipedia run BACKENDS=paradedb VOLUME_ROOT=/tin/benchmarker
```

The same target works for Stack Exchange and accepts a comma-separated
`BACKENDS` list. Omitting `BACKENDS` uses the usual backend defaults. A source
at a custom location can be copied again:

```bash
make -f Makefile.wikipedia copy BACKENDS=paradedb \
  VOLUME_ROOT=/tin/benchmarker TARGET_VOLUME_ROOT=/another/disk/benchmarker
```

`copy` requires completed source backends. It stops those source containers
cleanly, copies their complete data directories (including indexes, WAL, and
configuration), preserves ownership and permissions, and registers stopped
destination containers. It also copies the setup completion records, so `run`
can use the copy immediately without importing or indexing again. The copy
helper uses the installed backend images and Docker permissions; it does not
require `sudo`. Progress appears in the terminal and in
`benchmarks/.state/<project>/logs/<timestamp>-copy-<pid>.log`.

Existing target data is never overwritten. Transfers use temporary directories;
failed or interrupted transfers do not get a completion record. If file copying
completed but Docker registration failed, rerunning the same command finishes
registration. The original data remains available: omit `VOLUME_ROOT` to run
the original Docker-default copy again, or pass its previous root.

The custom-storage configuration mounts plain PostgreSQL's volume at its
actual PostgreSQL 18 data directory. The default configuration retains its
existing mount layout so previously loaded databases remain accessible. `copy`
resolves the actual source mount, including the original plain PostgreSQL
container's anonymous parent volume.

Use the same `VOLUME_ROOT` and `PROJECT` for `create`, `run`, and `clean`.
Each root automatically gets separate Docker container/volume names and setup
records; no additional `PROJECT` argument is needed when switching locations.
The internal Compose project is `<PROJECT>-v<root-id>`, where `root-id` is derived
from the normalized absolute root. Docker-default storage retains the original
names. `clean` drops data and clears completion records only at the selected
root; it retains the Docker volumes and host directories.

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
The Makefiles serialize `create`, `copy`, `run`, and `clean` across all storage
locations within each logical project.

| Variable               | Default                                  | Meaning                                                                            |
| ---------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------- |
| `WORKLOAD`             | `topk`                                   | `topk` or `count`                                                                  |
| `QUERY_STYLE`          | `disjunction`                            | `conjunction`, `disjunction`, `phrase`, or `mixed`                                 |
| `BACKENDS`             | All compatible backends                  | Comma-separated backend names in execution order                                   |
| `VUS`                  | `8`                                      | Concurrent query workers per backend                                               |
| `UPDATES_PER_SECOND`   | `0`                                      | Maximum paced document updates per second per active PostgreSQL backend; zero disables updates |
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
| `MAX_PARALLEL_MAINTENANCE_WORKERS` | `8`                           | PostgreSQL parallel maintenance worker limit for every target                      |
| `MAX_PARALLEL_WORKERS` | `8`                                    | Total parallel worker limit per PostgreSQL backend for every target                |
| `POSTGRES_SHM_SIZE`    | `16g`                                    | Docker `/dev/shm` capacity for PostgreSQL backends                                 |
| `WORKERS`              | `1`                                      | Parallel workers during CSV loading                                                |
| `BATCH_SIZE`           | `10000`                                  | Rows per load batch                                                                |
| `BUILD`                | `1`                                      | Set to `0` to reuse already-built images                                           |
| `PROJECT`              | `bench-datasets-<dataset>`               | Logical project; custom storage adds an automatic suffix to Docker names             |
| `VOLUME_ROOT`          | Unset (Docker default storage)           | Absolute host root for database files, under `<root>/<project>/<backend>`            |
| `TARGET_VOLUME_ROOT`   | Required for `copy`                      | Absolute host root for the copied database files                                    |

pg_textsearch supports only `WORKLOAD=topk QUERY_STYLE=disjunction` in this
runner. Other selections default to ParadeDB, PostgreSQL, and Elasticsearch.
Explicitly selecting pg_textsearch with an unsupported combination fails before
container startup. `mixed` executes all three query styles with equal frequency
over a complete pass; no styles are silently substituted.

To run queries alongside document updates:

```bash
make -f Makefile.wikipedia run \
  BACKENDS=paradedb,pg_textsearch,postgres UPDATES_PER_SECOND=10
```

`UPDATES_PER_SECOND` applies to both dataset Makefiles and accepts integers from
0 through 1,000,000,000. A positive value adds one updater VU alongside each
backend's query VUs; `VUS` continues to count only query workers. Each updater
appends one space to a randomly selected document's `body`, preserving its
search terms while exercising normal table and index updates. The three
PostgreSQL backends support this operation. Selecting Elasticsearch with a
positive update rate fails validation before container startup.

Updates share the query phase's measurement window. The rate limits scheduled
update starts; slow updates can produce a lower completion rate. Each updater
also performs two committed startup updates before measurement, which are
excluded from the reported update count. With the default of zero, no updater
VU or startup updates are created. Updates persist across benchmark runs.
The dashboard reports confirmed updates and PostgreSQL WAL per updated record;
update failures produce a failing k6 threshold. See
[paced random updates](../docs/scripting.md#paced-random-updates) for details.

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
named `<compose-project>-<backend>` and use project-scoped named volumes. PostgreSQL
and Elasticsearch data survive `stop` and subsequent `run` invocations.

Required tools are GNU Make, Bash, Docker with Compose, gzip, sha256sum, flock,
util-linux's `script` for interactive runs, and Go when building the loader or
k6 extension. Setting `VOLUME_ROOT` also requires `realpath` and Compose 2.24.4
or newer (for the volume mount override). The `copy` target additionally needs
Python 3.9 or newer and uses the locally installed backend images. Node.js is
used only by the JavaScript unit tests. No npm dependencies are needed.

For local fixtures or externally stored data, `DATA_GZ`, `DATA_CSV`, and
`CHECKSUMS` can override the default file paths. `CHECKSUMS` must contain a
`data.csv` SHA-256 entry matching that CSV. `STATE_DIR` can also be overridden;
custom storage keeps its records in `STATE_DIR/volumes/<root-id>/`.
Use absolute paths for overrides. `MEMORY` and `CPUS` control Docker limits;
`SHARED_BUFFERS` and `MAINTENANCE_WORK_MEM` control PostgreSQL's buffer and
maintenance allocations independently.

```bash
make -f Makefile.wikipedia help
make -f Makefile.wikipedia stop
node --test benchmarks/queries.test.js
```

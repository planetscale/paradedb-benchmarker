// Package postgres provides the shared PostgreSQL driver implementation.
// Individual PostgreSQL-based backends (paradedb, postgres)
// import this package and register themselves separately.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nickbruun/pgsplit"
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
)

const (
	// A cancel request should normally interrupt a PostgreSQL query immediately.
	// Keep a bounded network deadline as a fallback for a backend that does not
	// process interrupts, without using pgx's default immediate connection close.
	queryCancelDeadlineDelay = 5 * time.Second

	// Every benchmark VU owns one pool with one connection. Effectively disable
	// age-based recycling so that backend-local state survives from pg_prewarm,
	// through query prewarm, and to the end of measurement.
	benchmarkConnectionLifetime = time.Duration(1<<63 - 1)
)

// ConfigQuery is a custom SQL query whose result is captured during CaptureConfig.
// A query returning a single text column stores its first row under Key. A query
// returning two text columns is treated as (name, value) rows; each name is
// grouped into a config section by its dot-prefix, the same way GUCs are
// (e.g., "paradedb.segments_at_run_start.idx" lands in the "paradedb" section).
type ConfigQuery struct {
	Key   string // Config map key for single-column results (e.g., "paradedb_version")
	Query string // SQL to execute (e.g., "SELECT paradedb.version_info()::text")
}

// Driver implements the backends.Driver interface for PostgreSQL.
type Driver struct {
	pool                    *pgxpool.Pool
	connString              string
	indexIOAccessMethods    []string
	segmentDiagnosticsQuery string
	extraGUCs               []string      // Additional GUCs to capture (e.g., "paradedb.xxx")
	extraGUCPrefixes        []string      // GUC prefixes captured wholesale (e.g., "paradedb")
	extraQueries            []ConfigQuery // Additional SQL queries to capture
}

// New creates a new PostgreSQL driver.
func New(connString string) (backends.Driver, error) {
	ctx := context.Background()

	config, err := newPoolConfig(connString)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	return &Driver{pool: pool, connString: connString}, nil
}

func newPoolConfig(connString string) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config.MaxConns = 1
	config.MinConns = 1
	config.MaxConnLifetime = benchmarkConnectionLifetime
	config.MaxConnIdleTime = benchmarkConnectionLifetime
	config.ConnConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{
			Conn:               conn,
			CancelRequestDelay: 0,
			DeadlineDelay:      queryCancelDeadlineDelay,
		}
	}

	return config, nil
}

// Close closes the connection pool.
func (d *Driver) Close() error {
	if d.pool != nil {
		d.pool.Close()
	}
	return nil
}

// Pool returns the underlying connection pool for custom queries.
func (d *Driver) Pool() *pgxpool.Pool {
	return d.pool
}

// SetExtraGUCs sets additional GUCs to capture in CaptureConfig.
// GUCs are grouped by prefix (e.g., "paradedb.xxx" -> "paradedb" section).
func (d *Driver) SetExtraGUCs(gucs []string) {
	d.extraGUCs = gucs
}

// SetExtraGUCPrefixes sets GUC prefixes to capture wholesale in CaptureConfig.
// Every pg_settings entry under "<prefix>." is captured into a section named
// after the prefix (e.g., "paradedb" -> all "paradedb.*" GUCs).
func (d *Driver) SetExtraGUCPrefixes(prefixes []string) {
	d.extraGUCPrefixes = prefixes
}

// SetExtraQueries sets additional SQL queries to run during CaptureConfig.
// See ConfigQuery for the supported result shapes.
func (d *Driver) SetExtraQueries(queries []ConfigQuery) {
	d.extraQueries = queries
}

// SetIndexIOStatsAccessMethods enables index I/O collection for the named
// PostgreSQL index access methods.
func (d *Driver) SetIndexIOStatsAccessMethods(methods ...string) {
	d.indexIOAccessMethods = append([]string(nil), methods...)
}

// SetSegmentDiagnosticsQuery configures an optional query returning per-index
// segment rows for inclusion in phase diagnostics. The expected columns are
// index name, relation bytes, kind, source state, origin, sequence, documents,
// dead documents, postings, pages, and root block.
func (d *Driver) SetSegmentDiagnosticsQuery(query string) {
	d.segmentDiagnosticsQuery = query
}

// IndexIOStatsEnabled reports whether this PostgreSQL specialization opted in.
func (d *Driver) IndexIOStatsEnabled() bool {
	return len(d.indexIOAccessMethods) > 0
}

// IndexIOStatsIdentity identifies the database whose statistics are reset.
func (d *Driver) IndexIOStatsIdentity() string {
	return d.connString
}

// ResetIndexIOStats resets statistics for the current database. This does not
// evict buffers or modify database contents.
func (d *Driver) ResetIndexIOStats(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, "SELECT pg_stat_reset()")
	return err
}

// ReadIndexIOStats returns cumulative reads and shared-buffer hits for indexes
// using the configured search access methods.
func (d *Driver) ReadIndexIOStats(ctx context.Context) (backends.IndexIOStats, error) {
	const query = `
		WITH totals AS (
			SELECT
				COALESCE(SUM(stats.idx_blks_read), 0)::bigint
					* current_setting('block_size')::bigint AS read_bytes,
				COALESCE(SUM(stats.idx_blks_hit), 0)::bigint
					* current_setting('block_size')::bigint AS hit_bytes
			FROM pg_statio_user_indexes AS stats
			JOIN pg_class AS index_relation ON index_relation.oid = stats.indexrelid
			JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
			WHERE access_method.amname = ANY($1)
		)
		SELECT
			read_bytes,
			hit_bytes,
			CASE WHEN read_bytes = 0 THEN '0B' ELSE pg_size_pretty(read_bytes) END,
			CASE WHEN hit_bytes = 0 THEN '0B' ELSE pg_size_pretty(hit_bytes) END
		FROM totals
	`

	var stats backends.IndexIOStats
	err := d.pool.QueryRow(ctx, query, d.indexIOAccessMethods).Scan(
		&stats.ReadBytes,
		&stats.HitBytes,
		&stats.Read,
		&stats.Hit,
	)
	return stats, err
}

// ReadWALPosition returns PostgreSQL's current WAL insert location as an
// absolute byte position.
func (d *Driver) ReadWALPosition(ctx context.Context) (uint64, error) {
	var lsn string
	if err := d.pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&lsn); err != nil {
		return 0, err
	}
	return parseWALLSN(lsn)
}

// ResetPostgresDiagnostics resets database-local and shared cumulative counters
// at the measured phase boundary. It does not evict buffers or alter data.
func (d *Driver) ResetPostgresDiagnostics(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `
		SELECT
			pg_stat_reset(),
			pg_stat_reset_shared('io'),
			pg_stat_reset_shared('wal'),
			pg_stat_reset_shared('bgwriter'),
			pg_stat_reset_shared('checkpointer')
	`)
	return err
}

// ReadPostgresDiagnostics reads cumulative counters and instantaneous wait and
// segment state without emitting benchmark query metrics.
func (d *Driver) ReadPostgresDiagnostics(ctx context.Context) (backends.PostgresDiagnosticsSample, error) {
	var sample backends.PostgresDiagnosticsSample
	const cumulativeQuery = `
		SELECT
			wal.wal_records,
			wal.wal_fpi,
			wal.wal_bytes::bigint,
			wal.wal_buffers_full,
			checkpointer.num_timed,
			checkpointer.num_requested,
			checkpointer.num_done,
			checkpointer.write_time,
			checkpointer.sync_time,
			checkpointer.buffers_written,
			bgwriter.buffers_clean,
			bgwriter.maxwritten_clean,
			bgwriter.buffers_alloc,
			database.blks_read,
			database.blks_hit,
			database.blk_read_time,
			database.blk_write_time,
			database.temp_files,
			database.temp_bytes,
			database.deadlocks
		FROM pg_stat_wal AS wal
		CROSS JOIN pg_stat_checkpointer AS checkpointer
		CROSS JOIN pg_stat_bgwriter AS bgwriter
		CROSS JOIN LATERAL (
			SELECT * FROM pg_stat_database WHERE datname = current_database()
		) AS database
	`
	if err := d.pool.QueryRow(ctx, cumulativeQuery).Scan(
		&sample.WAL.Records,
		&sample.WAL.FullPages,
		&sample.WAL.Bytes,
		&sample.WAL.BuffersFull,
		&sample.Checkpointer.Timed,
		&sample.Checkpointer.Requested,
		&sample.Checkpointer.Done,
		&sample.Checkpointer.WriteTimeMS,
		&sample.Checkpointer.SyncTimeMS,
		&sample.Checkpointer.BuffersWritten,
		&sample.BackgroundWriter.BuffersClean,
		&sample.BackgroundWriter.MaxWrittenClean,
		&sample.BackgroundWriter.BuffersAllocated,
		&sample.Database.BlocksRead,
		&sample.Database.BlocksHit,
		&sample.Database.BlockReadTimeMS,
		&sample.Database.BlockWriteTimeMS,
		&sample.Database.TempFiles,
		&sample.Database.TempBytes,
		&sample.Database.Deadlocks,
	); err != nil {
		return sample, err
	}

	const ioQuery = `
		SELECT
			backend_type,
			object,
			context,
			COALESCE(reads, 0),
			COALESCE(read_bytes, 0)::bigint,
			COALESCE(read_time, 0),
			COALESCE(writes, 0),
			COALESCE(write_bytes, 0)::bigint,
			COALESCE(write_time, 0),
			COALESCE(writebacks, 0),
			COALESCE(writeback_time, 0),
			COALESCE(extends, 0),
			COALESCE(extend_bytes, 0)::bigint,
			COALESCE(extend_time, 0),
			COALESCE(hits, 0),
			COALESCE(evictions, 0),
			COALESCE(reuses, 0),
			COALESCE(fsyncs, 0),
			COALESCE(fsync_time, 0)
		FROM pg_stat_io
		WHERE COALESCE(reads, 0) <> 0
			OR COALESCE(writes, 0) <> 0
			OR COALESCE(writebacks, 0) <> 0
			OR COALESCE(extends, 0) <> 0
			OR COALESCE(hits, 0) <> 0
			OR COALESCE(evictions, 0) <> 0
			OR COALESCE(reuses, 0) <> 0
			OR COALESCE(fsyncs, 0) <> 0
		ORDER BY backend_type, object, context
	`
	rows, err := d.pool.Query(ctx, ioQuery)
	if err != nil {
		return sample, err
	}
	for rows.Next() {
		var io metrics.PostgresIODiagnostics
		if err := rows.Scan(
			&io.BackendType,
			&io.Object,
			&io.Context,
			&io.Reads,
			&io.ReadBytes,
			&io.ReadTimeMS,
			&io.Writes,
			&io.WriteBytes,
			&io.WriteTimeMS,
			&io.Writebacks,
			&io.WritebackTimeMS,
			&io.Extends,
			&io.ExtendBytes,
			&io.ExtendTimeMS,
			&io.Hits,
			&io.Evictions,
			&io.Reuses,
			&io.Fsyncs,
			&io.FsyncTimeMS,
		); err != nil {
			rows.Close()
			return sample, err
		}
		sample.IO = append(sample.IO, io)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return sample, err
	}
	rows.Close()

	const activityQuery = `
		SELECT
			COALESCE(backend_type, ''),
			COALESCE(state, ''),
			COALESCE(wait_event_type, ''),
			COALESCE(wait_event, ''),
			count(*)::bigint
		FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()
		GROUP BY backend_type, state, wait_event_type, wait_event
		ORDER BY backend_type, state, wait_event_type, wait_event
	`
	rows, err = d.pool.Query(ctx, activityQuery)
	if err != nil {
		return sample, err
	}
	for rows.Next() {
		var activity metrics.PostgresActivityDiagnostics
		if err := rows.Scan(
			&activity.BackendType,
			&activity.State,
			&activity.WaitEventType,
			&activity.WaitEvent,
			&activity.Sessions,
		); err != nil {
			rows.Close()
			return sample, err
		}
		sample.Activity = append(sample.Activity, activity)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return sample, err
	}
	rows.Close()

	if d.segmentDiagnosticsQuery == "" {
		return sample, nil
	}
	rows, err = d.pool.Query(ctx, d.segmentDiagnosticsQuery)
	if err != nil {
		return sample, err
	}
	sample.IndexBytes = make(map[string]int64)
	for rows.Next() {
		var segment metrics.PostgresSegmentDiagnostics
		var indexBytes int64
		if err := rows.Scan(
			&segment.Index,
			&indexBytes,
			&segment.Kind,
			&segment.SourceState,
			&segment.Origin,
			&segment.Sequence,
			&segment.Documents,
			&segment.DeadDocs,
			&segment.Postings,
			&segment.Pages,
			&segment.RootBlock,
		); err != nil {
			rows.Close()
			return sample, err
		}
		sample.IndexBytes[segment.Index] = indexBytes
		sample.Segments = append(sample.Segments, segment)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return sample, err
	}
	rows.Close()
	return sample, nil
}

func parseWALLSN(lsn string) (uint64, error) {
	highText, lowText, ok := strings.Cut(lsn, "/")
	if !ok || highText == "" || lowText == "" || strings.Contains(lowText, "/") {
		return 0, fmt.Errorf("invalid PostgreSQL WAL LSN %q", lsn)
	}
	high, err := strconv.ParseUint(highText, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid PostgreSQL WAL LSN %q: %w", lsn, err)
	}
	low, err := strconv.ParseUint(lowText, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid PostgreSQL WAL LSN %q: %w", lsn, err)
	}
	return high<<32 | low, nil
}

// Exec executes SQL statements separated by semicolons.
func (d *Driver) Exec(ctx context.Context, statements string) error {
	stmts, err := pgsplit.SplitStatements(statements)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err := d.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Query executes a query and returns the hit count.
func (d *Driver) Query(ctx context.Context, query string, args ...any) (int, error) {
	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	return count, rows.Err()
}

// Insert bulk inserts rows using COPY.
func (d *Driver) Insert(ctx context.Context, table string, cols []string, rows [][]any) (int, error) {
	count, err := d.pool.CopyFrom(ctx,
		pgx.Identifier{table},
		cols,
		pgx.CopyFromRows(rows),
	)
	return int(count), err
}

// Update upserts rows using INSERT ... ON CONFLICT DO UPDATE.
// keyCols are the conflict target columns, cols is all columns (keys first, then values).
func (d *Driver) Update(ctx context.Context, table string, keyCols []string, cols []string, rows [][]any) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Build value columns (everything not in keyCols)
	keySet := make(map[string]bool, len(keyCols))
	for _, k := range keyCols {
		keySet[k] = true
	}
	var valCols []string
	for _, c := range cols {
		if !keySet[c] {
			valCols = append(valCols, c)
		}
	}

	// Build: INSERT INTO t (cols) VALUES ($1,$2,...), ($3,$4,...) ON CONFLICT (keyCols) DO UPDATE SET col=EXCLUDED.col, ...
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(") VALUES ")

	paramIdx := 1
	args := make([]any, 0, len(rows)*len(cols))
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for j := range cols {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
			args = append(args, row[j])
		}
		b.WriteByte(')')
	}

	b.WriteString(" ON CONFLICT (")
	b.WriteString(strings.Join(keyCols, ", "))
	b.WriteString(") DO UPDATE SET ")
	for i, col := range valCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
		b.WriteString(" = EXCLUDED.")
		b.WriteString(col)
	}

	tag, err := d.pool.Exec(ctx, b.String(), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// AppendSpaceToRandomDocument updates one tuple chosen from a bounded random
// heap page. The SQL is one statement so tuple selection and mutation share a
// snapshot and lock scope.
func (d *Driver) AppendSpaceToRandomDocument(ctx context.Context, target string) (int, error) {
	if target == "" {
		return 0, fmt.Errorf("random update target is empty")
	}
	tag, err := d.pool.Exec(ctx, appendSpaceToRandomDocumentSQL(target), target)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func appendSpaceToRandomDocumentSQL(target string) string {
	table := pgx.Identifier{target}.Sanitize()
	return fmt.Sprintf(`
WITH relation_size AS MATERIALIZED (
    SELECT GREATEST(
        pg_relation_size($1::regclass)
            / current_setting('block_size')::bigint,
        1
    ) AS blocks
),
bounds AS MATERIALIZED (
    SELECT floor(random() * blocks)::bigint AS block
    FROM relation_size
),
-- Keep each bound scalar so PostgreSQL builds init plans and a TID range scan.
-- A CROSS JOIN turns these predicates into join filters and scans the heap.
candidate AS MATERIALIZED (
    SELECT document.ctid
    FROM %s AS document
    WHERE document.ctid >= (
        SELECT format('(%%s,0)', bounds.block)::tid
        FROM bounds
    )
      AND document.ctid < (
        SELECT format('(%%s,0)', bounds.block + 1)::tid
        FROM bounds
    )
    LIMIT 1
)
UPDATE %s AS document
SET body = document.body || ' '
WHERE document.ctid = (SELECT ctid FROM candidate)`, table, table)
}

// CaptureConfig captures database configuration and registers it with metrics.
func (d *Driver) CaptureConfig(ctx context.Context, backendName string) {
	config := make(map[string]interface{})

	// Base PostgreSQL settings
	baseSettings := []string{
		"shared_buffers", "work_mem", "effective_cache_size",
		"random_page_cost", "max_connections", "max_parallel_workers",
		"max_parallel_workers_per_gather", "track_io_timing",
		"track_wal_io_timing", "autovacuum", "checkpoint_timeout",
		"checkpoint_completion_target", "max_wal_size", "min_wal_size",
		"wal_buffers", "synchronous_commit", "wal_sync_method",
		"full_page_writes", "log_checkpoints", "log_lock_waits",
		"log_temp_files", "deadlock_timeout", "jit",
	}

	// Combine base + extra GUCs
	allSettings := append(baseSettings, d.extraGUCs...)

	likePatterns := make([]string, len(d.extraGUCPrefixes))
	for i, prefix := range d.extraGUCPrefixes {
		likePatterns[i] = prefix + ".%"
	}

	pgSettings := make(map[string]string)
	extraByPrefix := make(map[string]map[string]string)

	// addEntry groups a name/value pair by dot-prefix (e.g. "paradedb.xxx"
	// lands in the "paradedb" section, unprefixed names in "postgresql").
	addEntry := func(name, value string) {
		if idx := strings.Index(name, "."); idx > 0 {
			prefix := name[:idx]
			if extraByPrefix[prefix] == nil {
				extraByPrefix[prefix] = make(map[string]string)
			}
			extraByPrefix[prefix][name] = value
		} else {
			pgSettings[name] = value
		}
	}

	rows, err := d.pool.Query(ctx, `
		SELECT name, setting, unit
		FROM pg_settings
		WHERE name = ANY($1) OR name LIKE ANY($2)
	`, allSettings, likePatterns)
	if err == nil {
		for rows.Next() {
			var name, setting string
			var unit *string
			if rows.Scan(&name, &setting, &unit) == nil {
				value := setting
				if unit != nil && *unit != "" {
					value = setting + *unit
				}
				addEntry(name, value)
			}
		}
		rows.Close()
	}

	// Run config queries (base + specialization-registered)
	allQueries := append([]ConfigQuery{
		{Key: "version", Query: "SELECT version()"},
	}, d.extraQueries...)
	for _, q := range allQueries {
		rows, err := d.pool.Query(ctx, q.Query)
		if err != nil {
			continue
		}
		twoColumns := len(rows.FieldDescriptions()) >= 2
		for rows.Next() {
			if twoColumns {
				var name, value string
				if rows.Scan(&name, &value) == nil {
					addEntry(name, value)
				}
			} else {
				var result string
				if rows.Scan(&result) == nil {
					config[q.Key] = result
				}
			}
		}
		rows.Close()
	}

	config["postgresql"] = pgSettings
	for prefix, settings := range extraByPrefix {
		config[prefix] = settings
	}

	metrics.RegisterBackendConfig(backendName, config)
}

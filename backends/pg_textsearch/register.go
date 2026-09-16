// Package pg_textsearch registers the pg_textsearch backend.
package pg_textsearch

import (
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/backends/shared/postgres"
)

func init() {
	backends.Register("pg_textsearch", backends.BackendConfig{
		Factory:     New,
		FileType:    "sql",
		EnvVar:      "PG_TEXTSEARCH_URL",
		DefaultConn: "postgres://postgres:postgres@localhost:5435/benchmark",
		Container:   "pg_textsearch",
	})
}

// New creates a PostgreSQL driver with pg_textsearch-specific configuration capture.
func New(connString string) (backends.Driver, error) {
	driver, err := postgres.New(connString)
	if err != nil {
		return nil, err
	}

	pgDriver := driver.(*postgres.Driver)
	pgDriver.SetIndexIOStatsAccessMethods("bm25")
	pgDriver.SetExtraGUCs([]string{
		"pg_textsearch.library_version",
		"pg_textsearch.default_limit",
		"pg_textsearch.bulk_load_threshold",
		"pg_textsearch.memtable_pages_threshold",
		"pg_textsearch.segments_per_level",
		"pg_textsearch.compress_segments",
		"pg_textsearch.memtable_cache_enabled",
		"pg_textsearch.memory_limit",
	})
	pgDriver.SetExtraQueries([]postgres.ConfigQuery{
		{Key: "pg_textsearch_version", Query: "SELECT extversion::text FROM pg_extension WHERE extname = 'pg_textsearch'"},
	})

	return driver, nil
}

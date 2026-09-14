// Package postgres registers the PostgreSQL backend.
package postgres

import (
	"github.com/paradedb/benchmarker/backends"
	pgshared "github.com/paradedb/benchmarker/backends/shared/postgres"
)

func init() {
	backends.Register("postgres", backends.BackendConfig{
		Factory:     NewFTS,
		FileType:    "sql",
		EnvVar:      "POSTGRES_URL",
		DefaultConn: "postgres://postgres:postgres@localhost:5433/benchmark",
		Container:   "postgres",
	})
}

// NewFTS creates the native PostgreSQL full-text-search driver and enables
// index I/O accounting for its GIN index.
func NewFTS(connString string) (backends.Driver, error) {
	driver, err := pgshared.New(connString)
	if err != nil {
		return nil, err
	}
	if postgresDriver, ok := driver.(*pgshared.Driver); ok {
		postgresDriver.SetIndexIOStatsAccessMethods("gin")
	}
	return driver, nil
}

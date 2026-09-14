package postgres

import (
	"testing"

	"github.com/paradedb/benchmarker/backends"
	pgshared "github.com/paradedb/benchmarker/backends/shared/postgres"
)

func TestPostgresFTSBackendRegistration(t *testing.T) {
	config, ok := backends.GetConfig("postgres")
	if !ok || config.EnvVar != "POSTGRES_URL" || config.Container != "postgres" ||
		config.DefaultConn != "postgres://postgres:postgres@localhost:5433/benchmark" {
		t.Fatalf("unexpected registration for postgres: %+v", config)
	}
	driver, err := config.Factory(config.DefaultConn)
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close()
	if !driver.(*pgshared.Driver).IndexIOStatsEnabled() {
		t.Fatal("postgres did not enable GIN index I/O stats")
	}
}

func TestPostgresFTSEnablesGINIndexIOStats(t *testing.T) {
	driver, err := NewFTS("postgres://postgres:postgres@localhost:5433/benchmark")
	if err != nil {
		t.Fatalf("NewFTS: %v", err)
	}
	postgresDriver, ok := driver.(*pgshared.Driver)
	if !ok {
		t.Fatalf("driver type = %T, want *postgres.Driver", driver)
	}
	defer postgresDriver.Close()
	if !postgresDriver.IndexIOStatsEnabled() {
		t.Fatal("postgres did not enable GIN index I/O stats")
	}
}

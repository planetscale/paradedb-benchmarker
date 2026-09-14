package search

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
)

type collectingPostgresDiagnosticsDriver struct {
	backends.Driver
	resets atomic.Int32
	reads  atomic.Int32
}

func (d *collectingPostgresDiagnosticsDriver) ResetPostgresDiagnostics(context.Context) error {
	d.resets.Add(1)
	return nil
}

func (d *collectingPostgresDiagnosticsDriver) ReadPostgresDiagnostics(context.Context) (backends.PostgresDiagnosticsSample, error) {
	read := d.reads.Add(1)
	return backends.PostgresDiagnosticsSample{
		WAL: metrics.PostgresWALDiagnostics{Bytes: int64(read) * 1024},
	}, nil
}

func TestPostgresDiagnosticsResetThrottleAndFinalSnapshot(t *testing.T) {
	alias := "postgres-diagnostics-" + t.Name()
	driver := &collectingPostgresDiagnosticsDriver{}
	b := &Backends{clients: map[string]*backends.K6Client{
		alias: backends.NewK6Client(nil, driver, alias),
	}}

	b.ResetPostgresDiagnostics([]string{alias})
	b.CollectPostgresDiagnostics(alias, false)
	b.CollectPostgresDiagnostics(alias, false)
	b.CollectPostgresDiagnostics(alias, true)

	if got := driver.resets.Load(); got != 1 {
		t.Fatalf("diagnostics resets = %d, want 1", got)
	}
	if got := driver.reads.Load(); got != 2 {
		t.Fatalf("diagnostics reads = %d, want 2", got)
	}
	samples := metrics.GetPostgresDiagnostics()[alias]
	if len(samples) != 2 || samples[0].Time == 0 || samples[1].WAL.Bytes != 2048 {
		t.Fatalf("diagnostics samples = %#v", samples)
	}
	metrics.ResetPostgresDiagnostics(alias)
}

package search

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/common"
)

type collectingWALDriver struct {
	backends.Driver
	mu        sync.Mutex
	positions []uint64
	errors    []error
	reads     int
}

func (d *collectingWALDriver) ReadWALPosition(context.Context) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	index := d.reads
	d.reads++
	if index < len(d.errors) && d.errors[index] != nil {
		return 0, d.errors[index]
	}
	if index >= len(d.positions) {
		return 0, errors.New("unexpected WAL position read")
	}
	return d.positions[index], nil
}

func (d *collectingWALDriver) readCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads
}

func TestWALStatsResetAndCollectionAreSelectedAndPhaseScoped(t *testing.T) {
	selected := "wal-selected-" + t.Name()
	unselected := "wal-unselected-" + t.Name()
	metrics.RegisterUpdateWorkload(selected)
	metrics.RegisterUpdateWorkload(unselected)
	selectedDriver := &collectingWALDriver{positions: []uint64{1000, 1256}}
	unselectedDriver := &collectingWALDriver{positions: []uint64{5000}}
	b := &Backends{clients: map[string]*backends.K6Client{
		selected:   backends.NewK6Client(nil, selectedDriver, selected),
		unselected: backends.NewK6Client(nil, unselectedDriver, unselected),
	}}

	b.ResetWALStats([]string{selected})
	b.CollectWALStats(selected)

	stats, ok := metrics.GetWALStats(selected)
	if !ok || stats.Bytes != 256 {
		t.Fatalf("collected WAL stats = %#v, %v; want 256 bytes", stats, ok)
	}
	if got := selectedDriver.readCount(); got != 2 {
		t.Fatalf("selected WAL reads = %d, want 2", got)
	}
	if got := unselectedDriver.readCount(); got != 0 {
		t.Fatalf("unselected WAL reads = %d, want 0", got)
	}
}

func TestWALStatsMethodsAreAvailableToJavaScript(t *testing.T) {
	alias := "wal-javascript-" + t.Name()
	metrics.RegisterUpdateWorkload(alias)
	driver := &collectingWALDriver{positions: []uint64{1000, 1064}}
	runtime := sobek.New()
	runtime.SetFieldNameMapper(common.FieldNameMapper{})
	b := &Backends{
		vu: runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, driver, alias),
		},
	}
	if err := runtime.Set("backends", b); err != nil {
		t.Fatalf("expose backends: %v", err)
	}
	if err := runtime.Set("alias", alias); err != nil {
		t.Fatalf("expose alias: %v", err)
	}
	script := `
backends.resetWALStats([alias]);
backends.collectWALStats(alias);
`
	if _, err := runtime.RunString(script); err != nil {
		t.Fatalf("JavaScript WAL methods failed: %v", err)
	}
	stats, ok := metrics.GetWALStats(alias)
	if !ok || stats.Bytes != 64 {
		t.Fatalf("JavaScript WAL stats = %#v, %v; want 64 bytes", stats, ok)
	}
}

func TestWALStatsSkipReadOnlyAndUnsupportedBackends(t *testing.T) {
	readOnly := "wal-read-only-" + t.Name()
	unsupported := "wal-unsupported-" + t.Name()
	readOnlyDriver := &collectingWALDriver{positions: []uint64{1000}}
	metrics.RegisterUpdateWorkload(unsupported)
	b := &Backends{clients: map[string]*backends.K6Client{
		readOnly:    backends.NewK6Client(nil, readOnlyDriver, readOnly),
		unsupported: backends.NewK6Client(nil, nil, unsupported),
	}}

	b.ResetWALStats([]string{readOnly, unsupported})

	if got := readOnlyDriver.readCount(); got != 0 {
		t.Fatalf("read-only backend performed %d WAL reads", got)
	}
	if _, ok := metrics.GetWALStats(readOnly); ok {
		t.Fatal("read-only backend exposed WAL stats")
	}
	if _, ok := metrics.GetWALStats(unsupported); ok {
		t.Fatal("unsupported backend exposed WAL stats")
	}
}

func TestWALCollectionFailurePreservesLastValidSnapshot(t *testing.T) {
	backend := "wal-error-" + t.Name()
	metrics.RegisterUpdateWorkload(backend)
	driver := &collectingWALDriver{
		positions: []uint64{1000, 1200, 0},
		errors:    []error{nil, nil, errors.New("read failed")},
	}
	b := &Backends{clients: map[string]*backends.K6Client{
		backend: backends.NewK6Client(nil, driver, backend),
	}}

	b.ResetWALStats([]string{backend})
	b.CollectWALStats(backend)
	b.CollectWALStats(backend)
	stats, ok := metrics.GetWALStats(backend)
	if !ok || stats.Bytes != 200 {
		t.Fatalf("WAL stats after failure = %#v, %v; want 200 bytes", stats, ok)
	}
}

func TestWALBaselineFailureLeavesMetricAbsent(t *testing.T) {
	backend := "wal-baseline-error-" + t.Name()
	metrics.RegisterUpdateWorkload(backend)
	driver := &collectingWALDriver{
		positions: []uint64{0},
		errors:    []error{errors.New("baseline failed")},
	}
	b := &Backends{clients: map[string]*backends.K6Client{
		backend: backends.NewK6Client(nil, driver, backend),
	}}

	b.ResetWALStats([]string{backend})

	if _, ok := metrics.GetWALStats(backend); ok {
		t.Fatal("failed baseline exposed WAL stats")
	}
	b.CollectWALStats(backend)
	if got := driver.readCount(); got != 1 {
		t.Fatalf("collection without baseline performed %d reads, want baseline read only", got)
	}
}

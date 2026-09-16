package search

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
	k6metrics "go.k6.io/k6/metrics"
)

type collectingIndexIODriver struct {
	backends.Driver
	identity string
	reads    atomic.Int32
	resets   atomic.Int32
	resetErr error
}

func (*collectingIndexIODriver) IndexIOStatsEnabled() bool { return true }
func (d *collectingIndexIODriver) IndexIOStatsIdentity() string {
	return d.identity
}
func (d *collectingIndexIODriver) ResetIndexIOStats(context.Context) error {
	d.resets.Add(1)
	return d.resetErr
}
func (d *collectingIndexIODriver) ReadIndexIOStats(context.Context) (backends.IndexIOStats, error) {
	read := int64(d.reads.Add(1)) * 8192
	return backends.IndexIOStats{
		ReadBytes: read,
		HitBytes:  read * 2,
		Read:      "8 kB",
		Hit:       "16 kB",
	}, nil
}

func TestCollectIndexIOStatsThrottlesAndForcesFinalSnapshot(t *testing.T) {
	driver := &collectingIndexIODriver{identity: "collector-test"}
	client := backends.NewK6Client(nil, driver, "collector-index-io-test")
	b := &Backends{
		clients:        map[string]*backends.K6Client{"collector-index-io-test": client},
		indexIOEnabled: true,
	}

	if got := b.collectIndexIOStats(false); len(got) != 1 {
		t.Fatalf("first collection = %v, want one backend", got)
	}
	if got := b.collectIndexIOStats(false); len(got) != 0 {
		t.Fatalf("unthrottled collection = %v, want no snapshot", got)
	}
	if got := b.collectIndexIOStats(true); len(got) != 1 {
		t.Fatalf("forced collection = %v, want one backend", got)
	}
	if got := driver.reads.Load(); got != 2 {
		t.Fatalf("stats reads = %d, want 2", got)
	}

	stats, ok := metrics.GetIndexIOStats("collector-index-io-test")
	if !ok || stats.ReadBytes != 16384 || stats.HitBytes != 32768 {
		t.Fatalf("latest stats = %#v, %v", stats, ok)
	}
}

func TestCollectIndexIOStatsSkipsStoppedBackends(t *testing.T) {
	stoppedDriver := &collectingIndexIODriver{identity: "stopped-" + t.Name()}
	activeDriver := &collectingIndexIODriver{identity: "active-" + t.Name()}
	b := &Backends{
		clients: map[string]*backends.K6Client{
			"postgres": backends.NewK6Client(nil, stoppedDriver, "postgres"),
			"custom":   backends.NewK6Client(nil, activeDriver, "custom"),
		},
		containers: map[string]string{
			"postgres": "postgres",
			"custom":   "custom",
		},
		stoppedContainers: map[string]struct{}{"postgres": {}},
		indexIOEnabled:    true,
	}

	result := b.collectIndexIOStats(true)
	if _, present := result["postgres"]; present {
		t.Fatalf("stopped backend was sampled: %v", result)
	}
	if _, present := result["custom"]; !present {
		t.Fatalf("active backend was not sampled: %v", result)
	}
	if got := stoppedDriver.reads.Load(); got != 0 {
		t.Fatalf("stopped backend stats reads = %d, want 0", got)
	}
	if got := activeDriver.reads.Load(); got != 1 {
		t.Fatalf("active backend stats reads = %d, want 1", got)
	}
}

func TestCollectFinalForcesIndexIOSnapshot(t *testing.T) {
	driver := &collectingIndexIODriver{identity: "collect-final-" + t.Name()}
	client := backends.NewK6Client(nil, driver, "collect-final-index-io-test")
	b := &Backends{
		clients:             map[string]*backends.K6Client{"collect-final-index-io-test": client},
		indexIOEnabled:      true,
		lastIndexIOSnapshot: time.Now(),
	}

	result := b.CollectFinal()
	if result == nil || result["index_io"] == nil {
		t.Fatalf("final collection = %v, want index I/O snapshot", result)
	}
	if got := driver.reads.Load(); got != 1 {
		t.Fatalf("stats reads = %d, want 1", got)
	}
}

type taggedVU struct {
	modules.VU
	state *lib.State
}

func (v taggedVU) State() *lib.State { return v.state }

func TestFinalIndexIOSnapshotTag(t *testing.T) {
	registry := k6metrics.NewRegistry()
	state := &lib.State{
		Tags: lib.NewVUStateTags(registry.RootTagSet().With(finalSnapshotTag, "true")),
	}
	b := &Backends{vu: taggedVU{state: state}}
	if !b.isFinalIndexIOSnapshot() {
		t.Fatal("final snapshot tag was not detected")
	}
}

type runtimeVU struct {
	modules.VU
	runtime *sobek.Runtime
}

func (v runtimeVU) Runtime() *sobek.Runtime { return v.runtime }

func TestAddDockerMetricsCollectorSchedulesFinalIndexIOSnapshot(t *testing.T) {
	runtime := sobek.New()
	driver := &collectingIndexIODriver{identity: "finalizer-" + t.Name()}
	client := backends.NewK6Client(nil, driver, "finalizer-backend")
	b := &Backends{
		vu:      runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{"finalizer-backend": client},
	}
	scenarios := runtime.NewObject()

	result := b.AddDockerMetricsCollector(sobek.FunctionCall{Arguments: []sobek.Value{
		scenarios,
		runtime.ToValue("10s"),
	}})
	if sobek.IsUndefined(result) {
		t.Fatal("collector function is undefined")
	}
	if got := driver.resets.Load(); got != 1 {
		t.Fatalf("stats resets = %d, want 1", got)
	}

	finalizer := scenarios.Get("index_io_finalizer").ToObject(runtime)
	if got := finalizer.Get("startTime").String(); got != "11s" {
		t.Fatalf("finalizer startTime = %q, want 11s", got)
	}
	if got := finalizer.Get("exec").String(); got != "collectMetrics" {
		t.Fatalf("finalizer exec = %q, want collectMetrics", got)
	}
	tags := finalizer.Get("tags").ToObject(runtime)
	if got := tags.Get(finalSnapshotTag).String(); got != "true" {
		t.Fatalf("final snapshot tag = %q, want true", got)
	}
}

func TestAddDockerMetricsCollectorValidatesDurationBeforeReset(t *testing.T) {
	runtime := sobek.New()
	driver := &collectingIndexIODriver{identity: "invalid-duration-" + t.Name()}
	client := backends.NewK6Client(nil, driver, "invalid-duration-backend")
	b := &Backends{
		vu:      runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{"invalid-duration-backend": client},
	}
	scenarios := runtime.NewObject()

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		b.AddDockerMetricsCollector(sobek.FunctionCall{Arguments: []sobek.Value{
			scenarios,
			runtime.ToValue("not-a-duration"),
		}})
	}()

	if !panicked {
		t.Fatal("invalid duration did not fail")
	}
	if got := driver.resets.Load(); got != 0 {
		t.Fatalf("stats resets = %d, want 0 after validation failure", got)
	}
}

func TestResetIndexIOStatsTouchesOnlySelectedAliases(t *testing.T) {
	runtime := sobek.New()
	selectedDriver := &collectingIndexIODriver{identity: "selected-" + t.Name()}
	unselectedDriver := &collectingIndexIODriver{identity: "unselected-" + t.Name()}
	b := &Backends{
		vu: runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			"selected":   backends.NewK6Client(nil, selectedDriver, "selected"),
			"unselected": backends.NewK6Client(nil, unselectedDriver, "unselected"),
		},
	}

	b.ResetIndexIOStats([]string{"selected"})

	if got := selectedDriver.resets.Load(); got != 1 {
		t.Fatalf("selected resets = %d, want 1", got)
	}
	if got := unselectedDriver.resets.Load(); got != 0 {
		t.Fatalf("unselected resets = %d, want 0", got)
	}
}

func TestResetIndexIOStatsAcceptsJavaScriptAliasArray(t *testing.T) {
	runtime := sobek.New()
	runtime.SetFieldNameMapper(common.FieldNameMapper{})
	selectedDriver := &collectingIndexIODriver{identity: "javascript-selected-" + t.Name()}
	unselectedDriver := &collectingIndexIODriver{identity: "javascript-unselected-" + t.Name()}
	b := &Backends{
		vu: runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			"selected":   backends.NewK6Client(nil, selectedDriver, "selected"),
			"unselected": backends.NewK6Client(nil, unselectedDriver, "unselected"),
		},
	}
	if err := runtime.Set("backends", b); err != nil {
		t.Fatalf("expose backends to JavaScript: %v", err)
	}

	if _, err := runtime.RunString(`backends.resetIndexIOStats(["selected"]);`); err != nil {
		t.Fatalf("JavaScript reset failed: %v", err)
	}
	if got := selectedDriver.resets.Load(); got != 1 {
		t.Fatalf("selected resets = %d, want 1", got)
	}
	if got := unselectedDriver.resets.Load(); got != 0 {
		t.Fatalf("unselected resets = %d, want 0", got)
	}
}

func TestResetIndexIOStatsValidatesAllAliasesBeforeReset(t *testing.T) {
	runtime := sobek.New()
	driver := &collectingIndexIODriver{identity: "validate-first-" + t.Name()}
	b := &Backends{
		vu: runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			"known": backends.NewK6Client(nil, driver, "known"),
		},
	}

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		b.ResetIndexIOStats([]string{"known", "missing"})
	}()

	if !panicked {
		t.Fatal("unknown alias did not throw")
	}
	if got := driver.resets.Load(); got != 0 {
		t.Fatalf("known backend reset %d times before validation completed", got)
	}
}

func TestResetIndexIOStatsThrowsProviderError(t *testing.T) {
	runtime := sobek.New()
	driver := &collectingIndexIODriver{
		identity: "reset-error-" + t.Name(),
		resetErr: errors.New("reset failed"),
	}
	b := &Backends{
		vu: runtimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			"broken": backends.NewK6Client(nil, driver, "broken"),
		},
	}

	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		b.ResetIndexIOStats([]string{"broken"})
	}()

	if !panicked {
		t.Fatal("provider reset error did not throw")
	}
}

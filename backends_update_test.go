package search

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/modules"
)

type randomUpdateDriver struct {
	backends.Driver
	calls atomic.Int32
	err   error
}

func (d *randomUpdateDriver) AppendSpaceToRandomDocument(context.Context, string) (int, error) {
	d.calls.Add(1)
	if d.err != nil {
		return 0, d.err
	}
	return 1, nil
}

type updaterRuntimeVU struct {
	modules.VU
	runtime *sobek.Runtime
}

func (v updaterRuntimeVU) Runtime() *sobek.Runtime { return v.runtime }

func TestAddRandomDocumentUpdaterAtZeroAddsNoScenarioOrState(t *testing.T) {
	runtime := sobek.New()
	alias := "disabled-updater-" + t.Name()
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, &randomUpdateDriver{}, alias),
		},
	}
	scenarios := runtime.NewObject()

	result := b.AddRandomDocumentUpdater(sobek.FunctionCall{Arguments: []sobek.Value{
		scenarios,
		runtime.ToValue("paradedb_query"),
		runtime.ToValue(alias),
		runtime.ToValue(0),
		runtime.ToValue("documents"),
		runtime.ToValue("updateParadedbDocuments"),
	}})
	if sobek.IsUndefined(result) {
		t.Fatal("disabled updater should still return an exportable no-op")
	}
	if scenario := scenarios.Get("paradedb_query_updates"); scenario != nil && !sobek.IsUndefined(scenario) {
		t.Fatalf("zero rate added updater scenario: %v", scenario)
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(alias); enabled {
		t.Fatal("zero rate registered update workload state")
	}
}

func TestAddRandomDocumentUpdaterSchedulesOneDedicatedVU(t *testing.T) {
	runtime := sobek.New()
	alias := "enabled-updater-" + t.Name()
	otherAlias := "other-updater-" + t.Name()
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias:      backends.NewK6Client(nil, &randomUpdateDriver{}, alias),
			otherAlias: backends.NewK6Client(nil, &randomUpdateDriver{}, otherAlias),
		},
	}
	scenarios := runtime.NewObject()
	if err := scenarios.Set("paradedb_query", map[string]interface{}{
		"executor":  "constant-vus",
		"vus":       8,
		"duration":  "10s",
		"startTime": "5s",
		"exec":      "paradedbQuery",
	}); err != nil {
		t.Fatalf("set source scenario: %v", err)
	}

	b.AddRandomDocumentUpdater(sobek.FunctionCall{Arguments: []sobek.Value{
		scenarios,
		runtime.ToValue("paradedb_query"),
		runtime.ToValue(alias),
		runtime.ToValue(3),
		runtime.ToValue("documents"),
		runtime.ToValue("updateParadedbDocuments"),
	}})
	scenario := scenarios.Get("paradedb_query_updates").ToObject(runtime)
	if got := scenario.Get("executor").String(); got != "constant-arrival-rate" {
		t.Fatalf("executor = %q, want constant-arrival-rate", got)
	}
	if got := scenario.Get("rate").ToInteger(); got != 3 {
		t.Fatalf("rate = %d, want 3", got)
	}
	if got := scenario.Get("timeUnit").String(); got != "1s" {
		t.Fatalf("timeUnit = %q, want 1s", got)
	}
	if got := scenario.Get("preAllocatedVUs").ToInteger(); got != 1 {
		t.Fatalf("preAllocatedVUs = %d, want 1", got)
	}
	if got := scenario.Get("maxVUs").ToInteger(); got != 1 {
		t.Fatalf("maxVUs = %d, want 1", got)
	}
	if got := scenario.Get("gracefulStop").String(); got != "0s" {
		t.Fatalf("gracefulStop = %q, want 0s", got)
	}
	if got := scenario.Get("duration").String(); got != "10s" {
		t.Fatalf("duration = %q, want 10s", got)
	}
	if got := scenario.Get("startTime").String(); got != "5s" {
		t.Fatalf("startTime = %q, want source startTime 5s", got)
	}
	if got := scenario.Get("exec").String(); got != "updateParadedbDocuments" {
		t.Fatalf("exec = %q, want updateParadedbDocuments", got)
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(alias); !enabled {
		t.Fatal("enabled updater did not register workload state")
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(otherAlias); enabled {
		t.Fatal("backend-specific updater registered an unrelated backend")
	}
}

func TestRandomDocumentWorkerDoesNotCountFailedUpdate(t *testing.T) {
	alias := "failed-updater-" + t.Name()
	driver := &randomUpdateDriver{err: errors.New("write failed")}
	client := backends.NewK6Client(nil, driver, alias)
	workload := metrics.RegisterUpdateWorkload(alias)
	client.EnableRandomDocumentUpdates(workload)
	b := &Backends{clients: map[string]*backends.K6Client{alias: client}}

	b.updateRandomDocument(alias, "documents")
	stats := workload.Stats()
	if stats.CompletedUpdates != 0 {
		t.Fatalf("failed update changed completion count: %#v", stats)
	}
	if got := driver.calls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
}

func TestRandomDocumentUpdaterReturnsOnePhaseControlledUpdate(t *testing.T) {
	runtime := sobek.New()
	alias := "phase-updater-" + t.Name()
	driver := &randomUpdateDriver{}
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, driver, alias),
		},
	}

	value := b.RandomDocumentUpdater(sobek.FunctionCall{Arguments: []sobek.Value{
		runtime.ToValue(alias),
		runtime.ToValue("documents"),
	}})
	update, ok := sobek.AssertFunction(value)
	if !ok {
		t.Fatal("phase updater did not return a function")
	}
	result, err := update(sobek.Undefined())
	if err != nil {
		t.Fatalf("phase update: %v", err)
	}
	if got := result.ToObject(runtime).Get("updated").ToInteger(); got != 1 {
		t.Fatalf("updated = %d, want 1", got)
	}
	if got := driver.calls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
	stats, enabled := metrics.GetUpdateWorkloadStats(alias)
	if !enabled || stats.CompletedUpdates != 1 {
		t.Fatalf("update workload = %#v, enabled %v", stats, enabled)
	}
}

func TestRandomDocumentPrewarmerDoesNotRegisterMeasuredUpdate(t *testing.T) {
	runtime := sobek.New()
	alias := "phase-update-prewarm-" + t.Name()
	driver := &randomUpdateDriver{}
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, driver, alias),
		},
	}

	value := b.RandomDocumentPrewarmer(sobek.FunctionCall{Arguments: []sobek.Value{
		runtime.ToValue(alias),
		runtime.ToValue("documents"),
	}})
	prewarm, ok := sobek.AssertFunction(value)
	if !ok {
		t.Fatal("phase update prewarmer did not return a function")
	}
	result, err := prewarm(sobek.Undefined())
	if err != nil {
		t.Fatalf("phase update prewarm: %v", err)
	}
	if got := result.ToObject(runtime).Get("updated").ToInteger(); got != 1 {
		t.Fatalf("updated = %d, want 1", got)
	}
	if got := driver.calls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(alias); enabled {
		t.Fatal("update prewarm registered measured workload state")
	}
}

func TestRandomDocumentPrewarmerReturnsUpdateError(t *testing.T) {
	runtime := sobek.New()
	alias := "failed-phase-update-prewarm-" + t.Name()
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, &randomUpdateDriver{err: errors.New("write failed")}, alias),
		},
	}

	value := b.RandomDocumentPrewarmer(sobek.FunctionCall{Arguments: []sobek.Value{
		runtime.ToValue(alias),
		runtime.ToValue("documents"),
	}})
	prewarm, ok := sobek.AssertFunction(value)
	if !ok {
		t.Fatal("phase update prewarmer did not return a function")
	}
	if _, err := prewarm(sobek.Undefined()); err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("prewarm error = %v, want write failed", err)
	}
}

func TestAddRandomDocumentUpdaterSkipsFilteredBackend(t *testing.T) {
	runtime := sobek.New()
	alias := "filtered-updater-" + t.Name()
	b := &Backends{
		vu: updaterRuntimeVU{runtime: runtime},
		clients: map[string]*backends.K6Client{
			alias: backends.NewK6Client(nil, &randomUpdateDriver{}, alias),
		},
	}
	scenarios := runtime.NewObject()

	b.AddRandomDocumentUpdater(sobek.FunctionCall{Arguments: []sobek.Value{
		scenarios,
		runtime.ToValue("missing_query_scenario"),
		runtime.ToValue(alias),
		runtime.ToValue(3),
		runtime.ToValue("documents"),
		runtime.ToValue("updateFilteredDocuments"),
	}})
	if scenario := scenarios.Get("missing_query_scenario_updates"); scenario != nil && !sobek.IsUndefined(scenario) {
		t.Fatalf("filtered backend added updater scenario: %v", scenario)
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(alias); enabled {
		t.Fatal("filtered backend registered update workload state")
	}
}

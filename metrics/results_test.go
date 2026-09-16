package metrics

import (
	"context"
	"testing"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/lib"
	k6metrics "go.k6.io/k6/metrics"
)

type fakeVU struct {
	ctx   context.Context
	state *lib.State
}

func (f fakeVU) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (fakeVU) Events() common.Events { return common.Events{} }

func (fakeVU) InitEnv() *common.InitEnvironment { return nil }

func (f fakeVU) State() *lib.State { return f.state }

func (fakeVU) Runtime() *sobek.Runtime { return nil }

func (fakeVU) RegisterCallback() func(func() error) {
	return func(func() error) {}
}

func TestCaptureQueryPatternUsesExplicitBackend(t *testing.T) {
	oldPatterns := QueryPatterns
	QueryPatterns = make(map[string]string)
	defer func() {
		QueryPatterns = oldPatterns
	}()

	registry := k6metrics.NewRegistry()
	state := &lib.State{
		Tags: lib.NewVUStateTags(
			registry.RootTagSet().
				With("scenario", "search_scenario").
				With("chart", "primary"),
		),
	}

	CaptureQueryPattern(fakeVU{state: state}, "paradedb", "SELECT 1")

	got := GetQueryPattern("paradedb", "primary", "search_scenario")
	if got != "SELECT 1" {
		t.Fatalf("expected captured query for explicit backend, got %q", got)
	}
}

func TestUpdateResultEmitsDurationForEveryAttemptAndCountsOutcome(t *testing.T) {
	registry := k6metrics.NewRegistry()
	var err error
	oldDuration, oldDocs, oldErrors := updateDuration, updateDocs, updateErrors
	updateDuration, err = registry.NewMetric("update_duration_test", k6metrics.Trend, k6metrics.Time)
	if err != nil {
		t.Fatalf("create update duration metric: %v", err)
	}
	updateDocs, err = registry.NewMetric("update_docs_test", k6metrics.Counter)
	if err != nil {
		t.Fatalf("create update docs metric: %v", err)
	}
	updateErrors, err = registry.NewMetric("update_errors_test", k6metrics.Counter)
	if err != nil {
		t.Fatalf("create update errors metric: %v", err)
	}
	t.Cleanup(func() {
		updateDuration, updateDocs, updateErrors = oldDuration, oldDocs, oldErrors
	})

	samples := make(chan k6metrics.SampleContainer, 4)
	state := &lib.State{
		Samples: samples,
		Tags:    lib.NewVUStateTags(registry.RootTagSet()),
	}
	vu := fakeVU{state: state}
	(&UpdateResult{Rows: 1, LatencyMs: 2.5}).Emit(context.Background(), vu, "custom")
	(&UpdateResult{LatencyMs: 15, Error: "timeout"}).Emit(context.Background(), vu, "custom")

	counts := make(map[string]int)
	for range 4 {
		for _, sample := range (<-samples).GetSamples() {
			counts[sample.Metric.Name]++
		}
	}
	if counts["update_duration_test"] != 2 || counts["update_docs_test"] != 1 || counts["update_errors_test"] != 1 {
		t.Fatalf("emitted update metrics = %v", counts)
	}
}

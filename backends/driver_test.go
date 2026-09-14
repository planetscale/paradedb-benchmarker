package backends

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func TestConvertValuePreservesEmptyText(t *testing.T) {
	got, err := convertValue("", "text")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty text value to stay empty, got %#v", got)
	}
}

func TestConvertValueKeepsEmptyIntegerAsNil(t *testing.T) {
	got, err := convertValue("", "integer")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected empty integer value to become nil, got %#v", got)
	}
}

func TestConvertValueRejectsInvalidInteger(t *testing.T) {
	_, err := convertValue("abc", "integer")
	if err == nil {
		t.Fatal("expected error for invalid integer")
	}
}

func TestConvertValueRejectsInvalidJSONArray(t *testing.T) {
	_, err := convertValue("not-json", "text[]")
	if err == nil {
		t.Fatal("expected error for invalid JSON array")
	}
}

func TestConvertValueRejectsInvalidBoolean(t *testing.T) {
	_, err := convertValue("maybe", "boolean")
	if err == nil {
		t.Fatal("expected error for invalid boolean")
	}
}

type recordingDriver struct {
	inserted  int
	queryHits int
	queryErr  error
	queryCtx  context.Context
	queryText string
	queryArgs []any
}

type blockingQueryDriver struct {
	recordingDriver
}

func (d *blockingQueryDriver) Query(ctx context.Context, query string, args ...any) (int, error) {
	d.queryCtx = ctx
	<-ctx.Done()
	return 0, ctx.Err()
}

type randomUpdateRecordingDriver struct {
	recordingDriver
	updates int
	err     error
}

func (d *randomUpdateRecordingDriver) AppendSpaceToRandomDocument(context.Context, string) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	d.updates++
	return 1, nil
}

type indexIORecordingDriver struct {
	recordingDriver
	enabled    bool
	identity   string
	resetCalls *atomic.Int32
	resetErr   error
	stats      IndexIOStats
}

type walRecordingDriver struct {
	recordingDriver
	position uint64
	err      error
}

func (d *walRecordingDriver) ReadWALPosition(context.Context) (uint64, error) {
	return d.position, d.err
}

func (d *indexIORecordingDriver) IndexIOStatsEnabled() bool { return d.enabled }
func (d *indexIORecordingDriver) IndexIOStatsIdentity() string {
	return d.identity
}
func (d *indexIORecordingDriver) ResetIndexIOStats(context.Context) error {
	d.resetCalls.Add(1)
	return d.resetErr
}
func (d *indexIORecordingDriver) ReadIndexIOStats(context.Context) (IndexIOStats, error) {
	return d.stats, nil
}

func TestK6ClientResetsIndexIOStatsOncePerDatasource(t *testing.T) {
	var resetCalls atomic.Int32
	identity := "reset-once-" + t.Name()
	first := NewK6Client(nil, &indexIORecordingDriver{
		enabled: true, identity: identity, resetCalls: &resetCalls,
	}, "reset-once-first")
	second := NewK6Client(nil, &indexIORecordingDriver{
		enabled: true, identity: identity, resetCalls: &resetCalls,
	}, "reset-once-second")

	if supported, err := first.ResetIndexIOStats(context.Background()); err != nil || !supported {
		t.Fatalf("first reset = supported %v, err %v", supported, err)
	}
	if supported, err := second.ResetIndexIOStats(context.Background()); err != nil || !supported {
		t.Fatalf("second reset = supported %v, err %v", supported, err)
	}
	if got := resetCalls.Load(); got != 1 {
		t.Fatalf("reset calls = %d, want 1", got)
	}
}

func TestK6ClientLateInitializationDoesNotClearSnapshot(t *testing.T) {
	var resetCalls atomic.Int32
	identity := "late-init-" + t.Name()
	const backend = "late-init-backend"
	client := NewK6Client(nil, &indexIORecordingDriver{
		enabled: true, identity: identity, resetCalls: &resetCalls,
	}, backend)
	if _, err := client.ResetIndexIOStats(context.Background()); err != nil {
		t.Fatalf("initial reset: %v", err)
	}

	want := metrics.IndexIOStats{ReadBytes: 8192, HitBytes: 16384, Read: "8192 bytes", Hit: "16 kB"}
	metrics.RegisterIndexIOStats(backend, want)
	lateClient := NewK6Client(nil, &indexIORecordingDriver{
		enabled: true, identity: identity, resetCalls: &resetCalls,
	}, backend)
	if _, err := lateClient.ResetIndexIOStats(context.Background()); err != nil {
		t.Fatalf("late reset: %v", err)
	}

	got, ok := metrics.GetIndexIOStats(backend)
	if !ok || got != want {
		t.Fatalf("snapshot after late initialization = %#v, %v; want %#v, true", got, ok, want)
	}
	if got := resetCalls.Load(); got != 1 {
		t.Fatalf("reset calls = %d, want 1", got)
	}
}

func TestK6ClientSkipsIndexIOStatsForUnsupportedDriver(t *testing.T) {
	client := NewK6Client(nil, &recordingDriver{}, "unsupported-index-io")
	if client.IndexIOStatsEnabled() {
		t.Fatal("plain driver unexpectedly enabled index I/O stats")
	}
	if supported, err := client.ResetIndexIOStats(context.Background()); err != nil || supported {
		t.Fatalf("unsupported reset = supported %v, err %v", supported, err)
	}
	if _, supported, err := client.ReadIndexIOStats(context.Background()); err != nil || supported {
		t.Fatalf("unsupported read = supported %v, err %v", supported, err)
	}
}

func TestK6ClientReadsOptionalWALPosition(t *testing.T) {
	client := NewK6Client(nil, &walRecordingDriver{position: 1234}, "wal")
	position, supported, err := client.ReadWALPosition(context.Background())
	if err != nil || !supported || position != 1234 {
		t.Fatalf("WAL position = %d, supported %v, err %v", position, supported, err)
	}

	unsupported := NewK6Client(nil, &recordingDriver{}, "unsupported-wal")
	position, supported, err = unsupported.ReadWALPosition(context.Background())
	if err != nil || supported || position != 0 {
		t.Fatalf("unsupported WAL position = %d, supported %v, err %v", position, supported, err)
	}
}

func TestK6ClientResetIndexIOStatsNowBypassesOnceGuard(t *testing.T) {
	var resetCalls atomic.Int32
	client := NewK6Client(nil, &indexIORecordingDriver{
		enabled: true, identity: "immediate-reset-" + t.Name(), resetCalls: &resetCalls,
	}, "immediate-reset")

	if supported, err := client.ResetIndexIOStats(context.Background()); err != nil || !supported {
		t.Fatalf("guarded reset = supported %v, err %v", supported, err)
	}
	if supported, err := client.ResetIndexIOStatsNow(context.Background()); err != nil || !supported {
		t.Fatalf("immediate reset = supported %v, err %v", supported, err)
	}
	if got := resetCalls.Load(); got != 2 {
		t.Fatalf("reset calls = %d, want 2", got)
	}
}

type prewarmVU struct {
	modules.VU
	ctx context.Context
}

func (v prewarmVU) Context() context.Context { return v.ctx }
func (prewarmVU) InitEnv() *common.InitEnvironment {
	return nil
}

func TestK6ClientPrewarmUsesVUContextWithoutInitializingMetrics(t *testing.T) {
	type contextKey string
	const key contextKey = "prewarm"
	driver := &recordingDriver{queryHits: 7}
	client := NewK6Client(prewarmVU{
		ctx: context.WithValue(context.Background(), key, "setup-vu"),
	}, driver, "prewarm")

	result := client.Prewarm("SELECT $1", "query")

	if got := result["hits"]; got != int64(7) {
		t.Fatalf("prewarm hits = %#v, want 7", got)
	}
	if _, ok := result["error"]; ok {
		t.Fatalf("successful prewarm returned error: %#v", result)
	}
	if driver.queryCtx.Value(key) != "setup-vu" {
		t.Fatal("prewarm query did not inherit the setup VU context")
	}
	if driver.queryText != "SELECT $1" || len(driver.queryArgs) != 1 || driver.queryArgs[0] != "query" {
		t.Fatalf("driver query = %q, %#v", driver.queryText, driver.queryArgs)
	}
	if client.initialized {
		t.Fatal("prewarm initialized measured backend metrics")
	}
}

func TestK6ClientPrewarmReturnsQueryError(t *testing.T) {
	driver := &recordingDriver{queryErr: errors.New("query failed")}
	client := NewK6Client(nil, driver, "prewarm-error")

	result := client.Prewarm("SELECT broken")

	if got := result["hits"]; got != 0 {
		t.Fatalf("prewarm hits = %#v, want 0", got)
	}
	if got := result["error"]; got != "query failed" {
		t.Fatalf("prewarm error = %#v, want query failed", got)
	}
}

func TestK6ClientCancelsQueryAtMeasurementDeadline(t *testing.T) {
	driver := &blockingQueryDriver{}
	client := NewK6Client(nil, driver, "deadline")
	deadline := time.Now().Add(20 * time.Millisecond)
	client.SetMeasurementDeadlineProvider(func() (time.Time, bool) {
		return deadline, true
	})

	result := client.Query("SELECT pg_sleep(10)")

	if got := result["error"]; got != "benchmark measurement deadline reached" {
		t.Fatalf("query error = %#v, want measurement deadline", got)
	}
	if got, ok := driver.queryCtx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("driver deadline = %v, %v; want %v, true", got, ok, deadline)
	}
}

func TestK6ClientCancelsPhasePrewarmQueryWithoutInitializingMetrics(t *testing.T) {
	driver := &blockingQueryDriver{}
	client := NewK6Client(nil, driver, "prewarm-deadline")
	deadline := time.Now().Add(20 * time.Millisecond)
	client.SetPhaseQueryStateProvider(func() (time.Time, bool, bool) {
		return deadline, false, true
	})

	result := client.Query("SELECT pg_sleep(10)")

	if got := result["deadlineReached"]; got != true {
		t.Fatalf("prewarm deadline marker = %#v, want true", got)
	}
	if _, ok := result["error"]; ok {
		t.Fatalf("prewarm boundary cancellation returned an error: %#v", result)
	}
	if client.initialized {
		t.Fatal("phase prewarm query initialized measured metrics")
	}
	if got, ok := driver.queryCtx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatalf("driver deadline = %v, %v; want %v, true", got, ok, deadline)
	}
}

func TestK6ClientPhaseMeasurementContinuesThroughMeasuredQueryPath(t *testing.T) {
	driver := &recordingDriver{queryHits: 3}
	client := NewK6Client(nil, driver, "measuring")
	deadline := time.Now().Add(time.Second)
	client.SetPhaseQueryStateProvider(func() (time.Time, bool, bool) {
		return deadline, true, true
	})
	client.SetMeasurementDeadlineProvider(func() (time.Time, bool) {
		return deadline, true
	})

	result := client.Query("SELECT 1")

	if got := result["hits"]; got != int64(3) {
		t.Fatalf("measured hits = %#v, want 3", got)
	}
	if !client.initialized {
		t.Fatal("measured phase query did not initialize metrics")
	}
}

func (d *recordingDriver) Close() error                       { return nil }
func (d *recordingDriver) Exec(context.Context, string) error { return nil }
func (d *recordingDriver) Query(ctx context.Context, query string, args ...any) (int, error) {
	d.queryCtx = ctx
	d.queryText = query
	d.queryArgs = args
	return d.queryHits, d.queryErr
}
func (d *recordingDriver) CaptureConfig(context.Context, string) {}
func (d *recordingDriver) Insert(_ context.Context, _ string, _ []string, rows [][]any) (int, error) {
	d.inserted += len(rows)
	return len(rows), nil
}
func (d *recordingDriver) Update(_ context.Context, _ string, _ []string, _ []string, rows [][]any) (int, error) {
	return len(rows), nil
}

func TestK6ClientQueriesDoNotDriveFixedRateUpdateWorkload(t *testing.T) {
	backend := "fixed-rate-query-independence-" + t.Name()
	workload := metrics.RegisterUpdateWorkload(backend)
	driver := &recordingDriver{}
	client := NewK6Client(nil, driver, backend)
	client.EnableRandomDocumentUpdates(workload)

	client.Query("SELECT 1")
	driver.queryErr = errors.New("query failed")
	client.Query("SELECT broken")

	if got := workload.Stats().CompletedUpdates; got != 0 {
		t.Fatalf("queries changed completed update count to %d", got)
	}
}

func TestK6ClientRecordsOnlyConfirmedRandomUpdate(t *testing.T) {
	backend := "random-update-" + t.Name()
	workload := metrics.RegisterUpdateWorkload(backend)
	driver := &randomUpdateRecordingDriver{}
	client := NewK6Client(nil, driver, backend)
	client.EnableRandomDocumentUpdates(workload)
	if count, err := client.PrewarmAppendSpaceToRandomDocument("documents"); err != nil || count != 1 {
		t.Fatalf("prewarm random update = %d, %v; want 1, nil", count, err)
	}
	if got := workload.Stats().CompletedUpdates; got != 0 {
		t.Fatalf("prewarm changed completed updates to %d", got)
	}
	if count, err := client.AppendSpaceToRandomDocument("documents"); err != nil || count != 1 {
		t.Fatalf("random update = %d, %v; want 1, nil", count, err)
	}
	if got := workload.Stats().CompletedUpdates; got != 1 {
		t.Fatalf("completed updates = %d, want 1", got)
	}

	driver.err = errors.New("update failed")
	if _, err := client.AppendSpaceToRandomDocument("documents"); err == nil {
		t.Fatal("expected update error")
	}
	if got := workload.Stats().CompletedUpdates; got != 1 {
		t.Fatalf("failed update changed completed count to %d", got)
	}
}

func TestSchemaColumnsInOrderRejectsMissingColumns(t *testing.T) {
	_, err := schemaColumnsInOrder(&Schema{
		Columns: map[string]string{"id": "text", "title": "text", "content": "text"},
	}, []string{"id", "title"})
	if err == nil {
		t.Fatal("expected error for missing schema column")
	}
	if !strings.Contains(err.Error(), "content") {
		t.Fatalf("expected missing column name in error, got %v", err)
	}
}

func TestCLILoaderLoadFailsOnInvalidTypedValue(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("numbers\nnot-json\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	driver := &recordingDriver{}
	loader := NewCLILoader("test", "sql", "stub://default", func(string) (Driver, error) {
		return driver, nil
	})

	_, err := loader.Load(context.Background(), &Schema{
		Table:   "documents",
		Columns: map[string]string{"numbers": "integer[]"},
	}, csvPath, 100, 1)
	if err == nil {
		t.Fatal("expected error for invalid typed value")
	}
	if !strings.Contains(err.Error(), `row 2 column "numbers"`) {
		t.Fatalf("expected row/column context in error, got %v", err)
	}
	if driver.inserted != 0 {
		t.Fatalf("expected no rows inserted, got %d", driver.inserted)
	}
}

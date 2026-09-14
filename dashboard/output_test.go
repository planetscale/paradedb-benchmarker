package dashboard

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/metrics"
	k6metrics "go.k6.io/k6/metrics"
)

func queryMetricsFixture(name string, latencies []float64, startTime, endTime int64) *QueryMetrics {
	query := newQueryMetrics(name)
	for i, latency := range latencies {
		sampleTime := startTime
		if len(latencies) > 1 {
			sampleTime += int64(i) * (endTime - startTime) / int64(len(latencies)-1)
		}
		query.recordLatency(latency, sampleTime)
	}
	return query
}

func TestUpdateIngestRateSkipsInitialPoint(t *testing.T) {
	o := &Output{}
	rm := &RunMetrics{TotalIngested: 100}

	o.updateIngestRate(rm, 1000)

	if len(rm.IngestRate) != 0 {
		t.Fatalf("expected no ingest-rate points on first sample, got %d", len(rm.IngestRate))
	}
	if rm.LastIngestTime != 1000 {
		t.Fatalf("expected first ingest timestamp to be recorded, got %d", rm.LastIngestTime)
	}
	if rm.LastIngestDocs != 100 {
		t.Fatalf("expected first ingest doc count to be recorded, got %d", rm.LastIngestDocs)
	}
}

func TestGetSummaryUsesFirstIngestTimeForAverageRate(t *testing.T) {
	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"ingest": {
					Name:            "ingest",
					Backend:         "paradedb",
					Queries:         map[string]*QueryMetrics{},
					TotalIngested:   300,
					FirstIngestTime: 1000,
					EndTime:         3000,
				},
			},
		},
	}

	summary := o.getSummary()
	runs := summary["runs"].(map[string]interface{})
	run := runs["ingest"].(map[string]interface{})

	got := run["avgIngestRate"].(float64)
	if math.Abs(got-150) > 0.001 {
		t.Fatalf("expected ingest rate of 150 docs/sec, got %.3f", got)
	}
}

func TestGetSummaryUsesQueryWindowForQPS(t *testing.T) {
	latencies := make([]float64, 10)
	for i := range latencies {
		latencies[i] = 100
	}

	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"search": {
					Name:      "search",
					Backend:   "paradedb",
					StartTime: 1000,
					EndTime:   21000,
					Queries: map[string]*QueryMetrics{
						"search_query": queryMetricsFixture("search_query", latencies, 1000, 6000),
					},
				},
			},
		},
	}

	summary := o.getSummary()
	runs := summary["runs"].(map[string]interface{})
	run := runs["search"].(map[string]interface{})
	queries := run["queries"].(map[string]interface{})
	query := queries["search_query"].(map[string]interface{})

	got := query["qps"].(float64)
	if math.Abs(got-2.0) > 0.001 {
		t.Fatalf("expected query qps of 2.0, got %.3f", got)
	}
	if got := query["count"].(int); got != len(latencies) {
		t.Fatalf("expected query count of %d, got %d", len(latencies), got)
	}
	if _, exists := query["updates"]; exists {
		t.Fatal("disabled update workload should omit the updates field")
	}
	if _, exists := query["walBytesPerUpdate"]; exists {
		t.Fatal("read-only workload should omit WAL per update")
	}
}

func TestDashboardShowsLiveQueryCountAfterMax(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}

	content := string(html)
	maxPos := strings.Index(content, `id="${queryId}-max"`)
	countPos := strings.Index(content, `id="${queryId}-count"`)
	if maxPos < 0 || countPos < 0 || countPos < maxPos {
		t.Fatalf("expected query count card immediately after max card")
	}
	if !strings.Contains(content, `class="query-stat-label">Queries</div>`) {
		t.Fatalf("expected query count card to be labeled Queries")
	}
	if !strings.Contains(content, `"Per Query"`) {
		t.Fatal("expected dynamic Per Query card")
	}
	if !strings.Contains(content, "formatNumber(q.count || 0)") {
		t.Fatalf("expected query count card to update from live count data")
	}
	if strings.Count(content, "white-space: nowrap;") < 2 {
		t.Fatal("expected query metric values and labels to stay on one line")
	}
	if !strings.Contains(content, "totalIndexBytes / q.count") {
		t.Fatal("expected bytes per query to use total index traffic and query count")
	}
}

func TestPrewarmProgressSurvivesLiveAndExportAggregation(t *testing.T) {
	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"custom_topk": {
					Name:            "custom_topk",
					Backend:         "custom",
					Queries:         map[string]*QueryMetrics{},
					PrewarmProgress: 42.5,
					Prewarming:      true,
				},
			},
		},
	}

	summaryRun := o.getSummary()["runs"].(map[string]interface{})["custom_topk"].(map[string]interface{})
	if got := summaryRun["prewarmProgress"]; got != 42.5 {
		t.Fatalf("live prewarm progress = %v, want 42.5", got)
	}
	if got := summaryRun["prewarming"]; got != true {
		t.Fatalf("live prewarming = %v, want true", got)
	}

	raw := o.getExportData()
	rawRun := raw["runs"].(map[string]interface{})["custom_topk"].(map[string]interface{})
	if got := rawRun["prewarmProgress"]; got != 42.5 {
		t.Fatalf("raw prewarm progress = %v, want 42.5", got)
	}
	if got := rawRun["prewarming"]; got != true {
		t.Fatalf("raw prewarming = %v, want true", got)
	}

	aggregated := aggregateExportData(raw, time.Second, 0)
	aggregatedRun := aggregated["runs"].(map[string]interface{})["custom_topk"].(map[string]interface{})
	if got := aggregatedRun["prewarmProgress"]; got != 42.5 {
		t.Fatalf("aggregated prewarm progress = %v, want 42.5", got)
	}
	if got := aggregatedRun["prewarming"]; got != true {
		t.Fatalf("aggregated prewarming = %v, want true", got)
	}
}

func TestDashboardShowsPrewarmProgressUntilMeasurementStarts(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`return run.prewarming || Object.keys(run.queries || {}).length > 0`,
		`<span>Prewarm</span>`,
		`id="${safeNameId}-prewarm-percent"`,
		`id="${safeNameId}-prewarm-fill"`,
		`prewarmEl.style.display = run.prewarming ? "block" : "none"`,
		`Math.min(100, Number(run.prewarmProgress) || 0)`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing prewarm UI fragment %q", fragment)
		}
	}
}

func TestPrewarmMetricSwitchesToMeasurementAndCannotReopen(t *testing.T) {
	registry := k6metrics.NewRegistry()
	progressMetric, err := registry.NewMetric("prewarm_progress", k6metrics.Gauge)
	if err != nil {
		t.Fatalf("create progress metric: %v", err)
	}
	startedMetric, err := registry.NewMetric("scenario_started", k6metrics.Gauge)
	if err != nil {
		t.Fatalf("create started metric: %v", err)
	}
	tags := registry.RootTagSet().With("backend", "custom").With("scenario", "custom_topk")
	o := &Output{
		data: &DashboardData{
			StartTime:  time.Unix(0, 0),
			Runs:       map[string]*RunMetrics{},
			Containers: map[string]*ContainerMetrics{},
		},
	}
	push := func(metric *k6metrics.Metric, value float64, at time.Time) {
		o.AddMetricSamples([]k6metrics.SampleContainer{k6metrics.Samples{{
			TimeSeries: k6metrics.TimeSeries{Metric: metric, Tags: tags},
			Time:       at,
			Value:      value,
		}}})
		o.flush()
	}

	push(progressMetric, 37.5, time.UnixMilli(1000))
	run := o.data.Runs["custom"]
	if run == nil || !run.Prewarming || run.PrewarmProgress != 37.5 || run.StartTime != 0 {
		t.Fatalf("prewarm run = %#v", run)
	}
	push(progressMetric, 25, time.UnixMilli(1001))
	if run.PrewarmProgress != 37.5 {
		t.Fatalf("prewarm progress regressed to %v", run.PrewarmProgress)
	}

	push(startedMetric, 1, time.UnixMilli(2000))
	if run.Prewarming || run.StartTime != 2000 {
		t.Fatalf("measured run = %#v", run)
	}

	push(progressMetric, 100, time.UnixMilli(1500))
	if run.Prewarming {
		t.Fatal("late prewarm metric reopened a measured run")
	}
	if run.PrewarmProgress != 100 {
		t.Fatalf("final prewarm progress = %v, want 100", run.PrewarmProgress)
	}
}

func TestSelectedQuerySurvivesLiveAndExportAggregation(t *testing.T) {
	const query = "rust AND postgres"
	o := &Output{
		data: &DashboardData{
			StartTime:     time.Unix(0, 0),
			SelectedQuery: query,
			Runs:          map[string]*RunMetrics{},
			Containers:    map[string]*ContainerMetrics{},
		},
	}

	if got := o.getSummary()["selectedQuery"]; got != query {
		t.Fatalf("live selected query = %v, want %q", got, query)
	}
	raw := o.getExportData()
	if got := raw["selectedQuery"]; got != query {
		t.Fatalf("raw selected query = %v, want %q", got, query)
	}
	aggregated := aggregateExportData(raw, time.Second, 0)
	if got := aggregated["selectedQuery"]; got != query {
		t.Fatalf("aggregated selected query = %v, want %q", got, query)
	}
}

func TestSelectedQueryComesFromBenchmarkerExternalOptions(t *testing.T) {
	external := map[string]json.RawMessage{
		"benchmarker": json.RawMessage(`{"selectedQuery":"  rust AND postgres  "}`),
	}
	if got := selectedQueryFromExternal(external); got != "rust AND postgres" {
		t.Fatalf("selected query = %q, want %q", got, "rust AND postgres")
	}
	if got := selectedQueryFromExternal(nil); got != "" {
		t.Fatalf("unset selected query = %q, want empty", got)
	}
}

func TestBenchmarkQueryDisplayName(t *testing.T) {
	tests := []struct {
		name       string
		run        *RunMetrics
		querySet   string
		workload   string
		queryStyle string
		fallback   string
		want       string
	}{
		{
			name:     "term set",
			run:      &RunMetrics{Backend: "paradedb"},
			querySet: "3-terms",
			workload: "disjunction",
			fallback: "paradedb_topk_disjunction",
			want:     "paradedb (3-term, disjunction)",
		},
		{
			name:     "named set and alias",
			run:      &RunMetrics{Backend: "paradedb", Alias: "paradedb-alternate"},
			querySet: "combined",
			workload: "phrase",
			fallback: "paradedb_alternate_topk_phrase",
			want:     "paradedb-alternate (combined, phrase)",
		},
		{
			name:       "topk includes query style",
			run:        &RunMetrics{Backend: "custom"},
			querySet:   "5-terms",
			workload:   "topk",
			queryStyle: "conjunction",
			fallback:   "custom_topk_conjunction",
			want:       "custom (5-term, topk/conjunction)",
		},
		{
			name:       "count retains operation",
			run:        &RunMetrics{Backend: "custom"},
			querySet:   "5-terms",
			workload:   "count",
			queryStyle: "phrase",
			fallback:   "custom_count",
			want:       "custom (5-term, count/phrase)",
		},
		{
			name:     "missing metadata preserves internal name",
			run:      &RunMetrics{Backend: "paradedb"},
			querySet: "",
			workload: "count",
			fallback: "paradedb_count",
			want:     "paradedb_count",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := benchmarkQueryDisplayName(test.run, test.querySet, test.workload, test.queryStyle, test.fallback); got != test.want {
				t.Fatalf("display name = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFriendlyQueryDisplayNameSurvivesDashboardDataPaths(t *testing.T) {
	const (
		runName       = "paradedb"
		internalName  = "paradedb_topk_disjunction"
		expectedLabel = "paradedb (3-term, topk/disjunction)"
	)

	o := &Output{
		benchmarkQuerySet:   "3-terms",
		benchmarkWorkload:   "topk",
		benchmarkQueryStyle: "disjunction",
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				runName: {
					Name:      runName,
					Backend:   runName,
					StartTime: 1_000,
					EndTime:   2_000,
					Queries: map[string]*QueryMetrics{
						internalName: queryMetricsFixture(internalName, []float64{10}, 1_000, 1_000),
					},
				},
			},
			Containers: map[string]*ContainerMetrics{},
		},
	}

	raw := o.getExportData()
	dataPaths := map[string]map[string]interface{}{
		"live summary": o.getSummary(),
		"raw export":   raw,
		"HTML export":  aggregateExportData(raw, time.Second, time.Second),
	}
	for name, data := range dataPaths {
		runs := data["runs"].(map[string]interface{})
		run := runs[runName].(map[string]interface{})
		queries := run["queries"].(map[string]interface{})
		query := queries[internalName].(map[string]interface{})
		if got := query["displayName"]; got != expectedLabel {
			t.Errorf("%s display name = %q, want %q", name, got, expectedLabel)
		}
	}
}

func TestDashboardUsesFriendlyQueryDisplayNames(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`const displayName = q.displayName || qName`,
		`escapeHtml(displayName)`,
		`label: displayName`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing friendly query label fragment %q", fragment)
		}
	}
}

func TestDashboardRendersSelectedQueryInHeaderAsText(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`id="dashboard-title"`,
		`? ` + "`ParadeDB Benchmarker - ${data.selectedQuery}`",
		`document.getElementById("dashboard-title").textContent = dashboardTitle`,
		`document.title = dashboardTitle`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing selected-query header fragment %q", fragment)
		}
	}
}

func TestDashboardStartsLatencyRunsAtFirstTimelinePoint(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}

	content := string(html)
	if !strings.Contains(content, "getTimelineStart(queries, run.startTime || 0)") {
		t.Fatal("expected latency chart to use the first timeline point as its visual baseline")
	}
	if !strings.Contains(content, "query.timeline?.[0]?.time") {
		t.Fatal("expected timeline baseline to preserve relative offsets within each run")
	}
}

func TestQueryTimelineAdvancesOnlyForNewSamples(t *testing.T) {
	o := &Output{timelineWindow: time.Second}
	query := queryMetricsFixture("query", []float64{10}, 1_000, 1_000)

	o.updateQueryTimeline(query)
	o.updateQueryTimeline(query)

	if got := len(query.Timeline); got != 1 {
		t.Fatalf("timeline points without a new sample = %d, want 1", got)
	}
	if got := query.Timeline[0].Time; got != 1_000 {
		t.Fatalf("timeline point time = %d, want sample time 1000", got)
	}

	query.recordLatency(20, 1_200)
	o.updateQueryTimeline(query)
	if got := query.Timeline[len(query.Timeline)-1].Time; got != 1_200 {
		t.Fatalf("latest timeline point time = %d, want sample time 1200", got)
	}
}

func TestFlushAnchorsQueryTimelineToMetricTime(t *testing.T) {
	registry := k6metrics.NewRegistry()
	queryDuration, err := registry.NewMetric("query_duration", k6metrics.Trend, k6metrics.Time)
	if err != nil {
		t.Fatalf("create query duration metric: %v", err)
	}
	containerCPU, err := registry.NewMetric("container_cpu_percent", k6metrics.Gauge)
	if err != nil {
		t.Fatalf("create container CPU metric: %v", err)
	}
	backend := "event-time-" + t.Name()
	queryTags := registry.RootTagSet().With("backend", backend).With("scenario", "query")
	o := &Output{
		timelineWindow: time.Second,
		data: &DashboardData{
			StartTime:  time.Unix(0, 0),
			Runs:       make(map[string]*RunMetrics),
			Containers: make(map[string]*ContainerMetrics),
		},
	}
	completedAt := time.UnixMilli(1_000)
	o.AddMetricSamples([]k6metrics.SampleContainer{k6metrics.Samples{{
		TimeSeries: k6metrics.TimeSeries{Metric: queryDuration, Tags: queryTags},
		Time:       completedAt,
		Value:      10,
	}}})
	o.flush()

	query := o.data.Runs[backend].Queries["query"]
	if got := query.Timeline[0].Time; got != completedAt.UnixMilli() {
		t.Fatalf("timeline point time = %d, want metric time %d", got, completedAt.UnixMilli())
	}

	containerTags := registry.RootTagSet().With("container", "postgres")
	o.AddMetricSamples([]k6metrics.SampleContainer{k6metrics.Samples{{
		TimeSeries: k6metrics.TimeSeries{Metric: containerCPU, Tags: containerTags},
		Time:       completedAt.Add(time.Second),
		Value:      50,
	}}})
	o.flush()
	if got := len(query.Timeline); got != 1 {
		t.Fatalf("container flush extended query timeline to %d points, want 1", got)
	}
}

func TestBuildTimelineEndsAtLastSample(t *testing.T) {
	timeline := buildTimeline(
		[]float64{10, 20},
		[]int64{1_000, 1_950},
		[]int64{1, 2},
		200*time.Millisecond,
		time.Second,
	)
	if len(timeline) == 0 {
		t.Fatal("timeline is empty")
	}
	if got := timeline[len(timeline)-1].Time; got != 1_950 {
		t.Fatalf("last timeline point = %d, want last sample time 1950", got)
	}
}

func TestIndexIOStatsSurviveLiveAndExportAggregation(t *testing.T) {
	const backend = "index-io-dashboard-test"
	stats := metrics.IndexIOStats{
		ReadBytes: 32768,
		HitBytes:  65536,
		Read:      "32 kB",
		Hit:       "64 kB",
	}
	metrics.RegisterIndexIOStats(backend, stats)

	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"search": {
					Name:      "search",
					Backend:   backend,
					StartTime: 1000,
					EndTime:   2000,
					Queries: map[string]*QueryMetrics{
						"search_query": queryMetricsFixture("search_query", []float64{1}, 1500, 1500),
					},
				},
			},
		},
	}

	summaryQuery := o.getSummary()["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	assertIndexIOStats(t, summaryQuery, stats)

	raw := o.getExportData()
	rawQuery := raw["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	assertIndexIOStats(t, rawQuery, stats)

	aggregated := aggregateExportData(raw, time.Second, 0)
	aggregatedQuery := aggregated["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	assertIndexIOStats(t, aggregatedQuery, stats)
}

func TestPostgresDiagnosticsAppearOnlyInExportPayload(t *testing.T) {
	backend := "postgres-diagnostics-dashboard-" + t.Name()
	metrics.ResetPostgresDiagnostics(backend)
	metrics.RegisterPostgresDiagnostics(backend, metrics.PostgresDiagnosticsSample{
		Time: 1234,
		WAL:  metrics.PostgresWALDiagnostics{Bytes: 4096},
	})
	defer metrics.ResetPostgresDiagnostics(backend)

	o := &Output{data: &DashboardData{
		StartTime: time.Unix(0, 0),
		Runs:      map[string]*RunMetrics{},
	}}
	if _, exists := o.getSummary()["postgresDiagnostics"]; exists {
		t.Fatal("live dashboard exposed PostgreSQL diagnostics")
	}
	exported, ok := o.getExportData()["postgresDiagnostics"].(map[string][]metrics.PostgresDiagnosticsSample)
	if !ok || len(exported[backend]) != 1 || exported[backend][0].WAL.Bytes != 4096 {
		t.Fatalf("exported diagnostics = %#v", exported)
	}
}

func TestUpdateCountSurvivesLiveAndExportAggregation(t *testing.T) {
	backend := "update-dashboard-" + t.Name()
	workload := metrics.RegisterUpdateWorkload(backend)
	for range 2 {
		workload.RecordCompletedUpdate()
	}
	metrics.ResetWALStats(backend, 1000, 0)
	if !metrics.RegisterWALPosition(backend, 1256, 2) {
		t.Fatal("register WAL position")
	}

	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"search": {
					Name:    "search",
					Backend: backend,
					Queries: map[string]*QueryMetrics{
						"search_query": queryMetricsFixture("search_query", []float64{1}, 1500, 1500),
					},
				},
			},
		},
	}

	summaryQuery := o.getSummary()["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	if got := summaryQuery["updates"]; got != uint64(2) {
		t.Fatalf("live updates = %v, want 2", got)
	}
	assertWALStats(t, summaryQuery, 256, 128)
	raw := o.getExportData()
	rawQuery := raw["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	if got := rawQuery["updates"]; got != uint64(2) {
		t.Fatalf("raw updates = %v, want 2", got)
	}
	assertWALStats(t, rawQuery, 256, 128)
	aggregated := aggregateExportData(raw, time.Second, 0)
	aggregatedQuery := aggregated["runs"].(map[string]interface{})["search"].(map[string]interface{})["queries"].(map[string]interface{})["search_query"].(map[string]interface{})
	if got := aggregatedQuery["updates"]; got != uint64(2) {
		t.Fatalf("aggregated updates = %v, want 2", got)
	}
	assertWALStats(t, aggregatedQuery, 256, 128)
}

func TestRawJSONIncludesUpdateMetricsWithoutExposingThemToDashboard(t *testing.T) {
	registry := k6metrics.NewRegistry()
	duration, err := registry.NewMetric("update_duration", k6metrics.Trend, k6metrics.Time)
	if err != nil {
		t.Fatalf("create update duration metric: %v", err)
	}
	documents, err := registry.NewMetric("update_docs", k6metrics.Counter)
	if err != nil {
		t.Fatalf("create update documents metric: %v", err)
	}
	errors, err := registry.NewMetric("update_errors", k6metrics.Counter)
	if err != nil {
		t.Fatalf("create update errors metric: %v", err)
	}

	backend := "raw-update-metrics-" + t.Name()
	tags := registry.RootTagSet().With("backend", backend)
	o := &Output{
		data: &DashboardData{
			StartTime:  time.Unix(0, 0),
			Runs:       make(map[string]*RunMetrics),
			Containers: make(map[string]*ContainerMetrics),
		},
	}
	o.AddMetricSamples([]k6metrics.SampleContainer{k6metrics.Samples{
		{TimeSeries: k6metrics.TimeSeries{Metric: duration, Tags: tags}, Time: time.UnixMilli(1000), Value: 2.5},
		{TimeSeries: k6metrics.TimeSeries{Metric: documents, Tags: tags}, Time: time.UnixMilli(1000), Value: 1},
		{TimeSeries: k6metrics.TimeSeries{Metric: duration, Tags: tags}, Time: time.UnixMilli(2000), Value: 15},
		{TimeSeries: k6metrics.TimeSeries{Metric: errors, Tags: tags}, Time: time.UnixMilli(2000), Value: 1},
	}})
	o.flush()

	raw := o.getExportData()
	rawRun := raw["runs"].(map[string]interface{})[backend].(map[string]interface{})
	updateMetrics, ok := rawRun["updateMetrics"].(*UpdateMetrics)
	if !ok {
		t.Fatalf("raw export updateMetrics = %#v", rawRun["updateMetrics"])
	}
	if len(updateMetrics.Duration) != 2 || updateMetrics.Duration[1] != (TimeValue{Time: 2000, Value: 15}) {
		t.Fatalf("duration samples = %#v", updateMetrics.Duration)
	}
	if len(updateMetrics.Documents) != 1 || updateMetrics.Documents[0] != (TimeValue{Time: 1000, Value: 1}) {
		t.Fatalf("document samples = %#v", updateMetrics.Documents)
	}
	if len(updateMetrics.Errors) != 1 || updateMetrics.Errors[0] != (TimeValue{Time: 2000, Value: 1}) {
		t.Fatalf("error samples = %#v", updateMetrics.Errors)
	}

	jsonData, err := marshalExportJSON(raw)
	if err != nil {
		t.Fatalf("marshal raw export: %v", err)
	}
	if !strings.Contains(string(jsonData), `"updateMetrics"`) {
		t.Fatalf("JSON export omitted updateMetrics: %s", jsonData)
	}
	if _, exists := o.getSummary()["runs"].(map[string]interface{})[backend].(map[string]interface{})["updateMetrics"]; exists {
		t.Fatal("live dashboard summary exposed raw update metrics")
	}
	aggregatedRun := aggregateExportData(raw, time.Second, 0)["runs"].(map[string]interface{})[backend].(map[string]interface{})
	if _, exists := aggregatedRun["updateMetrics"]; exists {
		t.Fatal("standalone dashboard exposed raw update metrics")
	}
}

func TestDashboardShowsUpdateMetricsAtEnd(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`function ensureOptionalQueryStats(queryEl, queryId, query)`,
		`statsEl.insertBefore(bytesCard, updatesEl?.parentElement || null)`,
		`updatesCard.className = "query-stat-update-start"`,
		`statsEl.appendChild(updatesCard)`,
		`statsEl.appendChild(walCard)`,
		`ensureOptionalQueryStats(queryEl, queryId, q);`,
		`formatNumber(q.updates ?? 0)`,
		`"WAL"`,
		`formatBytes(`,
		`q.walBytesPerUpdate ?? 0`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing update UI fragment %q", fragment)
		}
	}
}

func TestDashboardQueryStatsLayoutAdaptsToContent(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`grid-auto-flow: column`,
		`grid-auto-columns: minmax(max-content, 1fr)`,
		`grid-template-columns: minmax(0, 1fr) minmax(390px, max-content)`,
		`grid-template-columns: repeat(auto-fit, minmax(64px, 1fr))`,
		`.query-stat-update-start`,
		`border-left: 1px solid var(--border)`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing content-driven layout %q", fragment)
		}
	}
	for _, fragment := range []string{
		`.query-stats.with-updates`,
		`.query-stats.with-wal`,
		`.query-stats.with-index-io`,
		`repeat(10, 1fr)`,
	} {
		if strings.Contains(content, fragment) {
			t.Fatalf("dashboard retains count-specific layout %q", fragment)
		}
	}
}

func TestDashboardQueryChartDefaultsToLogScaleAndCanSwitchToLinear(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	for _, fragment := range []string{
		`let queryScaleType = "logarithmic"`,
		`class="scale-type-select"`,
		`<option value="logarithmic" selected>Log Scale</option>`,
		`<option value="linear">Linear Scale</option>`,
		`createChartOptions("ms", groupId, queryScaleType)`,
		`chart.options.scales.y.type = queryScaleType`,
		`if (logarithmic) delete yScale.min`,
		`if (!logarithmic) yScale.min = 0`,
	} {
		if !strings.Contains(content, fragment) {
			t.Fatalf("dashboard missing query-scale fragment %q", fragment)
		}
	}
}

func TestDashboardLogScaleExcludesZeroWhenZooming(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	start := strings.Index(content, "      function getVisibleYRange")
	if start < 0 {
		t.Fatal("could not find visible Y-range function")
	}
	end := strings.Index(content[start:], "      function getAllCharts")
	if end < 0 {
		t.Fatal("could not find end of visible Y-range function")
	}

	runtime := sobek.New()
	_, err = runtime.RunString(content[start:start+end] + `
const chart = {
  options: { scales: { y: { type: "logarithmic" } } },
  data: { datasets: [{ data: [{ x: 1, y: 0 }, { x: 2, y: 10 }] }] },
};
const logRange = getVisibleYRange(chart, 0, 3);
if (logRange.min <= 0 || Math.abs(logRange.min - 10 / 1.2) > 0.0001) {
  throw new Error("invalid logarithmic range: " + JSON.stringify(logRange));
}
chart.options.scales.y.type = "linear";
const linearRange = getVisibleYRange(chart, 0, 3);
if (linearRange.min !== 0 || linearRange.max !== 11) {
  throw new Error("invalid linear range: " + JSON.stringify(linearRange));
}
`)
	if err != nil {
		t.Fatalf("evaluate visible Y ranges: %v", err)
	}
}

func TestDashboardConditionallyCreatesWALPerUpdateCard(t *testing.T) {
	html, err := staticFiles.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("read dashboard HTML: %v", err)
	}
	content := string(html)
	start := strings.Index(content, "      function createQueryStat")
	if start < 0 {
		t.Fatal("could not extract optional query-stat functions")
	}
	end := strings.Index(content[start:], "      function safeId")
	if end < 0 {
		t.Fatal("could not find end of optional query-stat functions")
	}
	functions := content[start : start+end]
	harness := `
function makeElement(tag) {
  return {
    tag,
    id: "",
    textContent: "",
    className: "",
    children: [],
    parentElement: null,
    append(...nodes) {
      for (const node of nodes) {
        node.parentElement = this;
        this.children.push(node);
      }
    },
    appendChild(node) { this.append(node); },
    insertBefore(node, reference) {
      node.parentElement = this;
      const index = reference == null ? -1 : this.children.indexOf(reference);
      if (index < 0) this.children.push(node); else this.children.splice(index, 0, node);
    },
    get firstElementChild() { return this.children[0] || null; },
  };
}
const statsEl = makeElement("div");
const queryEl = { querySelector: () => statsEl };
function findById(node, id) {
  if (node.id === id) return node;
  for (const child of node.children || []) {
    const found = findById(child, id);
    if (found) return found;
  }
  return null;
}
const document = {
  createElement: makeElement,
  getElementById: (id) => findById(statsEl, id),
};
ensureOptionalQueryStats(queryEl, "custom", { walBytesPerUpdate: 99 });
if (statsEl.children.length !== 0) {
  throw new Error("read-only query created an optional card");
}
ensureOptionalQueryStats(queryEl, "custom", { updates: 1 });
const updating = {
  updates: 2,
  walBytesPerUpdate: 128,
  indexReadBytes: 10,
  indexHitBytes: 20,
};
ensureOptionalQueryStats(queryEl, "custom", updating);
const ids = statsEl.children.map((card) => card.firstElementChild.id).join("|");
if (ids !== "custom-bytes-per-query|custom-updates|custom-wal-per-update") {
  throw new Error("optional card order = " + ids);
}
if (statsEl.children[1].className !== "query-stat-update-start") {
  throw new Error("update metrics are missing their divider");
}
ensureOptionalQueryStats(queryEl, "custom", updating);
if (statsEl.children.length !== 3) {
  throw new Error("optional cards were duplicated");
}
`
	if _, err := sobek.New().RunString(functions + harness); err != nil {
		t.Fatalf("optional dashboard stats behavior: %v", err)
	}
}

func assertWALStats(t *testing.T, query map[string]interface{}, wantBytes uint64, wantPerUpdate float64) {
	t.Helper()
	if got := query["walBytes"]; got != wantBytes {
		t.Fatalf("WAL bytes = %v, want %d", got, wantBytes)
	}
	if got := query["walBytesPerUpdate"]; got != wantPerUpdate {
		t.Fatalf("WAL bytes/update = %v, want %.1f", got, wantPerUpdate)
	}
}

func assertIndexIOStats(t *testing.T, query map[string]interface{}, want metrics.IndexIOStats) {
	t.Helper()
	if query["indexRead"] != want.Read || query["indexHit"] != want.Hit {
		t.Fatalf("pretty index I/O = %v/%v, want %v/%v", query["indexRead"], query["indexHit"], want.Read, want.Hit)
	}
	if query["indexReadBytes"] != want.ReadBytes || query["indexHitBytes"] != want.HitBytes {
		t.Fatalf("index I/O bytes = %v/%v, want %v/%v", query["indexReadBytes"], query["indexHitBytes"], want.ReadBytes, want.HitBytes)
	}
}

func TestGetSummaryUsesConfiguredChartDuration(t *testing.T) {
	o := &Output{
		data: &DashboardData{
			StartTime:     time.Now(),
			TotalDuration: 300,
			Runs:          map[string]*RunMetrics{},
		},
	}

	summary := o.getSummary()
	if got := summary["chartDuration"].(float64); got != 300 {
		t.Fatalf("chart duration = %v, want 300", got)
	}
}

func TestGetSummaryEmitsTopLevelBackendsBlock(t *testing.T) {
	metrics.RegisterBackendOptions("paradedb", &metrics.BackendOptions{
		Container: "paradedb",
		Alias:     "paradedb",
		Color:     "#7c3aed",
	})
	metrics.RegisterBackendConfig("paradedb", map[string]interface{}{
		"version": "PostgreSQL 18.0",
	})
	t.Cleanup(func() {
		metrics.RegisterBackendConfig("paradedb", nil)
	})

	o := &Output{
		data: &DashboardData{
			StartTime: time.Unix(0, 0),
			Runs: map[string]*RunMetrics{
				"paradedb_simple": {
					Name:    "paradedb_simple",
					Backend: "paradedb",
					Queries: map[string]*QueryMetrics{},
				},
			},
		},
	}

	summary := o.getSummary()

	// Top-level backends block exists and carries the deduplicated config.
	backends, ok := summary["backends"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected top-level backends block, got %T", summary["backends"])
	}
	paradedb, ok := backends["paradedb"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected backends[paradedb], got %T", backends["paradedb"])
	}
	cfg, ok := paradedb["config"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected backends[paradedb].config map")
	}
	if cfg["version"] != "PostgreSQL 18.0" {
		t.Fatalf("expected version in config, got %v", cfg["version"])
	}

	// Per-run entries no longer carry config / containerLimits.
	run := summary["runs"].(map[string]interface{})["paradedb_simple"].(map[string]interface{})
	if _, present := run["config"]; present {
		t.Fatalf("expected run.config to be stripped (now top-level)")
	}
	if _, present := run["containerLimits"]; present {
		t.Fatalf("expected run.containerLimits to be stripped (now under container_info)")
	}
}

func TestReadMetaEnvInline(t *testing.T) {
	t.Setenv("BENCHMARKER_META", `{"commit":"abc123","version":"v0.23.1"}`)
	m := readMetaEnv()
	if m["commit"] != "abc123" {
		t.Fatalf("expected commit=abc123, got %v", m["commit"])
	}
	if m["version"] != "v0.23.1" {
		t.Fatalf("expected version=v0.23.1, got %v", m["version"])
	}
}

func TestReadMetaEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	if err := os.WriteFile(path, []byte(`{"commit":"xyz","links":["https://example.com"]}`), 0644); err != nil {
		t.Fatalf("write meta file: %v", err)
	}
	t.Setenv("BENCHMARKER_META", "@"+path)
	m := readMetaEnv()
	if m["commit"] != "xyz" {
		t.Fatalf("expected commit=xyz, got %v", m["commit"])
	}
	links, ok := m["links"].([]interface{})
	if !ok || len(links) != 1 || links[0] != "https://example.com" {
		t.Fatalf("expected links=[example.com], got %v", m["links"])
	}
}

func TestReadMetaEnvUnsetReturnsNil(t *testing.T) {
	t.Setenv("BENCHMARKER_META", "")
	if m := readMetaEnv(); m != nil {
		t.Fatalf("expected nil for unset env, got %v", m)
	}
}

// The standalone `--out dashboard=html` export hands aggregateExportData the
// in-memory getExportData() map, whose sample arrays are native Go slices
// ([]float64/[]int64) and whose times are native int64, not json.Unmarshal's
// []interface{}/float64. Regression: the json* helpers only accepted the
// unmarshaled types, so every query sample was dropped (count=0, empty timeline)
// while container CPU/mem still rendered.
func TestAggregateExportDataHandlesNativeGoTypes(t *testing.T) {
	const n = 100
	start := int64(1_000_000)
	lat := make([]float64, n)
	ts := make([]int64, n)
	hits := make([]int64, n)
	for i := 0; i < n; i++ {
		lat[i] = 2.0
		ts[i] = start + int64(i)*100 // spread across a ~10s window
		hits[i] = 10
	}
	rawData := map[string]interface{}{
		"runs": map[string]interface{}{
			"run1": map[string]interface{}{
				"startTime": start,             // native int64, not float64
				"endTime":   start + n*100 + 1, // native int64
				"queries": map[string]interface{}{
					"q1": map[string]interface{}{
						"name":       "q1",
						"vus":        1,
						"executor":   "constant-vus",
						"latencies":  lat,  // native []float64
						"timestamps": ts,   // native []int64
						"hitCounts":  hits, // native []int64
						"query":      "SELECT 1",
					},
				},
			},
		},
	}

	out := aggregateExportData(rawData, time.Second, 0)
	q := out["runs"].(map[string]interface{})["run1"].(map[string]interface{})["queries"].(map[string]interface{})["q1"].(map[string]interface{})

	if got := q["count"].(int); got != n {
		t.Fatalf("count = %d, want %d (native-typed samples were dropped)", got, n)
	}
	if tl, ok := q["timeline"].([]TimelinePoint); !ok || len(tl) == 0 {
		t.Fatalf("timeline is empty; native-typed samples were dropped: %v", q["timeline"])
	}
}

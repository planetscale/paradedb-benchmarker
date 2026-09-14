package search

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"

	// Import backends to register them via init()
	_ "github.com/paradedb/benchmarker/backends/clickhouse"
	_ "github.com/paradedb/benchmarker/backends/elasticsearch"
	_ "github.com/paradedb/benchmarker/backends/mongodb"
	_ "github.com/paradedb/benchmarker/backends/opensearch"
	_ "github.com/paradedb/benchmarker/backends/paradedb"
	_ "github.com/paradedb/benchmarker/backends/pg_textsearch"
	_ "github.com/paradedb/benchmarker/backends/postgres"
)

// Backends holds all configured backend clients.
type Backends struct {
	vu                      modules.VU
	clients                 map[string]*backends.K6Client
	indexIOEnabled          bool
	lastIndexIOSnapshot     time.Time
	lastPostgresDiagnostics map[string]time.Time
	containers              map[string]string
	stoppedContainers       map[string]struct{}
	completedBackends       map[string]struct{}
	Metrics                 *metrics.Collector `js:"metrics"`
}

const (
	indexIOPollInterval             = time.Second
	indexIOQueryTimeout             = 2 * time.Second
	indexIOResetTimeout             = 10 * time.Second
	walQueryTimeout                 = 2 * time.Second
	postgresDiagnosticsPollInterval = time.Second
	postgresDiagnosticsQueryTimeout = 2 * time.Second
	postgresDiagnosticsResetTimeout = 10 * time.Second
	finalSnapshotDelay              = time.Second
	finalSnapshotTag                = "index_io_final_snapshot"
	maxSafeInteger                  = uint64(1<<53 - 1)
)

// Get returns a backend client by its alias/name.
func (b *Backends) Get(alias string) *backends.K6Client {
	return b.clients[alias]
}

// GetAll returns a list of backend names.
func (b *Backends) GetAll() []string {
	names := make([]string, 0, len(b.clients))
	for name := range b.clients {
		names = append(names, name)
	}
	return names
}

func (m *ModuleInstance) configErrorf(format string, args ...interface{}) {
	err := fmt.Errorf(format, args...)
	if m.vu != nil {
		if rt := m.vu.Runtime(); rt != nil {
			common.Throw(rt, err)
			return
		}
	}
	panic(err.Error())
}

// newBackends creates a new backends registry with the specified configuration.
func (m *ModuleInstance) newBackends(config map[string]interface{}) *Backends {
	b := &Backends{
		vu:                m.vu,
		clients:           make(map[string]*backends.K6Client),
		containers:        make(map[string]string),
		stoppedContainers: make(map[string]struct{}),
	}
	var enabledContainers []string
	ctx := context.Background()

	datasetPath := parseDatasetPath(config, m.vu)
	defaults := backends.DefaultConnections()
	defaultContainers := backends.DefaultContainers()

	// Capture dataset.yaml (written by `loader pull`) if present.
	if datasetPath != "" {
		if data, err := os.ReadFile(filepath.Join(datasetPath, "dataset.yaml")); err == nil {
			metrics.RegisterRunCapture("dataset_yaml", string(data))
		}
	}

	// Capture the running k6 script's source. k6 is invoked as
	// `./k6 run [flags] <script.js>` — the script path is in os.Args.
	if src, path := readRunningScript(); src != "" {
		metrics.RegisterRunCapture("script", src)
		metrics.RegisterRunCapture("script_path", path)
	}

	// Parse backends array
	backendsArray, ok := config["backends"].([]interface{})
	if !ok {
		m.configErrorf("backends: 'backends' array is required")
		return nil
	}

	for _, item := range backendsArray {
		var backendType, alias, container, color, conn string
		// containerExplicit tracks whether the user set the container field at all
		// (including to ""). An explicit empty string opts the backend out of
		// docker metrics — used for off-host services like AWS RDS.
		var containerExplicit bool

		switch v := item.(type) {
		case string:
			// Shorthand: just the backend type name
			backendType = v
			alias = v
		case map[string]interface{}:
			// Full config object
			var ok bool
			backendType, ok = v["type"].(string)
			if !ok {
				m.configErrorf("backends: each backend config must have a 'type' field")
				return nil
			}
			alias = backendType
			if a, ok := v["alias"].(string); ok {
				alias = a
			}
			if raw, ok := v["container"]; ok {
				containerExplicit = true
				if c, ok := raw.(string); ok {
					container = c
				}
			}
			if c, ok := v["color"].(string); ok {
				color = c
			}
			if c, ok := v["connection"].(string); ok {
				conn = c
			}
		default:
			m.configErrorf("backends: each backend must be a string or object")
			return nil
		}

		// Validate backend type
		backendCfg, ok := backends.GetConfig(backendType)
		if !ok {
			m.configErrorf("backends: unknown backend type '%s'. Valid types: %v",
				backendType, backends.RegisteredBackends())
			return nil
		}

		// Apply defaults
		if conn == "" {
			conn = defaults[backendType]
		}
		// Only default the container name if the user didn't explicitly set it.
		// An explicit "" opts out of docker capture (off-host backends).
		if !containerExplicit {
			if alias != backendType {
				container = alias // default container to alias if alias is set
			} else {
				container = defaultContainers[backendType]
			}
		}

		// Check for duplicate alias
		if _, exists := b.clients[alias]; exists {
			m.configErrorf("backends: duplicate alias '%s'", alias)
			return nil
		}

		driver, err := backends.NewDriver(backendType, conn)
		if err != nil {
			if container == "" {
				// Connection parser errors can include the supplied credentials.
				m.configErrorf("backends: failed to configure '%s'; check its connection settings", alias)
				return nil
			}
			m.configErrorf("backends: failed to create '%s': %v", alias, err)
			return nil
		}

		// Register backend options
		metrics.RegisterBackendOptions(alias, &metrics.BackendOptions{
			Container: container,
			Alias:     alias,
			Color:     color,
		})

		client := backends.NewK6Client(m.vu, driver, alias)
		client.SetExternal(container == "")
		client.SetMeasurementDeadlineProvider(func() (time.Time, bool) {
			return m.root.measurementDeadline(alias)
		})
		client.SetPhaseQueryStateProvider(func() (time.Time, bool, bool) {
			return m.root.queryPhase(alias)
		})
		b.clients[alias] = client
		if container != "" {
			enabledContainers = append(enabledContainers, container)
			b.containers[alias] = container
		}

		driver.CaptureConfig(ctx, alias)
		if container != "" {
			metrics.CapturePrePostScripts(alias, backendType, datasetPath, backendCfg.FileType)
		}
	}

	// Auto-create metrics collector for enabled containers
	if len(enabledContainers) > 0 {
		containersInterface := make([]interface{}, len(enabledContainers))
		for i, c := range enabledContainers {
			containersInterface[i] = c
		}
		b.Metrics = metrics.NewCollector(m.vu, map[string]interface{}{
			"containers": containersInterface,
		})
	}

	return b
}

// PhaseBoundary gracefully stops a completed backend, flushes host filesystem
// buffers, and leaves the storage device idle for the requested cooldown. The
// phase coordinator does not release the next backend until this method
// returns.
func (b *Backends) PhaseBoundary(alias, cooldownText string) {
	cooldown, err := time.ParseDuration(cooldownText)
	if err != nil || cooldown < 0 {
		b.failPhaseBoundary(fmt.Errorf("phase boundary: invalid backend cooldown %q", cooldownText))
		return
	}
	if b.clients[alias] == nil {
		b.failPhaseBoundary(fmt.Errorf("phase boundary: unknown backend %s", alias))
		return
	}
	if _, completed := b.completedBackends[alias]; completed {
		b.failPhaseBoundary(fmt.Errorf("phase boundary: backend %s is already complete", alias))
		return
	}
	if b.completedBackends == nil {
		b.completedBackends = make(map[string]struct{})
	}
	container := b.containers[alias]
	if container == "" {
		b.completedBackends[alias] = struct{}{}
		fmt.Printf("[phase] %s: remote backend complete\n", alias)
		return
	}
	if b.Metrics == nil {
		b.failPhaseBoundary(fmt.Errorf("phase boundary: Docker metrics are unavailable"))
		return
	}
	if _, stopped := b.stoppedContainers[container]; stopped {
		b.failPhaseBoundary(fmt.Errorf("phase boundary: container %s is already stopped", container))
		return
	}

	fmt.Printf("[phase] %s: stopping completed container\n", alias)
	if err := b.Metrics.StopContainer(container); err != nil {
		b.failPhaseBoundary(fmt.Errorf("phase boundary for %s: %w", alias, err))
		return
	}
	if b.stoppedContainers == nil {
		b.stoppedContainers = make(map[string]struct{})
	}
	b.stoppedContainers[container] = struct{}{}
	b.completedBackends[alias] = struct{}{}
	if cooldown == 0 {
		return
	}

	ctx := context.Background()
	if b.vu != nil && b.vu.Context() != nil {
		ctx = b.vu.Context()
	}
	if output, err := exec.CommandContext(ctx, "sync").CombinedOutput(); err != nil {
		b.failPhaseBoundary(fmt.Errorf("phase boundary for %s: sync host filesystems: %w: %s", alias, err, output))
		return
	}

	fmt.Printf("[phase] %s: cooling storage for %s\n", alias, cooldown)
	timer := time.NewTimer(cooldown)
	defer timer.Stop()
	select {
	case <-timer.C:
		fmt.Printf("[phase] %s: storage cooldown complete\n", alias)
	case <-ctx.Done():
		b.failPhaseBoundary(ctx.Err())
	}
}

func (b *Backends) failPhaseBoundary(err error) {
	b.throwRuntime(err)
}

// Collect collects metrics from all enabled containers and participating
// PostgreSQL search indexes. Index I/O snapshots are limited to once per
// second, except for the forced post-workload snapshot.
func (b *Backends) Collect() map[string]interface{} {
	finalSnapshot := b.isFinalIndexIOSnapshot()
	var result map[string]interface{}
	if b.Metrics != nil && !finalSnapshot {
		result = b.Metrics.Collect()
	}

	if indexIO := b.collectIndexIOStats(finalSnapshot); len(indexIO) > 0 {
		if result == nil {
			result = make(map[string]interface{})
		}
		result["index_io"] = indexIO
	}

	if !finalSnapshot {
		time.Sleep(500 * time.Millisecond)
	}
	return result
}

// CollectFinal takes a final container/index-I/O snapshot without the ordinary
// collector delay.
func (b *Backends) CollectFinal() map[string]interface{} {
	result := make(map[string]interface{})
	if b.Metrics != nil {
		for key, value := range b.Metrics.Collect() {
			result[key] = value
		}
	}
	if indexIO := b.collectIndexIOStats(true); len(indexIO) > 0 {
		result["index_io"] = indexIO
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

type indexIOResult struct {
	backend string
	stats   backends.IndexIOStats
	err     error
}

func (b *Backends) resetIndexIOStats() (bool, error) {
	enabled := false
	for alias, client := range b.clients {
		ctx, cancel := context.WithTimeout(context.Background(), indexIOResetTimeout)
		supported, err := client.ResetIndexIOStats(ctx)
		cancel()
		if err != nil {
			return true, fmt.Errorf("reset index I/O stats for %s: %w", alias, err)
		}
		enabled = enabled || supported
	}
	return enabled, nil
}

// EnableIndexIOStats initializes index-I/O collection for a custom scenario.
func (b *Backends) EnableIndexIOStats() {
	enabled, err := b.resetIndexIOStats()
	if err != nil {
		b.throwRuntime(err)
		return
	}
	b.indexIOEnabled = enabled
}

// ResetIndexIOStats resets index I/O counters for selected backend aliases.
func (b *Backends) ResetIndexIOStats(aliases []string) {
	for _, alias := range aliases {
		if b.clients[alias] == nil {
			b.throwRuntime(fmt.Errorf("reset index I/O stats references unknown backend %s", alias))
			return
		}
	}

	for _, alias := range aliases {
		ctx, cancel := context.WithTimeout(context.Background(), indexIOResetTimeout)
		_, err := b.clients[alias].ResetIndexIOStatsNow(ctx)
		cancel()
		if err != nil {
			b.throwRuntime(fmt.Errorf("reset index I/O stats for %s: %w", alias, err))
			return
		}
	}
}

// ResetWALStats captures phase-start WAL positions for selected update-enabled
// PostgreSQL backends. Collection failures are telemetry failures and do not
// abort benchmark measurement.
func (b *Backends) ResetWALStats(aliases []string) {
	for _, alias := range aliases {
		if b.clients[alias] == nil {
			b.throwRuntime(fmt.Errorf("reset WAL stats references unknown backend %s", alias))
			return
		}
	}

	for _, alias := range aliases {
		updateStats, enabled := metrics.GetUpdateWorkloadStats(alias)
		if !enabled {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), walQueryTimeout)
		position, supported, err := b.clients[alias].ReadWALPosition(ctx)
		cancel()
		if !supported {
			metrics.ClearWALStats(alias)
			continue
		}
		if err != nil {
			metrics.ClearWALStats(alias)
			fmt.Fprintf(os.Stderr, "[%s] WAL metrics baseline failed: %v\n", alias, err)
			continue
		}
		metrics.ResetWALStats(alias, position, updateStats.CompletedUpdates)
	}
}

// CollectWALStats samples one active backend's phase-scoped WAL delta. A
// missing baseline means the phase is still prewarming or does not run updates.
func (b *Backends) CollectWALStats(alias string) {
	client := b.clients[alias]
	if client == nil {
		fmt.Fprintf(os.Stderr, "[%s] WAL metrics collection failed: unknown backend\n", alias)
		return
	}
	if _, enabled := metrics.GetUpdateWorkloadStats(alias); !enabled {
		return
	}
	if _, active := metrics.GetWALStats(alias); !active {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), walQueryTimeout)
	position, supported, err := client.ReadWALPosition(ctx)
	cancel()
	if !supported {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] WAL metrics collection failed: %v\n", alias, err)
		return
	}
	updateStats, enabled := metrics.GetUpdateWorkloadStats(alias)
	if !enabled {
		return
	}
	if !metrics.RegisterWALPosition(alias, position, updateStats.CompletedUpdates) {
		err := fmt.Errorf("WAL position moved backwards or has no phase baseline")
		fmt.Fprintf(os.Stderr, "[%s] WAL metrics collection failed: %v\n", alias, err)
		return
	}
}

// ResetPostgresDiagnostics resets one measured phase's PostgreSQL diagnostic
// counters. Telemetry failures are reported but do not abort the benchmark.
func (b *Backends) ResetPostgresDiagnostics(aliases []string) {
	for _, alias := range aliases {
		if b.clients[alias] == nil {
			fmt.Fprintf(os.Stderr, "[%s] PostgreSQL diagnostics reset failed: unknown backend\n", alias)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), postgresDiagnosticsResetTimeout)
		supported, err := b.clients[alias].ResetPostgresDiagnostics(ctx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] PostgreSQL diagnostics reset failed: %v\n", alias, err)
			continue
		}
		if supported {
			metrics.ResetPostgresDiagnostics(alias)
			if b.containers[alias] == "" {
				// External counters remain cumulative; preserve a starting sample.
				b.CollectPostgresDiagnostics(alias, true)
			}
		}
	}
}

// CollectPostgresDiagnostics samples one active backend at most once per
// second. A forced sample is used at phase completion.
func (b *Backends) CollectPostgresDiagnostics(alias string, force bool) {
	client := b.clients[alias]
	if client == nil {
		fmt.Fprintf(os.Stderr, "[%s] PostgreSQL diagnostics collection failed: unknown backend\n", alias)
		return
	}

	now := time.Now()
	if b.lastPostgresDiagnostics == nil {
		b.lastPostgresDiagnostics = make(map[string]time.Time)
	}
	if !force && now.Sub(b.lastPostgresDiagnostics[alias]) < postgresDiagnosticsPollInterval {
		return
	}
	b.lastPostgresDiagnostics[alias] = now

	ctx, cancel := context.WithTimeout(context.Background(), postgresDiagnosticsQueryTimeout)
	sample, supported, err := client.ReadPostgresDiagnostics(ctx)
	cancel()
	if !supported {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] PostgreSQL diagnostics collection failed: %v\n", alias, err)
		return
	}
	sample.Time = time.Now().UnixMilli()
	metrics.RegisterPostgresDiagnostics(alias, sample)
}

func (b *Backends) throwRuntime(err error) {
	if b.vu != nil {
		if rt := b.vu.Runtime(); rt != nil {
			common.Throw(rt, err)
			return
		}
	}
	panic(err)
}

func (b *Backends) hasIndexIOStats() bool {
	for _, client := range b.clients {
		if client.IndexIOStatsEnabled() {
			return true
		}
	}
	return false
}

func (b *Backends) collectIndexIOStats(force bool) map[string]interface{} {
	if !b.indexIOEnabled {
		return nil
	}
	now := time.Now()
	if !force && now.Sub(b.lastIndexIOSnapshot) < indexIOPollInterval {
		return nil
	}
	b.lastIndexIOSnapshot = now

	targets := make(map[string]*backends.K6Client)
	for alias, client := range b.clients {
		container := b.containers[alias]
		_, stopped := b.stoppedContainers[container]
		_, completed := b.completedBackends[alias]
		if client.IndexIOStatsEnabled() && !stopped && !completed {
			targets[alias] = client
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), indexIOQueryTimeout)
	defer cancel()
	results := make(chan indexIOResult, len(targets))
	for alias, client := range targets {
		go func(backend string, client *backends.K6Client) {
			stats, _, err := client.ReadIndexIOStats(ctx)
			results <- indexIOResult{backend: backend, stats: stats, err: err}
		}(alias, client)
	}

	collected := make(map[string]interface{}, len(targets))
	for range targets {
		result := <-results
		if result.err != nil {
			collected[result.backend] = map[string]interface{}{"error": result.err.Error()}
			continue
		}
		metrics.RegisterIndexIOStats(result.backend, result.stats)
		collected[result.backend] = map[string]interface{}{
			"read": result.stats.Read,
			"hit":  result.stats.Hit,
		}
	}
	return collected
}

func (b *Backends) isFinalIndexIOSnapshot() bool {
	if b.vu == nil || b.vu.State() == nil {
		return false
	}
	value, ok := b.vu.State().Tags.GetCurrentValues().Tags.Get(finalSnapshotTag)
	return ok && value == "true"
}

func delayedDuration(duration string) (string, error) {
	parsed, err := time.ParseDuration(duration)
	if err != nil {
		return "", err
	}
	return (parsed + finalSnapshotDelay).String(), nil
}

// AddDockerMetricsCollector adds a metrics_collector scenario to the given
// scenarios object. Pass a Timer or a duration string (e.g. "500s").
// Returns a function that the script should export as collectMetrics:
//
//	export const collectMetrics = backends.addDockerMetricsCollector(scenarios, timer);
func (b *Backends) AddDockerMetricsCollector(call sobek.FunctionCall) sobek.Value {
	rt := b.vu.Runtime()

	if len(call.Arguments) < 2 {
		common.Throw(rt, fmt.Errorf("addDockerMetricsCollector requires (scenarios, timer|duration)"))
		return sobek.Undefined()
	}

	scenarios := call.Arguments[0].ToObject(rt)

	var dur string
	var chartDuration string
	durationArg := call.Arguments[1].Export()
	switch v := durationArg.(type) {
	case *Timer:
		dur = v.TotalDuration()
		chartDuration = v.Duration()
	case string:
		dur = v
	default:
		// Timer comes through as a wrapped Go object — try unwrapping
		if timer, ok := durationArg.(*Timer); ok {
			dur = timer.TotalDuration()
			chartDuration = timer.Duration()
		} else {
			dur = call.Arguments[1].String()
		}
	}

	collectorScenario := map[string]interface{}{
		"executor": "constant-vus",
		"vus":      1,
		"duration": dur,
		"exec":     "collectMetrics",
	}
	if chartDuration != "" {
		collectorScenario["tags"] = map[string]string{
			"chart_duration": chartDuration,
		}
	}

	var finalSnapshotStart string
	if b.hasIndexIOStats() {
		var err error
		finalSnapshotStart, err = delayedDuration(dur)
		if err != nil {
			common.Throw(rt, fmt.Errorf("invalid metrics collector duration %q: %w", dur, err))
			return sobek.Undefined()
		}
	}

	indexIOEnabled, err := b.resetIndexIOStats()
	if err != nil {
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	b.indexIOEnabled = indexIOEnabled

	if err := scenarios.Set("metrics_collector", rt.ToValue(collectorScenario)); err != nil {
		common.Throw(rt, fmt.Errorf("failed to set metrics_collector scenario: %w", err))
	}

	if indexIOEnabled {
		finalizerScenario := map[string]interface{}{
			"executor":   "shared-iterations",
			"vus":        1,
			"iterations": 1,
			"startTime":  finalSnapshotStart,
			"exec":       "collectMetrics",
			"tags": map[string]string{
				finalSnapshotTag: "true",
			},
		}
		if err := scenarios.Set("index_io_finalizer", rt.ToValue(finalizerScenario)); err != nil {
			common.Throw(rt, fmt.Errorf("failed to set index_io_finalizer scenario: %w", err))
		}
	}

	return rt.ToValue(func() map[string]interface{} {
		return b.Collect()
	})
}

// AddRandomDocumentUpdater adds one optional update scenario alongside an
// existing query scenario. The returned function must be exported under the
// supplied exec name. A zero rate or missing source scenario returns a no-op
// without registering state or adding a VU.
func (b *Backends) AddRandomDocumentUpdater(call sobek.FunctionCall) sobek.Value {
	rt := b.vu.Runtime()
	if len(call.Arguments) < 6 {
		common.Throw(rt, fmt.Errorf("addRandomDocumentUpdater requires (scenarios, queryScenario, backend, updatesPerSecond, target, exec)"))
		return sobek.Undefined()
	}

	rateValue := call.Arguments[3].ToFloat()
	if math.IsNaN(rateValue) || math.IsInf(rateValue, 0) || rateValue < 0 || math.Trunc(rateValue) != rateValue || rateValue > float64(maxSafeInteger) {
		common.Throw(rt, fmt.Errorf("updatesPerSecond must be a non-negative safe integer"))
		return sobek.Undefined()
	}
	rate := uint64(rateValue)
	if rate == 0 {
		return randomDocumentUpdaterNoop(rt)
	}

	scenarios := call.Arguments[0].ToObject(rt)
	queryScenario := call.Arguments[1].String()
	sourceValue := scenarios.Get(queryScenario)
	if sourceValue == nil || sobek.IsUndefined(sourceValue) || sobek.IsNull(sourceValue) {
		return randomDocumentUpdaterNoop(rt)
	}
	source := sourceValue.ToObject(rt)
	duration := source.Get("duration").String()
	parsedDuration, err := time.ParseDuration(duration)
	if err != nil || parsedDuration <= 0 {
		common.Throw(rt, fmt.Errorf("query scenario %s has invalid duration %q", queryScenario, duration))
		return sobek.Undefined()
	}

	backend := call.Arguments[2].String()
	client := b.clients[backend]
	if client == nil {
		common.Throw(rt, fmt.Errorf("random document updater references unknown backend %s", backend))
		return sobek.Undefined()
	}
	if !client.RandomDocumentUpdatesEnabled() {
		common.Throw(rt, fmt.Errorf("backend %s does not support random document updates", backend))
		return sobek.Undefined()
	}

	target := call.Arguments[4].String()
	exec := call.Arguments[5].String()
	if target == "" || exec == "" {
		common.Throw(rt, fmt.Errorf("random document update target and exec must not be empty"))
		return sobek.Undefined()
	}
	client.EnableRandomDocumentUpdates(metrics.RegisterUpdateWorkload(backend))

	updaterScenario := map[string]interface{}{
		"executor":        "constant-arrival-rate",
		"rate":            rate,
		"timeUnit":        "1s",
		"duration":        duration,
		"preAllocatedVUs": 1,
		"maxVUs":          1,
		"gracefulStop":    "0s",
		"exec":            exec,
	}
	if startTime := source.Get("startTime"); startTime != nil && !sobek.IsUndefined(startTime) && !sobek.IsNull(startTime) {
		updaterScenario["startTime"] = startTime.String()
	}
	updaterName := queryScenario + "_updates"
	if err := scenarios.Set(updaterName, rt.ToValue(updaterScenario)); err != nil {
		common.Throw(rt, fmt.Errorf("failed to set %s scenario: %w", updaterName, err))
		return sobek.Undefined()
	}

	return rt.ToValue(func() map[string]interface{} {
		return b.updateRandomDocument(backend, target)
	})
}

func randomDocumentUpdaterNoop(rt *sobek.Runtime) sobek.Value {
	return rt.ToValue(func() map[string]interface{} {
		return nil
	})
}

func (b *Backends) updateRandomDocument(backend, target string) map[string]interface{} {
	client := b.clients[backend]
	if client == nil {
		return nil
	}

	count, err := client.AppendSpaceToRandomDocument(target)
	if err != nil {
		fmt.Printf("[%s] random document update error: %v\n", backend, err)
		return map[string]interface{}{"error": err.Error()}
	}
	return map[string]interface{}{"updated": count}
}

// RandomDocumentUpdater returns one validated update callback without adding a
// k6 scenario. Phase-controlled workloads schedule that callback themselves.
func (b *Backends) RandomDocumentUpdater(call sobek.FunctionCall) sobek.Value {
	rt := b.vu.Runtime()
	backend, target, client, ok := b.randomDocumentUpdateTarget(call, "randomDocumentUpdater")
	if !ok {
		return sobek.Undefined()
	}
	client.EnableRandomDocumentUpdates(metrics.RegisterUpdateWorkload(backend))
	return rt.ToValue(func() map[string]interface{} {
		return b.updateRandomDocument(backend, target)
	})
}

// RandomDocumentPrewarmer returns the same datasource mutation without
// measured update metrics or completion accounting.
func (b *Backends) RandomDocumentPrewarmer(call sobek.FunctionCall) sobek.Value {
	rt := b.vu.Runtime()
	backend, target, client, ok := b.randomDocumentUpdateTarget(call, "randomDocumentPrewarmer")
	if !ok {
		return sobek.Undefined()
	}
	return rt.ToValue(func() map[string]interface{} {
		count, err := client.PrewarmAppendSpaceToRandomDocument(target)
		if err != nil {
			common.Throw(rt, fmt.Errorf("prewarm update %s: %w", backend, err))
			return nil
		}
		return map[string]interface{}{"updated": count}
	})
}

func (b *Backends) randomDocumentUpdateTarget(call sobek.FunctionCall, function string) (string, string, *backends.K6Client, bool) {
	rt := b.vu.Runtime()
	if len(call.Arguments) < 2 {
		common.Throw(rt, fmt.Errorf("%s requires (backend, target)", function))
		return "", "", nil, false
	}
	backend := call.Arguments[0].String()
	target := call.Arguments[1].String()
	client := b.clients[backend]
	if client == nil {
		common.Throw(rt, fmt.Errorf("random document updater references unknown backend %s", backend))
		return "", "", nil, false
	}
	if target == "" {
		common.Throw(rt, fmt.Errorf("random document update target must not be empty"))
		return "", "", nil, false
	}
	if !client.RandomDocumentUpdatesEnabled() {
		common.Throw(rt, fmt.Errorf("backend %s does not support random document updates", backend))
		return "", "", nil, false
	}
	return backend, target, client, true
}

// Close closes all backend connections.
// Use this at the end of a test or between groups to clean up connections.
func (b *Backends) Close() {
	for _, client := range b.clients {
		client.Close()
	}
}

// SetTimeout sets the query timeout for all backends in seconds.
// Use 0 to disable timeout (default).
func (b *Backends) SetTimeout(seconds int) {
	for _, client := range b.clients {
		client.SetTimeout(seconds)
	}
}

// readRunningScript locates the currently-running k6 script via os.Args and
// returns its source text plus absolute path. The xk6 extension runs inside
// the k6 process so the script path is reachable from argv directly; k6's
// public extension API doesn't expose it as cleanly. Returns ("", "") if no
// candidate is found.
func readRunningScript() (source, path string) {
	for _, arg := range os.Args[1:] {
		if !looksLikeScript(arg) {
			continue
		}
		abs, err := filepath.Abs(arg)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			continue
		}
		return string(data), abs
	}
	return "", ""
}

func looksLikeScript(arg string) bool {
	switch filepath.Ext(arg) {
	case ".js", ".ts", ".mjs", ".cjs":
		return true
	}
	return false
}

// parseDatasetPath extracts dataset path from config.
// Defaults to "../" (parent of k6 script directory) and resolves relative to script location.
func parseDatasetPath(config map[string]interface{}, vu modules.VU) string {
	datasetPath := "../"
	if dp, ok := config["datasetPath"].(string); ok {
		datasetPath = dp
	}

	// Resolve relative to script location
	if vu != nil {
		if initEnv := vu.InitEnv(); initEnv != nil {
			datasetPath = initEnv.GetAbsFilePath(datasetPath)
		}
	}

	return datasetPath
}

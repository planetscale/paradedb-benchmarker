// Package metrics provides container metrics collection via Docker API.
package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

var (
	// Metrics registered once globally
	containerCPU    *metrics.Metric
	containerMemory *metrics.Metric
	metricsOnce     sync.Once

	// Captured docker-inspect data per container (captured once at first stats collection).
	containerInfo   = make(map[string]map[string]interface{})
	containerInfoMu sync.RWMutex
	infoCapture     = make(map[string]bool)

	// Backend configs (registered by each backend - database settings etc)
	backendConfigs   = make(map[string]map[string]interface{})
	backendConfigsMu sync.RWMutex

	// Backend options (registered once at init - container, alias, color)
	backendOptions   = make(map[string]*BackendOptions)
	backendOptionsMu sync.RWMutex

	// Per-run capture: dataset.yaml contents (if present) and the k6 script source.
	// These are set once during db.backends() init and surfaced verbatim in the
	// dashboard JSON. The frontend renders them as Dataset / Script tabs.
	runCapture   = make(map[string]string)
	runCaptureMu sync.RWMutex

	// Latest PostgreSQL index I/O snapshot per backend alias.
	indexIOStats   = make(map[string]IndexIOStats)
	indexIOStatsMu sync.RWMutex
)

// IndexIOStats is the cumulative PostgreSQL search-index I/O since the most
// recent statistics reset. Read and Hit are formatted by pg_size_pretty().
type IndexIOStats struct {
	ReadBytes int64
	HitBytes  int64
	Read      string
	Hit       string
}

// RegisterIndexIOStats replaces the latest snapshot for a backend alias.
func RegisterIndexIOStats(backend string, stats IndexIOStats) {
	indexIOStatsMu.Lock()
	defer indexIOStatsMu.Unlock()
	indexIOStats[backend] = stats
}

// RegisterInitialIndexIOStats stores a reset baseline without replacing a
// snapshot that a collector has already populated.
func RegisterInitialIndexIOStats(backend string, stats IndexIOStats) {
	indexIOStatsMu.Lock()
	defer indexIOStatsMu.Unlock()
	if _, exists := indexIOStats[backend]; !exists {
		indexIOStats[backend] = stats
	}
}

// GetIndexIOStats returns the latest snapshot for a backend alias.
func GetIndexIOStats(backend string) (IndexIOStats, bool) {
	indexIOStatsMu.RLock()
	defer indexIOStatsMu.RUnlock()
	stats, ok := indexIOStats[backend]
	return stats, ok
}

// RegisterRunCapture stores a run-level text artifact (e.g. "dataset.yaml" or
// "script.js") for later inclusion in the dashboard JSON. Empty values are
// ignored — call with "" to leave the slot unset.
func RegisterRunCapture(key, text string) {
	if text == "" {
		return
	}
	runCaptureMu.Lock()
	defer runCaptureMu.Unlock()
	runCapture[key] = text
}

// GetRunCapture returns the text registered under key, or "" if unset.
func GetRunCapture(key string) string {
	runCaptureMu.RLock()
	defer runCaptureMu.RUnlock()
	return runCapture[key]
}

// BackendOptions holds user-specified options for a backend.
type BackendOptions struct {
	Container string
	Alias     string
	Color     string
}

// RegisterBackendConfig stores a backend's configuration for dashboard display.
// Called by backends when they capture their database config.
func RegisterBackendConfig(backend string, config map[string]interface{}) {
	backendConfigsMu.Lock()
	defer backendConfigsMu.Unlock()
	backendConfigs[backend] = config
}

// GetBackendConfig returns the registered config for a backend.
func GetBackendConfig(backend string) map[string]interface{} {
	backendConfigsMu.RLock()
	defer backendConfigsMu.RUnlock()
	config := backendConfigs[backend]
	if config == nil {
		return nil
	}
	result := make(map[string]interface{}, len(config))
	for k, v := range config {
		result[k] = v
	}
	return result
}

// GetContainerInfo returns the captured docker-inspect subset for a container.
// The returned map contains keys like image, image_id, cmd, env, host_config,
// mounts, labels, network_mode, ports, plus display-friendly cpu_limit and
// memory_limit strings.
func GetContainerInfo(container string) map[string]interface{} {
	containerInfoMu.RLock()
	defer containerInfoMu.RUnlock()

	info := containerInfo[container]
	if info == nil {
		return nil
	}

	result := make(map[string]interface{}, len(info))
	for k, v := range info {
		result[k] = v
	}
	return result
}

// GetContainerLimits returns just the CPU/memory limit strings from the captured
// container info. Retained for legacy callers; new code should use GetContainerInfo.
func GetContainerLimits(container string) map[string]interface{} {
	info := GetContainerInfo(container)
	if info == nil {
		return nil
	}
	limits := make(map[string]interface{})
	if v, ok := info["cpu_limit"]; ok {
		limits["cpu_limit"] = v
	}
	if v, ok := info["memory_limit"]; ok {
		limits["memory_limit"] = v
	}
	if len(limits) == 0 {
		return nil
	}
	return limits
}

// CapturePrePostScripts reads pre/post scripts from the dataset directory
// and adds them to the backend's config. The alias is used as the config key,
// while backendType is used for the directory name. The fileType should be "sql" or "json".
func CapturePrePostScripts(alias, backendType, datasetPath, fileType string) {
	if datasetPath == "" {
		return
	}

	backendConfigsMu.Lock()
	defer backendConfigsMu.Unlock()

	config := backendConfigs[alias]
	if config == nil {
		config = make(map[string]interface{})
		backendConfigs[alias] = config
	}

	preFile := filepath.Join(datasetPath, backendType, "pre."+fileType)
	if data, err := os.ReadFile(preFile); err == nil {
		config["pre_script"] = string(data)
	}

	postFile := filepath.Join(datasetPath, backendType, "post."+fileType)
	if data, err := os.ReadFile(postFile); err == nil {
		config["post_script"] = string(data)
	}
}

// RegisterBackendOptions stores user-specified options for a backend.
// Called once at init time from backends.go.
func RegisterBackendOptions(backend string, opts *BackendOptions) {
	backendOptionsMu.Lock()
	defer backendOptionsMu.Unlock()
	backendOptions[backend] = opts
}

// GetBackendOptions returns the registered options for a backend.
func GetBackendOptions(backend string) *BackendOptions {
	backendOptionsMu.RLock()
	defer backendOptionsMu.RUnlock()
	return backendOptions[backend]
}

// GetAllBackendOptions returns all registered backend options.
func GetAllBackendOptions() map[string]*BackendOptions {
	backendOptionsMu.RLock()
	defer backendOptionsMu.RUnlock()
	// Return a copy to avoid race conditions
	result := make(map[string]*BackendOptions)
	for k, v := range backendOptions {
		result[k] = v
	}
	return result
}

// Collector collects container metrics via Docker API.
type Collector struct {
	vu         modules.VU
	containers []string

	// HTTP client for Docker API
	httpClient *http.Client

	// Previous stats for CPU delta calculation (shared across calls)
	prevStats map[string]*rawDockerStats
	statsMu   sync.Mutex
	paused    map[string]bool
	stopped   map[string]bool
}

// ContainerStats holds calculated container stats.
type ContainerStats struct {
	CPUPercent  float64
	MemoryBytes float64
}

// rawDockerStats holds raw Docker API stats for delta calculation.
type rawDockerStats struct {
	CPUTotal    uint64
	SystemCPU   uint64
	OnlineCPUs  int
	MemoryUsage uint64
	MemoryCache uint64
}

// dockerStats matches the Docker API stats response (partial).
type dockerStats struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     int    `json:"online_cpus"`
	} `json:"cpu_stats"`
	MemoryStats struct {
		Usage uint64 `json:"usage"`
		Stats struct {
			Cache uint64 `json:"cache"`
		} `json:"stats"`
	} `json:"memory_stats"`
}

type dockerCLIConfig struct {
	CurrentContext string `json:"currentContext"`
}

type dockerContextMetadata struct {
	Endpoints map[string]struct {
		Host string `json:"Host"`
	} `json:"Endpoints"`
}

func dockerConfigDir() string {
	if dir := os.Getenv("DOCKER_CONFIG"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".docker")
}

func unixSocketPath(host string) string {
	const prefix = "unix://"
	if !strings.HasPrefix(host, prefix) {
		return ""
	}
	return strings.TrimPrefix(host, prefix)
}

func contextDockerSocket(configDir, contextName string) string {
	if configDir == "" || contextName == "" || contextName == "default" {
		return ""
	}
	hash := sha256.Sum256([]byte(contextName))
	metaPath := filepath.Join(configDir, "contexts", "meta", fmt.Sprintf("%x", hash), "meta.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return ""
	}
	var metadata dockerContextMetadata
	if json.Unmarshal(data, &metadata) != nil {
		return ""
	}
	return unixSocketPath(metadata.Endpoints["docker"].Host)
}

func resolveDockerSocket() string {
	configDir := dockerConfigDir()
	if contextName := os.Getenv("DOCKER_CONTEXT"); contextName != "" {
		if socket := contextDockerSocket(configDir, contextName); socket != "" {
			return socket
		}
	}
	if socket := unixSocketPath(os.Getenv("DOCKER_HOST")); socket != "" {
		return socket
	}

	data, err := os.ReadFile(filepath.Join(configDir, "config.json"))
	if err == nil {
		var config dockerCLIConfig
		if json.Unmarshal(data, &config) == nil {
			if socket := contextDockerSocket(configDir, config.CurrentContext); socket != "" {
				return socket
			}
		}
	}
	return "/var/run/docker.sock"
}

// NewCollector creates a new metrics collector.
func NewCollector(vu modules.VU, config map[string]interface{}) *Collector {
	// Register metrics once during init phase.
	if vu != nil && vu.InitEnv() != nil {
		metricsOnce.Do(func() {
			registry := vu.InitEnv().Registry
			containerCPU, _ = registry.NewMetric("container_cpu_percent", metrics.Gauge)
			containerMemory, _ = registry.NewMetric("container_memory_bytes", metrics.Gauge)
		})
	}

	var containers []string
	if c, ok := config["containers"].([]interface{}); ok {
		for _, name := range c {
			if s, ok := name.(string); ok {
				containers = append(containers, s)
			}
		}
	}

	// Create HTTP client for the active Docker context's socket with short timeout.
	dockerSocket := resolveDockerSocket()
	dialer := &net.Dialer{}
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", dockerSocket)
			},
		},
		Timeout: 2 * time.Second,
	}

	return &Collector{
		vu:         vu,
		containers: containers,
		prevStats:  make(map[string]*rawDockerStats),
		paused:     make(map[string]bool),
		stopped:    make(map[string]bool),
		httpClient: httpClient,
	}
}

// Start is a no-op for compatibility (collection happens in Collect).
func (c *Collector) Start() map[string]interface{} {
	return map[string]interface{}{"status": "ready"}
}

// Stop is a no-op for compatibility.
func (c *Collector) Stop() map[string]interface{} {
	return map[string]interface{}{"status": "stopped"}
}

// PauseContainer freezes a container after its benchmark phase so background
// work cannot interfere with the next backend.
func (c *Collector) PauseContainer(container string) error {
	if err := c.setContainerPaused(container, true); err != nil {
		return err
	}
	c.statsMu.Lock()
	if c.paused == nil {
		c.paused = make(map[string]bool)
	}
	c.paused[container] = true
	c.statsMu.Unlock()
	return nil
}

// UnpauseContainer resumes a container previously frozen by PauseContainer.
func (c *Collector) UnpauseContainer(container string) error {
	if err := c.setContainerPaused(container, false); err != nil {
		return err
	}
	c.statsMu.Lock()
	delete(c.paused, container)
	c.statsMu.Unlock()
	return nil
}

// StopContainer gracefully stops a completed benchmark container and removes
// it from subsequent metric collection. Docker honors the image's configured
// stop signal; PostgreSQL images use SIGINT for a fast, clean shutdown.
func (c *Collector) StopContainer(container string) error {
	endpoint := fmt.Sprintf("http://localhost/containers/%s/stop?t=600", url.PathEscape(container))
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	controlClient := *c.httpClient
	controlClient.Timeout = 610 * time.Second
	resp, err := controlClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("docker stop %s failed with status %d: %s", container, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	c.statsMu.Lock()
	if c.stopped == nil {
		c.stopped = make(map[string]bool)
	}
	c.stopped[container] = true
	c.statsMu.Unlock()
	return nil
}

func (c *Collector) setContainerPaused(container string, paused bool) error {
	action := "unpause"
	if paused {
		action = "pause"
	}
	endpoint := fmt.Sprintf("http://localhost/containers/%s/%s", url.PathEscape(container), action)
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	controlClient := *c.httpClient
	controlClient.Timeout = 30 * time.Second
	resp, err := controlClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("docker %s %s failed with status %d: %s", action, container, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// containerResult holds the result of fetching stats for a single container.
type containerResult struct {
	container string
	stats     *ContainerStats
	err       error
}

// Collect fetches current stats and pushes them to k6 metrics.
// Call this from JavaScript in a loop with sleep.
func (c *Collector) Collect() map[string]interface{} {
	state := c.vu.State()
	if state == nil || containerCPU == nil || containerMemory == nil {
		return map[string]interface{}{"error": "not in VU context"}
	}

	ctx := c.vu.Context()
	if ctx == nil {
		return map[string]interface{}{"error": "no context"}
	}

	now := time.Now()
	baseTags := state.Tags.GetCurrentValues()
	results := make(map[string]interface{})
	containers := c.activeContainers()

	// Capture docker inspect info once per container (in parallel).
	var infoWg sync.WaitGroup
	for _, container := range containers {
		containerInfoMu.Lock()
		needsCapture := !infoCapture[container]
		containerInfoMu.Unlock()

		if needsCapture {
			infoWg.Add(1)
			go func(cont string) {
				defer infoWg.Done()
				if c.captureContainerInfo(cont) {
					containerInfoMu.Lock()
					infoCapture[cont] = true
					containerInfoMu.Unlock()
				}
			}(container)
		}
	}
	infoWg.Wait()

	// Fetch stats for all containers in parallel
	resultChan := make(chan containerResult, len(containers))
	for _, container := range containers {
		go func(cont string) {
			stats, err := c.fetchAndCalculateStats(cont)
			resultChan <- containerResult{container: cont, stats: stats, err: err}
		}(container)
	}

	// Collect results
	for range containers {
		res := <-resultChan
		if res.err != nil {
			results[res.container] = map[string]interface{}{"error": res.err.Error()}
			continue
		}

		// Push to k6 metrics
		tags := baseTags.Tags.With("container", res.container)

		metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: containerCPU, Tags: tags},
			Time:       now,
			Value:      res.stats.CPUPercent,
		})
		metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
			TimeSeries: metrics.TimeSeries{Metric: containerMemory, Tags: tags},
			Time:       now,
			Value:      res.stats.MemoryBytes,
		})

		results[res.container] = map[string]interface{}{
			"cpu":    res.stats.CPUPercent,
			"memory": res.stats.MemoryBytes,
		}
	}

	return results
}

func (c *Collector) activeContainers() []string {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	active := make([]string, 0, len(c.containers))
	for _, container := range c.containers {
		if !c.paused[container] && !c.stopped[container] {
			active = append(active, container)
		}
	}
	return active
}

// fetchAndCalculateStats gets raw stats from Docker API and calculates CPU percentage.
func (c *Collector) fetchAndCalculateStats(container string) (*ContainerStats, error) {
	url := fmt.Sprintf("http://localhost/containers/%s/stats?stream=true", container)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("docker API error %d: %s", resp.StatusCode, string(body))
	}

	decoder := json.NewDecoder(resp.Body)
	var stats dockerStats
	if err := decoder.Decode(&stats); err != nil {
		return nil, err
	}

	current := &rawDockerStats{
		CPUTotal:    stats.CPUStats.CPUUsage.TotalUsage,
		SystemCPU:   stats.CPUStats.SystemCPUUsage,
		OnlineCPUs:  stats.CPUStats.OnlineCPUs,
		MemoryUsage: stats.MemoryStats.Usage,
		MemoryCache: stats.MemoryStats.Stats.Cache,
	}

	c.statsMu.Lock()
	prev := c.prevStats[container]
	c.prevStats[container] = current
	c.statsMu.Unlock()

	cpuPercent := 0.0
	if prev != nil && current.SystemCPU > prev.SystemCPU {
		cpuDelta := float64(current.CPUTotal - prev.CPUTotal)
		systemDelta := float64(current.SystemCPU - prev.SystemCPU)
		if systemDelta > 0 && cpuDelta > 0 {
			cpuPercent = (cpuDelta / systemDelta) * float64(current.OnlineCPUs) * 100.0
		}
	}

	memoryBytes := float64(current.MemoryUsage)
	if current.MemoryCache > 0 && current.MemoryUsage > current.MemoryCache {
		memoryBytes = float64(current.MemoryUsage - current.MemoryCache)
	}

	return &ContainerStats{
		CPUPercent:  cpuPercent,
		MemoryBytes: memoryBytes,
	}, nil
}

// dockerInspect matches the Docker API inspect response (partial).
// Only the subset needed for the dashboard's "Container" tab is decoded.
type dockerInspect struct {
	Image  string `json:"Image"` // image SHA, e.g. "sha256:abc..."
	Config struct {
		Image string   `json:"Image"` // image tag, e.g. "paradedb/paradedb:v0.25.2"
		Cmd   []string `json:"Cmd"`
		Env   []string `json:"Env"`
	} `json:"Config"`
	HostConfig struct {
		NanoCPUs    int64    `json:"NanoCpus"`
		Memory      int64    `json:"Memory"`
		CPUQuota    int64    `json:"CpuQuota"`
		CapAdd      []string `json:"CapAdd"`
		SecurityOpt []string `json:"SecurityOpt"`
	} `json:"HostConfig"`
}

// captureContainerInfo fetches the docker inspect output and stores a curated
// subset for the dashboard. Returns true on success.
func (c *Collector) captureContainerInfo(container string) bool {
	url := fmt.Sprintf("http://localhost/containers/%s/json", container)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var inspect dockerInspect
	if err := json.NewDecoder(resp.Body).Decode(&inspect); err != nil {
		return false
	}

	info := map[string]interface{}{
		"image":        inspect.Config.Image,
		"image_id":     inspect.Image,
		"cmd":          inspect.Config.Cmd,
		"env":          inspect.Config.Env,
		"cap_add":      inspect.HostConfig.CapAdd,
		"security_opt": inspect.HostConfig.SecurityOpt,
	}

	// Display-friendly limit strings for the existing UI.
	if inspect.HostConfig.NanoCPUs > 0 {
		cpuLimit := float64(inspect.HostConfig.NanoCPUs) / 1e9
		info["cpu_limit"] = fmt.Sprintf("%.1f cores", cpuLimit)
	} else if inspect.HostConfig.CPUQuota > 0 {
		cpuLimit := float64(inspect.HostConfig.CPUQuota) / 100000
		info["cpu_limit"] = fmt.Sprintf("%.1f cores", cpuLimit)
	}
	if inspect.HostConfig.Memory > 0 {
		memGB := float64(inspect.HostConfig.Memory) / (1024 * 1024 * 1024)
		info["memory_limit"] = fmt.Sprintf("%.1f GB", memGB)
	}

	containerInfoMu.Lock()
	containerInfo[container] = info
	containerInfoMu.Unlock()
	return true
}

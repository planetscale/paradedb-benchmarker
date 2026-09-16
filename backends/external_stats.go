package backends

import (
	"context"
	"fmt"
	"sync"
)

type externalIndexIOKey struct {
	identity string
	backend  string
}

type externalIndexIOBaseline struct {
	mu    sync.Mutex
	stats IndexIOStats
}

// Query VUs and the collector have separate clients. Share the measured phase's
// baseline across them without resetting the remote database's counters.
var externalIndexIOBaselines sync.Map

func (c *K6Client) externalIndexIOBaseline(provider IndexIOStatsProvider) *externalIndexIOBaseline {
	key := externalIndexIOKey{identity: provider.IndexIOStatsIdentity(), backend: c.backend}
	value, _ := externalIndexIOBaselines.LoadOrStore(key, &externalIndexIOBaseline{})
	return value.(*externalIndexIOBaseline)
}

func (c *K6Client) resetIndexIOStats(ctx context.Context, provider IndexIOStatsProvider) error {
	if !c.external {
		return provider.ResetIndexIOStats(ctx)
	}
	baseline := c.externalIndexIOBaseline(provider)
	baseline.mu.Lock()
	defer baseline.mu.Unlock()
	stats, err := provider.ReadIndexIOStats(ctx)
	if err == nil {
		baseline.stats = stats
	}
	return err
}

func (c *K6Client) readExternalIndexIOStats(ctx context.Context, provider IndexIOStatsProvider) (IndexIOStats, error) {
	baseline := c.externalIndexIOBaseline(provider)
	baseline.mu.Lock()
	defer baseline.mu.Unlock()
	stats, err := provider.ReadIndexIOStats(ctx)
	if err != nil {
		return IndexIOStats{}, err
	}
	if stats.ReadBytes < baseline.stats.ReadBytes || stats.HitBytes < baseline.stats.HitBytes {
		return IndexIOStats{}, fmt.Errorf("remote index I/O counters moved backwards during the benchmark")
	}
	stats.ReadBytes -= baseline.stats.ReadBytes
	stats.HitBytes -= baseline.stats.HitBytes
	stats.Read = formatIndexIOBytes(stats.ReadBytes)
	stats.Hit = formatIndexIOBytes(stats.HitBytes)
	return stats, nil
}

func formatIndexIOBytes(bytes int64) string {
	if bytes == 0 {
		return "0B"
	}
	value := float64(bytes)
	units := [...]string{"B", "kB", "MB", "GB", "TB", "PB"}
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	return fmt.Sprintf("%.0f %s", value, units[unit])
}

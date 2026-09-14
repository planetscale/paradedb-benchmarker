package metrics

import "sync"

// WALStats is the PostgreSQL WAL generated since the current backend phase's
// baseline was captured.
type WALStats struct {
	Bytes            uint64
	CompletedUpdates uint64
}

type walTracker struct {
	baselinePosition uint64
	currentPosition  uint64
	baselineUpdates  uint64
	currentUpdates   uint64
}

var (
	walStats   = make(map[string]walTracker)
	walStatsMu sync.RWMutex
)

// ResetWALStats starts a new phase-scoped WAL measurement at position and its
// paired completed-update count.
func ResetWALStats(backend string, position, completedUpdates uint64) {
	walStatsMu.Lock()
	defer walStatsMu.Unlock()
	walStats[backend] = walTracker{
		baselinePosition: position,
		currentPosition:  position,
		baselineUpdates:  completedUpdates,
		currentUpdates:   completedUpdates,
	}
}

// ClearWALStats removes a backend's baseline and latest snapshot.
func ClearWALStats(backend string) {
	walStatsMu.Lock()
	defer walStatsMu.Unlock()
	delete(walStats, backend)
}

// RegisterWALPosition advances an existing backend's paired WAL position and
// update count. It rejects missing baselines or counters that move backwards.
func RegisterWALPosition(backend string, position, completedUpdates uint64) bool {
	walStatsMu.Lock()
	defer walStatsMu.Unlock()
	tracker, ok := walStats[backend]
	if !ok ||
		position < tracker.baselinePosition ||
		position < tracker.currentPosition ||
		completedUpdates < tracker.baselineUpdates ||
		completedUpdates < tracker.currentUpdates {
		return false
	}
	tracker.currentPosition = position
	tracker.currentUpdates = completedUpdates
	walStats[backend] = tracker
	return true
}

// GetWALStats returns the latest phase-scoped WAL byte delta for backend.
func GetWALStats(backend string) (WALStats, bool) {
	walStatsMu.RLock()
	defer walStatsMu.RUnlock()
	tracker, ok := walStats[backend]
	if !ok {
		return WALStats{}, false
	}
	return WALStats{
		Bytes:            tracker.currentPosition - tracker.baselinePosition,
		CompletedUpdates: tracker.currentUpdates - tracker.baselineUpdates,
	}, true
}

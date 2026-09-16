package metrics

import (
	"sync"
	"sync/atomic"
)

// UpdateWorkloadStats is a point-in-time snapshot of one backend's optional
// paced update workload.
type UpdateWorkloadStats struct {
	CompletedUpdates uint64
}

// UpdateWorkload is the process-wide completed-update counter shared by every
// k6 VU for one backend alias.
type UpdateWorkload struct {
	completedUpdates atomic.Uint64
}

var updateWorkloads sync.Map

// RegisterUpdateWorkload enables update completion accounting for a backend alias
// and returns the process-wide state shared by all VUs. Registration is
// idempotent because k6 evaluates init code once per VU.
func RegisterUpdateWorkload(backend string) *UpdateWorkload {
	if backend == "" {
		return nil
	}
	state, _ := updateWorkloads.LoadOrStore(backend, &UpdateWorkload{})
	return state.(*UpdateWorkload)
}

// RecordCompletedUpdate records one datasource-confirmed document update.
func (w *UpdateWorkload) RecordCompletedUpdate() {
	if w != nil {
		w.completedUpdates.Add(1)
	}
}

// Stats returns a point-in-time snapshot of this workload.
func (w *UpdateWorkload) Stats() UpdateWorkloadStats {
	if w == nil {
		return UpdateWorkloadStats{}
	}
	return UpdateWorkloadStats{
		CompletedUpdates: w.completedUpdates.Load(),
	}
}

// GetUpdateWorkloadStats snapshots an enabled backend's counters.
func GetUpdateWorkloadStats(backend string) (UpdateWorkloadStats, bool) {
	state, ok := updateWorkloads.Load(backend)
	if !ok {
		return UpdateWorkloadStats{}, false
	}
	return state.(*UpdateWorkload).Stats(), true
}

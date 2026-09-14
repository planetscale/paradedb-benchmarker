package search

import (
	"testing"

	"github.com/paradedb/benchmarker/backends"
	"github.com/paradedb/benchmarker/metrics"
)

func TestExternalPhaseCompletesWithoutDockerAndStopsPolling(t *testing.T) {
	d := &collectingIndexIODriver{identity: t.Name()}
	c := backends.NewK6Client(nil, d, t.Name())
	c.SetExternal(true)
	b := &Backends{
		clients:        map[string]*backends.K6Client{t.Name(): c},
		indexIOEnabled: true,
	}
	if got := b.collectIndexIOStats(true); len(got) != 1 {
		t.Fatalf("initial collection = %v", got)
	}
	// A nonzero cooldown must neither run host sync nor sleep for remote phases.
	b.PhaseBoundary(t.Name(), "1h")
	if got := b.collectIndexIOStats(true); len(got) != 0 || d.reads.Load() != 1 {
		t.Fatalf("completed remote backend still polled: %v", got)
	}
	// A mixed run may have a Docker collector; remote completion must not use it.
	b.Metrics = &metrics.Collector{}
	b.clients["second"] = c
	b.PhaseBoundary("second", "1h")
	for _, alias := range []string{t.Name(), "unknown"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("phase boundary accepted duplicate or unknown alias %q", alias)
				}
			}()
			b.PhaseBoundary(alias, "0s")
		}()
	}
}

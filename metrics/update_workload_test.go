package metrics

import (
	"sync"
	"testing"
)

func TestUpdateWorkloadCountsCompletedUpdates(t *testing.T) {
	workload := RegisterUpdateWorkload(t.Name())
	workload.RecordCompletedUpdate()
	workload.RecordCompletedUpdate()

	stats, enabled := GetUpdateWorkloadStats(t.Name())
	if !enabled {
		t.Fatal("registered workload is not visible")
	}
	if stats.CompletedUpdates != 2 {
		t.Fatalf("completed updates = %d, want 2", stats.CompletedUpdates)
	}
}

func TestUpdateWorkloadConcurrentCompletionAccounting(t *testing.T) {
	workload := RegisterUpdateWorkload(t.Name())
	const goroutines = 8
	const updatesPerGoroutine = 1000
	var group sync.WaitGroup
	group.Add(goroutines)
	for range goroutines {
		go func() {
			defer group.Done()
			for range updatesPerGoroutine {
				workload.RecordCompletedUpdate()
			}
		}()
	}
	group.Wait()

	if got := workload.Stats().CompletedUpdates; got != goroutines*updatesPerGoroutine {
		t.Fatalf("completed updates = %d, want %d", got, goroutines*updatesPerGoroutine)
	}
}

func TestMissingUpdateWorkloadIsDisabled(t *testing.T) {
	if _, enabled := GetUpdateWorkloadStats("missing-" + t.Name()); enabled {
		t.Fatal("unregistered backend unexpectedly enabled updates")
	}
}

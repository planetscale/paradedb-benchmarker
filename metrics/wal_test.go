package metrics

import "testing"

func TestWALStatsTracksPhaseScopedDelta(t *testing.T) {
	backend := "wal-delta-" + t.Name()
	ResetWALStats(backend, 1000, 10)
	if !RegisterWALPosition(backend, 1300, 12) {
		t.Fatal("valid WAL position was rejected")
	}

	stats, ok := GetWALStats(backend)
	if !ok || stats.Bytes != 300 || stats.CompletedUpdates != 2 {
		t.Fatalf("WAL stats = %#v, %v; want 300 bytes and 2 updates", stats, ok)
	}

	ResetWALStats(backend, 2000, 12)
	stats, ok = GetWALStats(backend)
	if !ok || stats.Bytes != 0 || stats.CompletedUpdates != 0 {
		t.Fatalf("reset WAL stats = %#v, %v; want zero delta", stats, ok)
	}
}

func TestWALStatsRejectsMissingOrBackwardPositions(t *testing.T) {
	missing := "wal-missing-" + t.Name()
	if RegisterWALPosition(missing, 100, 1) {
		t.Fatal("position without a baseline was accepted")
	}

	backend := "wal-backward-" + t.Name()
	ResetWALStats(backend, 100, 10)
	if !RegisterWALPosition(backend, 150, 11) {
		t.Fatal("forward position was rejected")
	}
	if RegisterWALPosition(backend, 140, 12) {
		t.Fatal("backward position was accepted")
	}
	if RegisterWALPosition(backend, 160, 10) {
		t.Fatal("backward update count was accepted")
	}
	stats, _ := GetWALStats(backend)
	if stats.Bytes != 50 {
		t.Fatalf("backward position replaced last valid delta: %#v", stats)
	}
}

func TestWALStatsReportsMissingBackend(t *testing.T) {
	if _, ok := GetWALStats("wal-never-registered-" + t.Name()); ok {
		t.Fatal("missing backend returned WAL stats")
	}
}

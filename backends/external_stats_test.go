package backends

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestExternalIndexIOBaselineIsSharedAcrossVUsAndSurvivesLateInitialization(t *testing.T) {
	ctx := context.Background()
	var resets atomic.Int32
	newClient := func(read, hit int64) (*K6Client, *indexIORecordingDriver) {
		d := &indexIORecordingDriver{
			enabled: true, identity: t.Name(), resetCalls: &resets,
			stats: IndexIOStats{ReadBytes: read, HitBytes: hit},
		}
		c := NewK6Client(nil, d, t.Name())
		c.SetExternal(true)
		if _, err := c.ResetIndexIOStats(ctx); err != nil {
			t.Fatal(err)
		}
		return c, d
	}
	leader, leaderDriver := newClient(8192, 16384)
	collector, collectorDriver := newClient(16384, 32768)
	assertDelta := func(c *K6Client, read, hit int64) {
		t.Helper()
		stats, supported, err := c.ReadIndexIOStats(ctx)
		if err != nil || !supported || stats.ReadBytes != read || stats.HitBytes != hit {
			t.Fatalf("delta = %+v, supported=%v, err=%v; want %d/%d", stats, supported, err, read, hit)
		}
		if stats.Read == "" || stats.Hit == "" {
			t.Fatal("missing display values")
		}
	}
	assertDelta(collector, 8192, 16384)

	// The query leader rebases after prewarming; the collector sees that baseline.
	leaderDriver.stats = IndexIOStats{ReadBytes: 24576, HitBytes: 49152}
	if _, err := leader.ResetIndexIOStatsNow(ctx); err != nil {
		t.Fatal(err)
	}
	collectorDriver.stats = IndexIOStats{ReadBytes: 32768, HitBytes: 65536}
	assertDelta(collector, 8192, 16384)
	late, _ := newClient(40960, 81920)
	assertDelta(late, 16384, 32768)
	assertDelta(collector, 8192, 16384)
	if resets.Load() != 0 {
		t.Fatal("external clients reset database statistics")
	}

	collectorDriver.stats = IndexIOStats{ReadBytes: 0, HitBytes: 0}
	if _, _, err := collector.ReadIndexIOStats(ctx); err == nil {
		t.Fatal("an external statistics reset should invalidate the delta")
	}
}

type externalDiagnosticsDriver struct {
	recordingDriver
	resets int
}

func (d *externalDiagnosticsDriver) ResetPostgresDiagnostics(context.Context) error {
	d.resets++
	return nil
}

func (*externalDiagnosticsDriver) ReadPostgresDiagnostics(context.Context) (PostgresDiagnosticsSample, error) {
	return PostgresDiagnosticsSample{}, nil
}

func TestExternalDiagnosticsNeverResetServerCounters(t *testing.T) {
	d := &externalDiagnosticsDriver{}
	c := NewK6Client(nil, d, t.Name())
	c.SetExternal(true)
	if supported, err := c.ResetPostgresDiagnostics(context.Background()); !supported || err != nil {
		t.Fatalf("external reset: supported=%v, err=%v", supported, err)
	}
	if _, supported, err := c.ReadPostgresDiagnostics(context.Background()); !supported || err != nil {
		t.Fatalf("external snapshot: supported=%v, err=%v", supported, err)
	}
	if d.resets != 0 {
		t.Fatal("external diagnostics reset server counters")
	}
	c.SetExternal(false)
	if _, err := c.ResetPostgresDiagnostics(context.Background()); err != nil || d.resets != 1 {
		t.Fatalf("managed reset changed: resets=%d, err=%v", d.resets, err)
	}
}

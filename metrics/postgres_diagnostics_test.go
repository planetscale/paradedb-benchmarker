package metrics

import "testing"

func TestPostgresDiagnosticsRegistryOwnsStoredSamples(t *testing.T) {
	backend := "postgres-diagnostics-" + t.Name()
	ResetPostgresDiagnostics(backend)
	sample := PostgresDiagnosticsSample{
		Time: 42,
		IO: []PostgresIODiagnostics{{
			BackendType: "client backend",
			ReadBytes:   8192,
		}},
		IndexBytes: map[string]int64{"idx": 16384},
		Segments:   []PostgresSegmentDiagnostics{{Index: "idx", Documents: 7}},
	}
	RegisterPostgresDiagnostics(backend, sample)

	sample.IO[0].ReadBytes = 1
	sample.IndexBytes["idx"] = 2
	sample.Segments[0].Documents = 3
	got := GetPostgresDiagnostics()[backend]
	if len(got) != 1 || got[0].IO[0].ReadBytes != 8192 || got[0].IndexBytes["idx"] != 16384 || got[0].Segments[0].Documents != 7 {
		t.Fatalf("stored diagnostics changed through caller-owned data: %#v", got)
	}

	got[0].IO[0].ReadBytes = 4
	if reread := GetPostgresDiagnostics()[backend][0].IO[0].ReadBytes; reread != 8192 {
		t.Fatalf("stored diagnostics changed through returned data: %d", reread)
	}
	ResetPostgresDiagnostics(backend)
	if _, exists := GetPostgresDiagnostics()[backend]; exists {
		t.Fatal("reset retained backend diagnostics")
	}
}

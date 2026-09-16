package dashboard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.k6.io/k6/output"
)

func TestParseOutputModes(t *testing.T) {
	cases := []struct {
		in                           string
		wantLive, wantJSON, wantHTML bool
		wantErr                      bool
	}{
		{"", true, false, false, false},
		{"live", true, false, false, false},
		{"json", false, true, false, false},
		{"html", false, false, true, false},
		{"live,html", true, false, true, false},
		{"live,json,html", true, true, true, false},
		{" html , json ", false, true, true, false},
		{"foo", false, false, false, true},
		{"live,foo", false, false, false, true},
	}
	for _, c := range cases {
		live, gotJSON, gotHTML, err := parseOutputModes(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseOutputModes(%q) err=%v, wantErr=%v", c.in, err, c.wantErr)
			continue
		}
		if c.wantErr {
			continue
		}
		if live != c.wantLive || gotJSON != c.wantJSON || gotHTML != c.wantHTML {
			t.Errorf("parseOutputModes(%q) = (live=%v, json=%v, html=%v); want (live=%v, json=%v, html=%v)",
				c.in, live, gotJSON, gotHTML, c.wantLive, c.wantJSON, c.wantHTML)
		}
	}
}

func TestNewRejectsUnknownMode(t *testing.T) {
	if _, err := New(output.Params{ConfigArgument: "live,bogus"}); err == nil {
		t.Fatalf("expected error for unknown mode, got nil")
	}
}

func TestDashboardExportPrefixDefaultsToDashboard(t *testing.T) {
	t.Setenv("DASHBOARD_EXPORT_PREFIX", "")
	prefix, err := dashboardExportPrefix()
	if err != nil {
		t.Fatalf("dashboardExportPrefix: %v", err)
	}
	if prefix != "dashboard" {
		t.Fatalf("dashboardExportPrefix = %q, want dashboard", prefix)
	}
}

func TestNewRejectsUnsafeExportPrefix(t *testing.T) {
	t.Setenv("DASHBOARD_EXPORT_PREFIX", "../results")
	if _, err := New(output.Params{ConfigArgument: "json"}); err == nil {
		t.Fatalf("expected error for unsafe export prefix, got nil")
	}
}

func TestNewCreatesExportDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "exports")
	t.Setenv("DASHBOARD_EXPORT_DIR", dir)

	if _, err := New(output.Params{ConfigArgument: "json"}); err != nil {
		t.Fatalf("New: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("export directory = %#v, %v; want directory", info, err)
	}
}

func TestNewRejectsUnusableExportDirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	t.Setenv("DASHBOARD_EXPORT_DIR", filepath.Join(file, "exports"))

	if _, err := New(output.Params{ConfigArgument: "json"}); err == nil {
		t.Fatal("expected unusable export directory error")
	}
}

// TestStartStopWritesRequestedExportFiles drives the full Start/Stop lifecycle
// through New() so we exercise the same code path k6 does, minus the http
// server (live mode disabled so no port binding).
func TestStartStopWritesRequestedExportFiles(t *testing.T) {
	cases := []struct {
		arg      string
		wantJSON bool
		wantHTML bool
	}{
		{"json", true, false},
		{"html", false, true},
		{"json,html", true, true},
	}
	for _, c := range cases {
		t.Run(c.arg, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "out")
			t.Setenv("DASHBOARD_EXPORT_DIR", dir)
			t.Setenv("DASHBOARD_EXPORT_PREFIX", "count_2-terms")

			out, err := New(output.Params{ConfigArgument: c.arg})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if err := out.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if err := out.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			var hasJSON, hasHTML bool
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir: %v", err)
			}
			for _, e := range entries {
				name := e.Name()
				if !strings.HasPrefix(name, "count_2-terms_") {
					continue
				}
				if strings.HasSuffix(name, ".json") {
					hasJSON = true
				}
				if strings.HasSuffix(name, ".html") {
					hasHTML = true
					content, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil {
						t.Fatalf("read html: %v", err)
					}
					if !strings.Contains(string(content), "__DASHBOARD_EMBEDDED_DATA") {
						t.Errorf("HTML export missing embedded data marker")
					}
				}
			}
			if hasJSON != c.wantJSON {
				t.Errorf("JSON file present=%v, want=%v", hasJSON, c.wantJSON)
			}
			if hasHTML != c.wantHTML {
				t.Errorf("HTML file present=%v, want=%v", hasHTML, c.wantHTML)
			}
		})
	}
}

package metrics

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func writeDockerContext(t *testing.T, configDir, name, socket string) {
	t.Helper()
	hash := sha256.Sum256([]byte(name))
	dir := filepath.Join(configDir, "contexts", "meta", fmt.Sprintf("%x", hash))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create context directory: %v", err)
	}
	metadata := fmt.Sprintf(`{"Name":%q,"Endpoints":{"docker":{"Host":%q}}}`, name, "unix://"+socket)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(metadata), 0o644); err != nil {
		t.Fatalf("write context metadata: %v", err)
	}
}

func TestResolveDockerSocketUsesDockerContext(t *testing.T) {
	configDir := t.TempDir()
	writeDockerContext(t, configDir, "colima", "/tmp/colima.sock")
	t.Setenv("DOCKER_CONFIG", configDir)
	t.Setenv("DOCKER_CONTEXT", "colima")
	t.Setenv("DOCKER_HOST", "unix:///tmp/ignored.sock")

	if got := resolveDockerSocket(); got != "/tmp/colima.sock" {
		t.Fatalf("socket = %q, want /tmp/colima.sock", got)
	}
}

func TestResolveDockerSocketUsesDockerHost(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "unix:///tmp/docker.sock")

	if got := resolveDockerSocket(); got != "/tmp/docker.sock" {
		t.Fatalf("socket = %q, want /tmp/docker.sock", got)
	}
}

func TestResolveDockerSocketUsesCurrentContext(t *testing.T) {
	configDir := t.TempDir()
	writeDockerContext(t, configDir, "colima", "/tmp/current.sock")
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(`{"currentContext":"colima"}`), 0o644); err != nil {
		t.Fatalf("write Docker config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", configDir)
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "")

	if got := resolveDockerSocket(); got != "/tmp/current.sock" {
		t.Fatalf("socket = %q, want /tmp/current.sock", got)
	}
}

func TestResolveDockerSocketFallsBackToDefault(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "")

	if got := resolveDockerSocket(); got != "/var/run/docker.sock" {
		t.Fatalf("socket = %q, want /var/run/docker.sock", got)
	}
}

func TestCollectorPausesAndUnpausesContainersThroughDockerAPI(t *testing.T) {
	var requests []string
	collector := &Collector{containers: []string{"paradedb-alternate", "custom"}, httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.Method+" "+request.URL.EscapedPath())
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}}

	if err := collector.PauseContainer("paradedb-alternate"); err != nil {
		t.Fatalf("pause container: %v", err)
	}
	if got := strings.Join(collector.activeContainers(), ","); got != "custom" {
		t.Fatalf("active containers after pause = %q, want custom", got)
	}
	if err := collector.UnpauseContainer("paradedb-alternate"); err != nil {
		t.Fatalf("unpause container: %v", err)
	}
	if got := strings.Join(collector.activeContainers(), ","); got != "paradedb-alternate,custom" {
		t.Fatalf("active containers after unpause = %q", got)
	}

	want := []string{
		"POST /containers/paradedb-alternate/pause",
		"POST /containers/paradedb-alternate/unpause",
	}
	if strings.Join(requests, "|") != strings.Join(want, "|") {
		t.Fatalf("Docker requests = %v, want %v", requests, want)
	}
}

func TestCollectorReportsDockerPauseFailure(t *testing.T) {
	collector := &Collector{httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusConflict,
			Body:       io.NopCloser(strings.NewReader("container is not running")),
		}, nil
	})}}

	err := collector.PauseContainer("custom")
	if err == nil || !strings.Contains(err.Error(), "container is not running") {
		t.Fatalf("pause error = %v", err)
	}
}

func TestCollectorStopsContainerThroughDockerAPI(t *testing.T) {
	var requestText string
	collector := &Collector{containers: []string{"paradedb-alternate", "custom"}, httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestText = request.Method + " " + request.URL.EscapedPath() + "?" + request.URL.RawQuery
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})}}

	if err := collector.StopContainer("paradedb-alternate"); err != nil {
		t.Fatalf("stop container: %v", err)
	}
	if got := strings.Join(collector.activeContainers(), ","); got != "custom" {
		t.Fatalf("active containers after stop = %q, want custom", got)
	}
	if want := "POST /containers/paradedb-alternate/stop?t=600"; requestText != want {
		t.Fatalf("Docker request = %q, want %q", requestText, want)
	}
}

func TestCollectorAcceptsAlreadyStoppedContainer(t *testing.T) {
	collector := &Collector{containers: []string{"custom"}, httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotModified,
			Body:       io.NopCloser(strings.NewReader("container already stopped")),
		}, nil
	})}}

	if err := collector.StopContainer("custom"); err != nil {
		t.Fatalf("stop already stopped container: %v", err)
	}
	if got := collector.activeContainers(); len(got) != 0 {
		t.Fatalf("active containers after stop = %v, want none", got)
	}
}

func TestCollectorReportsDockerStopFailure(t *testing.T) {
	collector := &Collector{httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader("shutdown failed")),
		}, nil
	})}}

	err := collector.StopContainer("custom")
	if err == nil || !strings.Contains(err.Error(), "shutdown failed") {
		t.Fatalf("stop error = %v", err)
	}
}

func TestIndexIOStatsRegistryStoresLatestSnapshot(t *testing.T) {
	const backend = "index-io-registry-test"
	first := IndexIOStats{
		ReadBytes: 8192,
		HitBytes:  16384,
		Read:      "8192 bytes",
		Hit:       "16 kB",
	}
	RegisterIndexIOStats(backend, first)

	got, ok := GetIndexIOStats(backend)
	if !ok || got != first {
		t.Fatalf("snapshot = %#v, %v; want %#v, true", got, ok, first)
	}

	latest := IndexIOStats{ReadBytes: 32768, HitBytes: 65536, Read: "32 kB", Hit: "64 kB"}
	RegisterIndexIOStats(backend, latest)
	got, ok = GetIndexIOStats(backend)
	if !ok || got != latest {
		t.Fatalf("latest snapshot = %#v, %v; want %#v, true", got, ok, latest)
	}
}

func TestIndexIOStatsRegistryReportsMissingBackend(t *testing.T) {
	if _, ok := GetIndexIOStats("missing-index-io-registry-test"); ok {
		t.Fatal("unexpected snapshot for missing backend")
	}
}

func TestInitialIndexIOStatsDoesNotReplaceCollectedSnapshot(t *testing.T) {
	const backend = "index-io-initial-test"
	want := IndexIOStats{ReadBytes: 8192, HitBytes: 16384, Read: "8192 bytes", Hit: "16 kB"}
	RegisterIndexIOStats(backend, want)
	RegisterInitialIndexIOStats(backend, IndexIOStats{Read: "0B", Hit: "0B"})

	got, ok := GetIndexIOStats(backend)
	if !ok || got != want {
		t.Fatalf("snapshot = %#v, %v; want %#v, true", got, ok, want)
	}
}

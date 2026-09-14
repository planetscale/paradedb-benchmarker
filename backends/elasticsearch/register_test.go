package elasticsearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/paradedb/benchmarker/backends"
)

func TestBackendRegistrationAndDrop(t *testing.T) {
	config, ok := backends.GetConfig("elasticsearch")
	if !ok || config.DefaultConn != "http://localhost:9200" ||
		config.EnvVar != "ELASTICSEARCH_URL" || config.Container != "elasticsearch" {
		t.Fatalf("unexpected Elasticsearch defaults: %+v", config)
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodDelete || r.URL.Path != "/documents" {
			t.Errorf("drop request = %s %s, want DELETE /documents", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer server.Close()
	loader := backends.GetCLILoader("elasticsearch", server.URL)
	if err := loader.Drop(context.Background(), &backends.Schema{Table: "documents"}); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("drop sent %d requests, want 1", got)
	}
}

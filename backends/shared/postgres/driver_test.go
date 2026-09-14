package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPoolConfigKeepsOneVUConnectionAndCancelsInPlace(t *testing.T) {
	config, err := newPoolConfig("postgres://postgres:postgres@localhost:5432/benchmark")
	if err != nil {
		t.Fatalf("newPoolConfig: %v", err)
	}
	if config.MaxConns != 1 || config.MinConns != 1 {
		t.Fatalf("pool bounds = %d..%d, want exactly one connection", config.MinConns, config.MaxConns)
	}
	if config.MaxConnLifetime != benchmarkConnectionLifetime || config.MaxConnIdleTime != benchmarkConnectionLifetime {
		t.Fatalf(
			"pool connection lifetime/idle = %s/%s, want %s/%s",
			config.MaxConnLifetime,
			config.MaxConnIdleTime,
			benchmarkConnectionLifetime,
			benchmarkConnectionLifetime,
		)
	}

	conn := new(pgconn.PgConn)
	handler, ok := config.ConnConfig.BuildContextWatcherHandler(conn).(*pgconn.CancelRequestContextWatcherHandler)
	if !ok {
		t.Fatalf("context watcher = %T, want connection-preserving cancel request handler", handler)
	}
	if handler.Conn != conn {
		t.Fatal("cancel request handler does not target the VU connection")
	}
	if handler.CancelRequestDelay != 0 || handler.DeadlineDelay != queryCancelDeadlineDelay {
		t.Fatalf(
			"cancel request delay/deadline = %s/%s, want 0/%s",
			handler.CancelRequestDelay,
			handler.DeadlineDelay,
			queryCancelDeadlineDelay,
		)
	}
}

func TestCanceledQueryKeepsBackendPID(t *testing.T) {
	connString := os.Getenv("BENCHMARKER_POSTGRES_TEST_URL")
	if connString == "" {
		t.Skip("BENCHMARKER_POSTGRES_TEST_URL is not set")
	}

	driverValue, err := New(connString)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	driver := driverValue.(*Driver)
	t.Cleanup(func() { _ = driver.Close() })

	var before int
	if err := driver.pool.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&before); err != nil {
		t.Fatalf("query backend PID before cancellation: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := driver.Query(ctx, "SELECT pg_sleep(10)"); err == nil {
		t.Fatal("long query was not canceled")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "57014" {
			t.Fatalf("canceled query error = %T %v, want PostgreSQL 57014", err, err)
		}
	}

	var after int
	if err := driver.pool.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&after); err != nil {
		t.Fatalf("query backend PID after cancellation: %v", err)
	}
	if after != before {
		t.Fatalf("backend PID changed across cancellation: %d -> %d", before, after)
	}
}

func TestParseWALLSN(t *testing.T) {
	for _, test := range []struct {
		lsn  string
		want uint64
	}{
		{lsn: "0/0", want: 0},
		{lsn: "0/16B6C50", want: 0x16B6C50},
		{lsn: "1/0", want: 1 << 32},
		{lsn: "A/FFFFFFFF", want: 0xAFFFFFFFF},
	} {
		got, err := parseWALLSN(test.lsn)
		if err != nil || got != test.want {
			t.Fatalf("parseWALLSN(%q) = %#x, %v; want %#x", test.lsn, got, err, test.want)
		}
	}
}

func TestParseWALLSNRejectsInvalidValues(t *testing.T) {
	for _, lsn := range []string{"", "123", "/1", "1/", "1/2/3", "XYZ/1", "1/XYZ", "100000000/0"} {
		if _, err := parseWALLSN(lsn); err == nil {
			t.Fatalf("parseWALLSN(%q) succeeded", lsn)
		}
	}
}

func TestAppendSpaceSQLUsesBoundedRandomHeapPage(t *testing.T) {
	sql := appendSpaceToRandomDocumentSQL("documents")
	for _, fragment := range []string{
		`FROM "documents" AS document`,
		`UPDATE "documents" AS document`,
		`document.ctid >= (`,
		`document.ctid < (`,
		`FROM bounds`,
		`bounds.block + 1`,
		`SET body = document.body || ' '`,
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("random update SQL missing %q:\n%s", fragment, sql)
		}
	}
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Fatalf("random update SQL must not sort the relation:\n%s", sql)
	}
	if strings.Contains(strings.ToUpper(sql), "CROSS JOIN BOUNDS") {
		t.Fatalf("CTID bounds must be scalar init plans, not a join filter:\n%s", sql)
	}
}

func TestAppendSpaceSQLQuotesTargetIdentifier(t *testing.T) {
	sql := appendSpaceToRandomDocumentSQL(`documents"; DROP TABLE documents; --`)
	if !strings.Contains(sql, `"documents""; DROP TABLE documents; --"`) {
		t.Fatalf("target identifier was not quoted safely:\n%s", sql)
	}
}

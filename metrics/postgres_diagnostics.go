package metrics

import "sync"

// PostgresDiagnosticsSample is one cumulative PostgreSQL diagnostics snapshot.
// Counters are reset at the measured phase boundary and sampled once per second.
type PostgresDiagnosticsSample struct {
	Time             int64                           `json:"time"`
	WAL              PostgresWALDiagnostics          `json:"wal"`
	Checkpointer     PostgresCheckpointerDiagnostics `json:"checkpointer"`
	BackgroundWriter PostgresBGWriterDiagnostics     `json:"backgroundWriter"`
	Database         PostgresDatabaseDiagnostics     `json:"database"`
	IO               []PostgresIODiagnostics         `json:"io"`
	Activity         []PostgresActivityDiagnostics   `json:"activity"`
	IndexBytes       map[string]int64                `json:"indexBytes,omitempty"`
	Segments         []PostgresSegmentDiagnostics    `json:"segments,omitempty"`
}

type PostgresWALDiagnostics struct {
	Records     int64 `json:"records"`
	FullPages   int64 `json:"fullPages"`
	Bytes       int64 `json:"bytes"`
	BuffersFull int64 `json:"buffersFull"`
}

type PostgresCheckpointerDiagnostics struct {
	Timed          int64   `json:"timed"`
	Requested      int64   `json:"requested"`
	Done           int64   `json:"done"`
	WriteTimeMS    float64 `json:"writeTimeMs"`
	SyncTimeMS     float64 `json:"syncTimeMs"`
	BuffersWritten int64   `json:"buffersWritten"`
}

type PostgresBGWriterDiagnostics struct {
	BuffersClean     int64 `json:"buffersClean"`
	MaxWrittenClean  int64 `json:"maxWrittenClean"`
	BuffersAllocated int64 `json:"buffersAllocated"`
}

type PostgresDatabaseDiagnostics struct {
	BlocksRead       int64   `json:"blocksRead"`
	BlocksHit        int64   `json:"blocksHit"`
	BlockReadTimeMS  float64 `json:"blockReadTimeMs"`
	BlockWriteTimeMS float64 `json:"blockWriteTimeMs"`
	TempFiles        int64   `json:"tempFiles"`
	TempBytes        int64   `json:"tempBytes"`
	Deadlocks        int64   `json:"deadlocks"`
}

type PostgresIODiagnostics struct {
	BackendType     string  `json:"backendType"`
	Object          string  `json:"object"`
	Context         string  `json:"context"`
	Reads           int64   `json:"reads"`
	ReadBytes       int64   `json:"readBytes"`
	ReadTimeMS      float64 `json:"readTimeMs"`
	Writes          int64   `json:"writes"`
	WriteBytes      int64   `json:"writeBytes"`
	WriteTimeMS     float64 `json:"writeTimeMs"`
	Writebacks      int64   `json:"writebacks"`
	WritebackTimeMS float64 `json:"writebackTimeMs"`
	Extends         int64   `json:"extends"`
	ExtendBytes     int64   `json:"extendBytes"`
	ExtendTimeMS    float64 `json:"extendTimeMs"`
	Hits            int64   `json:"hits"`
	Evictions       int64   `json:"evictions"`
	Reuses          int64   `json:"reuses"`
	Fsyncs          int64   `json:"fsyncs"`
	FsyncTimeMS     float64 `json:"fsyncTimeMs"`
}

type PostgresActivityDiagnostics struct {
	BackendType   string `json:"backendType"`
	State         string `json:"state"`
	WaitEventType string `json:"waitEventType"`
	WaitEvent     string `json:"waitEvent"`
	Sessions      int64  `json:"sessions"`
}

type PostgresSegmentDiagnostics struct {
	Index       string `json:"index"`
	Kind        string `json:"kind"`
	SourceState string `json:"sourceState"`
	Origin      string `json:"origin"`
	Sequence    int64  `json:"sequence"`
	Documents   int64  `json:"documents"`
	DeadDocs    int64  `json:"deadDocs"`
	Postings    int64  `json:"postings"`
	Pages       int64  `json:"pages"`
	RootBlock   int64  `json:"rootBlock"`
}

var (
	postgresDiagnostics   = make(map[string][]PostgresDiagnosticsSample)
	postgresDiagnosticsMu sync.RWMutex
)

// ResetPostgresDiagnostics clears one backend's samples at its measured phase boundary.
func ResetPostgresDiagnostics(backend string) {
	postgresDiagnosticsMu.Lock()
	defer postgresDiagnosticsMu.Unlock()
	delete(postgresDiagnostics, backend)
}

// RegisterPostgresDiagnostics appends one snapshot for a backend.
func RegisterPostgresDiagnostics(backend string, sample PostgresDiagnosticsSample) {
	postgresDiagnosticsMu.Lock()
	defer postgresDiagnosticsMu.Unlock()
	postgresDiagnostics[backend] = append(postgresDiagnostics[backend], clonePostgresDiagnosticsSample(sample))
}

// GetPostgresDiagnostics returns a copy of every collected backend stream.
func GetPostgresDiagnostics() map[string][]PostgresDiagnosticsSample {
	postgresDiagnosticsMu.RLock()
	defer postgresDiagnosticsMu.RUnlock()
	result := make(map[string][]PostgresDiagnosticsSample, len(postgresDiagnostics))
	for backend, samples := range postgresDiagnostics {
		cloned := make([]PostgresDiagnosticsSample, len(samples))
		for i, sample := range samples {
			cloned[i] = clonePostgresDiagnosticsSample(sample)
		}
		result[backend] = cloned
	}
	return result
}

func clonePostgresDiagnosticsSample(sample PostgresDiagnosticsSample) PostgresDiagnosticsSample {
	cloned := sample
	cloned.IO = append([]PostgresIODiagnostics(nil), sample.IO...)
	cloned.Activity = append([]PostgresActivityDiagnostics(nil), sample.Activity...)
	cloned.Segments = append([]PostgresSegmentDiagnostics(nil), sample.Segments...)
	if sample.IndexBytes != nil {
		cloned.IndexBytes = make(map[string]int64, len(sample.IndexBytes))
		for index, bytes := range sample.IndexBytes {
			cloned.IndexBytes[index] = bytes
		}
	}
	return cloned
}

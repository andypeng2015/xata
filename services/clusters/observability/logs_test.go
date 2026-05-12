package observability

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeLogsBackend struct {
	queries []string
	rows    []LogRow
	err     error
}

func (f *fakeLogsBackend) Query(_ context.Context, query string, _, _ time.Time, _ int) ([]LogRow, error) {
	f.queries = append(f.queries, query)
	return f.rows, f.err
}

func TestBuildLogsQL_AppendsBranchScope(t *testing.T) {
	q, err := buildLogsQL("xata-clusters", "br-1", nil, 0)
	require.NoError(t, err)
	require.Contains(t, q, `kubernetes.namespace_name:="xata-clusters"`)
	require.Contains(t, q, `kubernetes.container_name:="postgres"`)
	require.Contains(t, q, `logger:="postgres"`, "drops instance-manager / barman lines from the postgres container")
	require.Contains(t, q, `kubernetes.pod_name:~"^br-1-.*"`)
	require.NotContains(t, q, "_time:<", "no resume clause when cursor is empty")
}

func TestBuildLogsQL_ResumeCursorClause(t *testing.T) {
	q, err := buildLogsQL("xata-clusters", "br-1", nil, 1730000000000000000)
	require.NoError(t, err)
	require.Contains(t, q, "_time:<1730000000000000000")
}

func TestBuildLogsQL_FilterTranslation(t *testing.T) {
	tests := map[string]struct {
		filters  []LogFilter
		contains []string
	}{
		"instance in": {
			filters:  []LogFilter{{Field: "instance", Op: "in", Values: []string{"br-1-0", "br-1-1"}}},
			contains: []string{`kubernetes.pod_name:in ("br-1-0","br-1-1")`},
		},
		"level expansion": {
			filters:  []LogFilter{{Field: "level", Op: "in", Values: []string{"error"}}},
			contains: []string{`severity_text:in ("ERROR","FATAL","PANIC","CRITICAL")`},
		},
		"process in": {
			filters:  []LogFilter{{Field: "process", Op: "in", Values: []string{"client backend"}}},
			contains: []string{`backend_type:in ("client backend")`},
		},
		"body contains": {
			filters:  []LogFilter{{Field: "body", Op: "contains", Value: "slow"}},
			contains: []string{`body:"slow"`},
		},
		"body iregex prepends inline flag": {
			filters:  []LogFilter{{Field: "body", Op: "iregex", Value: "^conn"}},
			contains: []string{`body:~"(?i)^conn"`},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := buildLogsQL("xata-clusters", "br-1", tt.filters, 0)
			require.NoError(t, err)
			for _, frag := range tt.contains {
				require.Contains(t, got, frag, got)
			}
		})
	}
}

func TestLogsQuerier_DecodesEntriesAndSetsCursor(t *testing.T) {
	t1 := time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(-time.Minute)
	backend := &fakeLogsBackend{
		rows: []LogRow{
			{Timestamp: t1, Pod: "br-1-0", Severity: "ERROR", Process: "client backend", Message: `{"record":{"message":"boom"}}`},
			{Timestamp: t2, Pod: "br-1-0", Severity: "INFO", Message: "hello"},
		},
	}
	q := NewLogsQuerier(backend, "xata-clusters")

	res, err := q.Query(context.Background(), "br-1", t2.Add(-time.Hour), t1, nil, 2, "")
	require.NoError(t, err)

	require.Len(t, res.Entries, 2)
	require.Equal(t, "boom", res.Entries[0].Message, "CNPG record body should be unwrapped")
	require.Equal(t, "error", res.Entries[0].Level)
	require.Equal(t, "client backend", res.Entries[0].Process)
	require.Equal(t, "info", res.Entries[1].Level)

	require.NotEmpty(t, res.NextCursor, "cursor should be set when page is full")
	resumeNanos, err := decodeCursor(res.NextCursor)
	require.NoError(t, err)
	require.Equal(t, t2.UnixNano(), resumeNanos, "cursor anchors at oldest entry; LQL clause is strict less-than")
}

func TestLogsQuerier_NoCursorWhenPartialPage(t *testing.T) {
	backend := &fakeLogsBackend{rows: []LogRow{{Timestamp: time.Now(), Pod: "br-1-0", Message: "a"}}}
	q := NewLogsQuerier(backend, "xata-clusters")
	res, err := q.Query(context.Background(), "br-1", time.Now().Add(-time.Hour), time.Now(), nil, 100, "")
	require.NoError(t, err)
	require.Empty(t, res.NextCursor)
}

func TestUnwrapCNPGBody(t *testing.T) {
	require.Equal(t, "boom", unwrapCNPGBody(`{"record":{"message":"boom"}}`))
	require.Equal(t, "lifecycle", unwrapCNPGBody(`{"msg":"lifecycle"}`))
	require.Equal(t, "plain text", unwrapCNPGBody("plain text"))
	require.Equal(t, `{"foo":"bar"}`, unwrapCNPGBody(`{"foo":"bar"}`)) // unrecognised JSON shape passes through
}

func TestExpandLevels(t *testing.T) {
	got := expandLevels([]string{"error", "warning"})
	joined := strings.Join(got, ",")
	require.Contains(t, joined, "ERROR")
	require.Contains(t, joined, "FATAL")
	require.Contains(t, joined, "WARNING")
	require.NotContains(t, joined, "INFO")
}

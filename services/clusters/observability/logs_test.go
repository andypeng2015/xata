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
	require.Contains(t, q, `logger:in ("postgres","pgaudit")`, "scopes to postgres and pgaudit, drops instance-manager / barman")
	require.Contains(t, q, `branch_id:="br-1"`)
	require.NotContains(t, q, "_time:<", "no resume clause when cursor is empty")
}

func TestBuildLogsQL_ResumeCursorClause(t *testing.T) {
	q, err := buildLogsQL("xata-clusters", "br-1", nil, 1730000000000000000)
	require.NoError(t, err)
	require.Contains(t, q, "_time:<2024-10-27T03:33:20Z")
}

func TestCompileLogFilter(t *testing.T) {
	tests := map[string]struct {
		filter LogFilter
		want   string
	}{
		"instance in": {
			filter: LogFilter{Field: "instance", Op: "in", Values: []string{"br-1-0", "br-1-1"}},
			want:   `kubernetes.pod_name:in ("br-1-0","br-1-1")`,
		},
		"level error expands to postgres severities": {
			filter: LogFilter{Field: "level", Op: "in", Values: []string{"error"}},
			want:   `severity_text:in ("ERROR","FATAL","PANIC","CRITICAL")`,
		},
		"level info expands": {
			filter: LogFilter{Field: "level", Op: "in", Values: []string{"info"}},
			want:   `severity_text:in ("INFO","LOG","NOTICE")`,
		},
		"process in": {
			filter: LogFilter{Field: "process", Op: "in", Values: []string{"client backend"}},
			want:   `backend_type:in ("client backend")`,
		},
		"logger in": {
			filter: LogFilter{Field: "logger", Op: "in", Values: []string{"postgres"}},
			want:   `logger:in ("postgres")`,
		},
		"body contains is a literal substring regex on _msg": {
			filter: LogFilter{Field: "body", Op: "contains", Value: "slow"},
			want:   `_msg:~"slow"`,
		},
		"body icontains prepends the inline case-insensitive flag": {
			filter: LogFilter{Field: "body", Op: "icontains", Value: "checkpoint"},
			want:   `_msg:~"(?i)checkpoint"`,
		},
		"body contains escapes regex metacharacters": {
			filter: LogFilter{Field: "body", Op: "contains", Value: "a.b*c"},
			want:   `_msg:~"a\\.b\\*c"`,
		},
		"body icontains escapes regex metacharacters": {
			filter: LogFilter{Field: "body", Op: "icontains", Value: "1.5s"},
			want:   `_msg:~"(?i)1\\.5s"`,
		},
		"body regex passes the pattern through verbatim": {
			filter: LogFilter{Field: "body", Op: "regex", Value: "^conn.* established$"},
			want:   `_msg:~"^conn.* established$"`,
		},
		"body iregex passes the pattern through with inline flag": {
			filter: LogFilter{Field: "body", Op: "iregex", Value: "^conn"},
			want:   `_msg:~"(?i)^conn"`,
		},
		"value with embedded quotes is escaped for the LogsQL literal": {
			filter: LogFilter{Field: "body", Op: "contains", Value: `say "hi"`},
			want:   `_msg:~"say \"hi\""`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := compileLogFilter(tt.filter)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompileLogFilter_Rejected(t *testing.T) {
	tests := map[string]LogFilter{
		"instance only supports in":        {Field: "instance", Op: "contains", Value: "x"},
		"level only supports in":           {Field: "level", Op: "regex", Value: "x"},
		"process only supports in":         {Field: "process", Op: "icontains", Value: "x"},
		"logger only supports in":          {Field: "logger", Op: "contains", Value: "x"},
		"body rejects in":                  {Field: "body", Op: "in", Values: []string{"x"}},
		"body rejects unknown op":          {Field: "body", Op: "eq", Value: "x"},
		"unknown field":                    {Field: "trace_id", Op: "contains", Value: "x"},
		"level rejects unknown level name": {Field: "level", Op: "in", Values: []string{"bogus"}},
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := compileLogFilter(f)
			require.Error(t, err)
		})
	}
}

func TestBuildLogsQL_FullQueryLocksScopeAndFields(t *testing.T) {
	filters := []LogFilter{
		{Field: "instance", Op: "in", Values: []string{"br-1-0"}},
		{Field: "level", Op: "in", Values: []string{"error"}},
		{Field: "process", Op: "in", Values: []string{"checkpointer"}},
		{Field: "body", Op: "icontains", Value: "checkpoint"},
	}
	got, err := buildLogsQL("xata-clusters", "br-1", filters, 1730000000000000000)
	require.NoError(t, err)

	want := `kubernetes.namespace_name:="xata-clusters" AND kubernetes.container_name:="postgres"` +
		` AND logger:in ("postgres","pgaudit")` +
		` AND branch_id:="br-1"` +
		` AND _time:<2024-10-27T03:33:20Z` +
		` AND kubernetes.pod_name:in ("br-1-0")` +
		` AND severity_text:in ("ERROR","FATAL","PANIC","CRITICAL")` +
		` AND backend_type:in ("checkpointer")` +
		` AND _msg:~"(?i)checkpoint"`
	require.Equal(t, want, got)

	require.NotContains(t, got, "body:", "the message lives in _msg; body: matches nothing in VictoriaLogs")
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
	type cnpgBody struct {
		msg    string
		logger string
	}
	tests := map[string]struct {
		body string
		want cnpgBody
	}{
		"postgres record.message": {
			body: `{"record":{"message":"boom"}}`,
			want: cnpgBody{msg: "boom"},
		},
		"instance-manager msg": {
			body: `{"msg":"lifecycle"}`,
			want: cnpgBody{msg: "lifecycle"},
		},
		"plain text passes through": {
			body: "plain text",
			want: cnpgBody{msg: "plain text"},
		},
		"unrecognised JSON shape passes through": {
			body: `{"foo":"bar"}`,
			want: cnpgBody{msg: `{"foo":"bar"}`},
		},
		"pgaudit SESSION rendered as native CSV with logger stamp": {
			body: `{"logger":"pgaudit","record":{"message":"","audit":{"audit_type":"SESSION","statement_id":"1","substatement_id":"1","class":"READ","command":"SELECT","object_type":"","object_name":"","statement":"select * from accounts","parameter":"<not logged>"}}}`,
			want: cnpgBody{
				msg:    `AUDIT: SESSION,1,1,READ,SELECT,,,select * from accounts,<not logged>`,
				logger: "pgaudit",
			},
		},
		"pgaudit statement with commas is CSV-quoted": {
			body: `{"logger":"pgaudit","record":{"audit":{"audit_type":"SESSION","statement_id":"2","substatement_id":"1","class":"WRITE","command":"INSERT","object_type":"TABLE","object_name":"public.t","statement":"insert into t values (1, 2)","parameter":"<not logged>","rows":"1"}}}`,
			want: cnpgBody{
				msg:    `AUDIT: SESSION,2,1,WRITE,INSERT,TABLE,public.t,"insert into t values (1, 2)",<not logged>,1`,
				logger: "pgaudit",
			},
		},
		"pgaudit envelope with non-map audit falls through to record.message but still stamps logger": {
			body: `{"logger":"pgaudit","record":{"message":"fallback","audit":"not-a-map"}}`,
			want: cnpgBody{msg: "fallback", logger: "pgaudit"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			msg, logger := unwrapCNPGBody(tt.body)
			got := cnpgBody{msg: msg, logger: logger}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeRow(t *testing.T) {
	row := decodeRow(map[string]any{
		"_time":               "2025-05-01T12:00:00Z",
		"_msg":                "checkpoint complete",
		"kubernetes.pod_name": "br-1-0",
		"severity_text":       "LOG",
		"backend_type":        "checkpointer",
	})
	require.False(t, row.Timestamp.IsZero())
	require.Equal(t, "checkpoint complete", row.Message)
	require.Equal(t, "br-1-0", row.Pod)
	require.Equal(t, "LOG", row.Severity)
	require.Equal(t, "checkpointer", row.Process)
}

func TestExpandLevels(t *testing.T) {
	got := expandLevels([]string{"error", "warning"})
	joined := strings.Join(got, ",")
	require.Contains(t, joined, "ERROR")
	require.Contains(t, joined, "FATAL")
	require.Contains(t, joined, "WARNING")
	require.NotContains(t, joined, "INFO")
}

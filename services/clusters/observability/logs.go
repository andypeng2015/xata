package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// LogFilter mirrors the user-facing filter shape passed across the gRPC
// boundary. It's intentionally a copy of clustersv1.LogFilter so this package
// remains transport-agnostic.
type LogFilter struct {
	Field  string
	Op     string
	Values []string
	Value  string
}

// LogsBackend is the minimum surface LogsQuerier needs from VictoriaLogs.
type LogsBackend interface {
	Query(ctx context.Context, lqlQuery string, start, end time.Time, limit int) ([]LogRow, error)
}

// LogRow is one parsed entry from VictoriaLogs.
type LogRow struct {
	Timestamp time.Time
	Pod       string
	Severity  string
	Process   string
	Message   string
}

// LogEntry is the cleaned-up entry returned to the RPC.
type LogEntry struct {
	Timestamp  time.Time
	InstanceID string
	Level      string
	Message    string
	Process    string
}

// LogsResult contains everything the RPC needs to serialize.
type LogsResult struct {
	Entries    []LogEntry
	NextCursor string
}

// LogsQuerier resolves a branch logs query into a single LogsQL expression,
// runs it, and decodes the result. The branch-scope predicate on branch_id
// is always added server-side as defense in depth.
type LogsQuerier struct {
	backend   LogsBackend
	namespace string
}

func NewLogsQuerier(backend LogsBackend, namespace string) *LogsQuerier {
	return &LogsQuerier{backend: backend, namespace: namespace}
}

// schemaLevelToSeverities is the user-facing → CNPG/Postgres severity mapping
// preserved from the SigNoz client so historical queries stay shaped the same.
var schemaLevelToSeverities = map[string][]string{
	"debug":   {"DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5"},
	"info":    {"INFO", "LOG", "NOTICE"},
	"warning": {"WARN", "WARNING"},
	"error":   {"ERROR", "FATAL", "PANIC", "CRITICAL"},
}

var severityToLevel = func() map[string]string {
	out := make(map[string]string, 32)
	for level, severities := range schemaLevelToSeverities {
		for _, s := range severities {
			out[s] = level
		}
	}
	return out
}()

// expandLevels resolves one or more user levels to the underlying severities.
func expandLevels(levels []string) []string {
	if len(levels) == 0 {
		return nil
	}
	out := make([]string, 0, len(levels)*4)
	for _, lvl := range levels {
		out = append(out, schemaLevelToSeverities[lvl]...)
	}
	return out
}

// Query executes the LogsQL query and returns up to limit entries plus an
// opaque pagination cursor. The cursor carries the timestamp of the oldest
// returned entry so a subsequent call can resume strictly before it.
func (q *LogsQuerier) Query(ctx context.Context, branchID string, start, end time.Time, filters []LogFilter, limit int, cursor string) (*LogsResult, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	cursorNanos, err := decodeCursor(cursor)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}

	lql, err := buildLogsQL(q.namespace, branchID, filters, cursorNanos)
	if err != nil {
		return nil, err
	}

	rows, err := q.backend.Query(ctx, lql, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("query backend: %w", err)
	}

	out := &LogsResult{Entries: make([]LogEntry, 0, len(rows))}
	for _, r := range rows {
		entry := LogEntry{
			Timestamp:  r.Timestamp,
			InstanceID: r.Pod,
			Message:    unwrapCNPGBody(r.Message),
		}
		if mapped, ok := severityToLevel[strings.ToUpper(r.Severity)]; ok {
			entry.Level = mapped
		}
		if r.Process != "" {
			entry.Process = r.Process
		}
		out.Entries = append(out.Entries, entry)
	}

	// VictoriaLogs returns rows ordered by _time DESC; when the page is full
	// there may be older matching entries. Cursor the next page off the
	// oldest returned timestamp.
	if len(out.Entries) >= limit && len(out.Entries) > 0 {
		out.NextCursor = encodeCursor(out.Entries[len(out.Entries)-1].Timestamp)
	}
	return out, nil
}

// buildLogsQL renders the LogsQL expression. It starts with the namespace
// and container scopes, then appends an exact-match branch_id predicate
// (defense in depth) and finally each user-supplied filter. We use the
// LogsQL field-filter syntax (`field:value`, `field:in (a,b)`,
// `field:~"regex"`) which is the stable subset supported by VictoriaLogs.
//
// resumeBeforeNanos > 0 adds `_time:<{ns}` so paginated queries pick up
// strictly older rows than the previous page's oldest timestamp.
func buildLogsQL(namespace, branchID string, filters []LogFilter, resumeBeforeNanos int64) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "kubernetes.namespace_name:=%q AND kubernetes.container_name:=%q", namespace, "postgres")
	// The postgres container streams output from CNPG's instance manager,
	// which mixes its own logs (logger="instance-manager"), barman lines
	// (logger="backup") and actual postgres records (logger="postgres").
	// Match SigNoz behaviour and only surface the latter.
	b.WriteString(` AND logger:="postgres"`)
	// TODO(cleanup after one month): drop the pod_name regex disjunct once
	// VictoriaLogs retentionPeriod (30d) has aged past the branch_id
	// rollout; rows from before then carry no branch_id field.
	fmt.Fprintf(&b, ` AND (branch_id:=%q OR kubernetes.pod_name:~%q)`, branchID, "^"+regexp.QuoteMeta(branchID)+"-")
	if resumeBeforeNanos > 0 {
		fmt.Fprintf(&b, " AND _time:<%d", resumeBeforeNanos)
	}

	for _, f := range filters {
		clause, err := compileLogFilter(f)
		if err != nil {
			return "", err
		}
		b.WriteString(" AND ")
		b.WriteString(clause)
	}
	return b.String(), nil
}

func compileLogFilter(f LogFilter) (string, error) {
	switch f.Field {
	case "instance":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [instance]", f.Op)
		}
		return inClause("kubernetes.pod_name", f.Values), nil
	case "level":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [level]", f.Op)
		}
		expanded := expandLevels(f.Values)
		if len(expanded) == 0 {
			return "", fmt.Errorf("invalid log level set")
		}
		return inClause("severity_text", expanded), nil
	case "process":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [process]", f.Op)
		}
		return inClause("backend_type", f.Values), nil
	case "body":
		switch f.Op {
		case "contains":
			return fmt.Sprintf("body:%s", quoteLQL(f.Value)), nil
		case "icontains":
			return fmt.Sprintf("body:i(%s)", quoteLQL(f.Value)), nil
		case "regex":
			return fmt.Sprintf("body:~%s", quoteLQL(f.Value)), nil
		case "iregex":
			return fmt.Sprintf("body:~%s", quoteLQL("(?i)"+f.Value)), nil
		default:
			return "", fmt.Errorf("op [%s] not allowed for field [body]", f.Op)
		}
	}
	return "", fmt.Errorf("unknown field [%s]", f.Field)
}

func inClause(field string, values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = quoteLQL(v)
	}
	return fmt.Sprintf("%s:in (%s)", field, strings.Join(quoted, ","))
}

// quoteLQL renders a string literal for LogsQL. Go-style quoting matches
// LogsQL's understanding of double-quoted strings with backslash escapes for
// ", \, and control characters.
func quoteLQL(v string) string {
	return strconv.Quote(v)
}

// unwrapCNPGBody mirrors the legacy SigNoz unwrapping: CNPG wraps Postgres
// CSV records as `{...,"record":{"message":"..."}}` and its lifecycle logs
// as `{...,"msg":"..."}`. Falls back to the original on miss.
func unwrapCNPGBody(body string) string {
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return body
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return body
	}
	if record, ok := parsed["record"].(map[string]any); ok {
		if msg, ok := record["message"].(string); ok && msg != "" {
			return msg
		}
	}
	if msg, ok := parsed["msg"].(string); ok && msg != "" {
		return msg
	}
	return body
}

// encodeCursor / decodeCursor turn a timestamp into an opaque string for
// pagination. UnixNano-as-decimal is enough — the cursor is opaque to clients,
// and the integer round-trips without timezone or precision quirks.
func encodeCursor(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(c, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return n, nil
}

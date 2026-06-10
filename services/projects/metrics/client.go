package metrics

import (
	"context"
	"time"
)

//go:generate go run github.com/vektra/mockery/v3 --output metricsmock --outpkg metricsmock --with-expecter --name Client

type Client interface {
	// GetMetrics returns the time serie(s) for the given metrics and
	// timeframe in a single backend round-trip. branchID enforces branch
	// scope on the pod label. Results are returned in the same order as
	// the requested metrics.
	GetMetrics(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, metricNames []string, instances, aggregations []string) ([]BranchMetrics, error)
	// GetLogs returns the log entries for the given timeframe and filters.
	GetLogs(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, filters []LogFilter, limit int, cursor string) (*BranchLogs, error)
}

// LogFilter is a transport-neutral filter clause produced by the projects
// handler and forwarded to the cell, where the clusters service compiles it
// into a LogsQL predicate.
type LogFilter struct {
	// Field one of: "instance" | "level" | "process" | "body"
	Field string
	// Op one of: "in" | "contains" | "icontains" | "regex" | "iregex"
	Op string
	// Values used when Op == "in"
	Values []string
	// Value used when Op != "in"
	Value string
}

// BranchMetrics is the time-series response for one requested metric.
type BranchMetrics struct {
	Metric string         `json:"metric"`
	Unit   string         `json:"unit"`
	Series []MetricSeries `json:"series"`
}

// Values The metric series values
type Values struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float32   `json:"value"`
}

// MetricSeries The metric series aggregated data
type MetricSeries struct {
	// Aggregation The aggregation used to generate this time-series
	Aggregation string `json:"aggregation"`

	// InstanceID ID of the instance
	InstanceID string   `json:"instanceID"`
	Values     []Values `json:"values"`
}

type BranchLogs struct {
	Start      time.Time  `json:"start"`
	End        time.Time  `json:"end"`
	Logs       []LogEntry `json:"logs"`
	NextCursor *string    `json:"nextCursor"`
}

type LogEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	InstanceID string    `json:"instanceID"`
	Level      *string   `json:"level,omitempty"`
	Message    string    `json:"message"`
	Process    *string   `json:"process,omitempty"`
}

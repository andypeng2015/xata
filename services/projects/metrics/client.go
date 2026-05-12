package metrics

import (
	"context"
	"time"
)

//go:generate go run github.com/vektra/mockery/v3 --output metricsmock --outpkg metricsmock --with-expecter --name Client

type Client interface {
	// GetMetric returns the time serie(s) for the given metric and timeframe.
	// branchID is required by the per-cell backend to enforce branch scope on
	// the pod label; the legacy SigNoz client ignores it.
	GetMetric(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, metric string, instances, aggregations []string) (*BranchMetrics, error)
	// GetLogs returns the log entries for the given timeframe and filters.
	GetLogs(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, filters []LogFilter, limit int, cursor string) (*BranchLogs, error)
}

// LogFilter is a backend-neutral filter clause produced by the projects handler
// and translated by each Client implementation into the backend's query language.
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

type BranchMetrics struct {
	End    time.Time      `json:"end"`
	Metric string         `json:"metric"`
	Series []MetricSeries `json:"series"`
	Start  time.Time      `json:"start"`

	// Unit The unit of the metric (percentage, bytes, ms, etc.)
	Unit string `json:"unit"`
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

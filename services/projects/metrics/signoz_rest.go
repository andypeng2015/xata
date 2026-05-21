package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strings"
	"time"

	"k8s.io/utils/ptr"

	"xata/internal/signoz"
	"xata/internal/signoz/filter"
)

var sigNozMetricName = map[string]struct {
	name, unit, metricType, temporalAgg, spaceAgg string
	additionalFilters                             map[string]string
}{
	// Maps Xata API metric names to SigNoz metric names
	"cpu":                  {name: "container.cpu.time", unit: "percentage", metricType: "counter", temporalAgg: "rate"},
	"memory":               {name: "container.memory.working_set", unit: "bytes", metricType: "gauge", spaceAgg: "avg"},
	"disk":                 {name: "cnpg_pg_database_size_bytes", unit: "bytes", metricType: "gauge", spaceAgg: "sum"},
	"connections_active":   {name: "cnpg_pg_stat_activity_connections_active", unit: "connections", metricType: "gauge", spaceAgg: "sum"},
	"connections_idle":     {name: "cnpg_pg_stat_activity_connections_idle", unit: "connections", metricType: "gauge", spaceAgg: "sum"},
	"network_ingress":      {name: "k8s.pod.network.io", unit: "bytes", metricType: "counter", temporalAgg: "increase", additionalFilters: map[string]string{"direction": "receive"}},
	"network_egress":       {name: "k8s.pod.network.io", unit: "bytes", metricType: "counter", temporalAgg: "increase", additionalFilters: map[string]string{"direction": "transmit"}},
	"iops_read":            {name: "cnpg_pg_stat_io_total_reads", unit: "iops", metricType: "counter", temporalAgg: "rate"},
	"iops_write":           {name: "cnpg_pg_stat_io_total_writes", unit: "iops", metricType: "counter", temporalAgg: "rate"},
	"latency_read":         {name: "cnpg_pg_stat_io_total_read_time_ms", unit: "ms", metricType: "counter", temporalAgg: "rate"},
	"latency_write":        {name: "cnpg_pg_stat_io_total_write_time_ms", unit: "ms", metricType: "counter", temporalAgg: "rate"},
	"throughput_read":      {name: "container_fs_reads_bytes_total", unit: "bytes", metricType: "counter", temporalAgg: "rate"},
	"throughput_write":     {name: "container_fs_writes_bytes_total", unit: "bytes", metricType: "counter", temporalAgg: "rate"},
	"wal_sync_time":        {name: "cnpg_collector_wal_sync_time", unit: "ms", metricType: "gauge", spaceAgg: "avg"},
	"replication_lag_time": {name: "cnpg_pg_replication_lag", unit: "s", metricType: "gauge", spaceAgg: "avg"},
}

type SigNozClient struct {
	client            *signoz.ClientWithResponses
	clustersNamespace string
}

// NewSigNozClient creates a new SigNoz client
func NewSigNozClient(endpoint, apiKey, clustersNamespace string) (*SigNozClient, error) {
	client, err := signoz.NewClientWithResponses(
		endpoint,
		signoz.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("SigNoz-Api-Key", apiKey)
			return nil
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create signoz client: %w", err)
	}

	return &SigNozClient{
		client:            client,
		clustersNamespace: clustersNamespace,
	}, nil
}

func (sc *SigNozClient) GetMetric(ctx context.Context, _organizationID, _cellID string, start, end time.Time, branchID string, metric string, instances, aggregations []string) (*BranchMetrics, error) {
	if _, exists := sigNozMetricName[metric]; !exists {
		return nil, fmt.Errorf("metric %s not found", metric)
	}

	// Build request
	reqBody, queryToAgg, err := buildMetricsReq(sc.clustersNamespace, branchID, start, end, metric, instances, aggregations)
	if err != nil {
		return nil, err
	}

	// Parse response
	branchMetrics := BranchMetrics{
		Start:  start,
		End:    end,
		Metric: metric,
		Unit:   sigNozMetricName[metric].unit,
		Series: []MetricSeries{},
	}

	results, err := sc.queryRange(ctx, reqBody)
	if err != nil {
		return nil, err
	}
	if results == nil {
		return &branchMetrics, nil
	}

	series, err := parseMetricResults(results, queryToAgg)
	if err != nil {
		return nil, err
	}
	branchMetrics.Series = series

	return &branchMetrics, nil
}

// queryRange sends a v5 query and returns the raw results, or nil if the response carries none.
func (sc *SigNozClient) queryRange(ctx context.Context, reqBody signoz.QueryRangeV5JSONRequestBody) ([]any, error) {
	response, err := sc.client.QueryRangeV5WithResponse(ctx, reqBody)
	if err != nil {
		return nil, fmt.Errorf("query range v5: %w", err)
	}

	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", response.StatusCode())
	}

	if response.JSON200 == nil {
		return nil, fmt.Errorf("empty response")
	}

	if response.JSON200.Status != "success" {
		return nil, fmt.Errorf("unexpected status: %s", response.JSON200.Status)
	}

	queryData := response.JSON200.Data.Data
	if queryData == nil || queryData.Results == nil {
		return nil, nil
	}

	return *queryData.Results, nil
}

// parseMetricResults extracts metric series from SigNoz query results
func parseMetricResults(results []any, queryToAgg map[string]string) ([]MetricSeries, error) {
	parsed, err := decodeResults[signoz.Querybuildertypesv5TimeSeriesData](results)
	if err != nil {
		return nil, err
	}

	series := make([]MetricSeries, 0)
	for _, result := range parsed {
		queryName := ptr.Deref(result.QueryName, "")
		agg, ok := queryToAgg[queryName]
		if !ok {
			return nil, fmt.Errorf("unexpected query name: %s", queryName)
		}
		if result.Aggregations == nil {
			continue
		}
		for _, bucket := range *result.Aggregations {
			if bucket.Series == nil {
				continue
			}
			for _, s := range *bucket.Series {
				series = append(series, MetricSeries{
					Aggregation: agg,
					InstanceID:  extractInstanceID(s.Labels),
					Values:      parseMetricValues(s.Values),
				})
			}
		}
	}

	return series, nil
}

// extractInstanceID retrieves the pod name from series labels
func extractInstanceID(labels *[]signoz.Querybuildertypesv5Label) string {
	if labels == nil {
		return ""
	}
	for _, label := range *labels {
		if label.Key == nil || label.Key.Name != "k8s.pod.name" || label.Value == nil {
			continue
		}
		if name, ok := (*label.Value).(string); ok {
			return name
		}
	}

	return ""
}

// parseMetricValues converts SigNoz points to metric values
func parseMetricValues(points *[]signoz.Querybuildertypesv5TimeSeriesValue) []Values {
	if points == nil {
		return nil
	}

	values := make([]Values, 0, len(*points))
	for _, point := range *points {
		if point.Timestamp == nil || point.Value == nil {
			continue
		}
		v := *point.Value
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		values = append(values, Values{
			Timestamp: time.UnixMilli(*point.Timestamp),
			Value:     float32(v),
		})
	}

	return values
}

func buildMetricsReq(clustersNamespace, branchID string, start, end time.Time, metricName string, instances, aggregations []string) (signoz.QueryRangeV5JSONRequestBody, map[string]string, error) {
	step := stepForMetric(metricName, calculateStep(start, end))
	filterExpr := buildMetricsFilterExpression(clustersNamespace, branchID, instances, metricName)

	queries, queryToAgg, err := buildMetricQueries(metricName, step, aggregations, filterExpr)
	if err != nil {
		return signoz.QueryRangeV5JSONRequestBody{}, nil, err
	}

	return buildRequestBody(start, end, queries, signoz.TimeSeries), queryToAgg, nil
}

// SigNoz v5 ties the counter rate window to the step, so clamp counter steps
// up to 4×scrape (matches the Victoria path).
const counterRateWindowSeconds = 120

func stepForMetric(metricName string, step int) int {
	info, ok := sigNozMetricName[metricName]
	if !ok || info.metricType != "counter" {
		return step
	}
	return max(step, counterRateWindowSeconds)
}

func buildMetricsFilterExpression(namespace, branchID string, instances []string, metricName string) string {
	parts := buildPodFilters(namespace, instances)
	parts = append(parts, branchScopeFilter(branchID))
	if info := sigNozMetricName[metricName]; info.additionalFilters != nil {
		for key, val := range info.additionalFilters {
			parts = append(parts, filter.Eq(key, val))
		}
	}

	return filter.And(parts...).Render()
}

// branchScopeFilter returns the per-branch predicate, OR'd with the
// pre-branch_id pod-name regex so queries still hit data emitted before the
// branch_id attribute was added.
// TODO(cleanup after one month): drop the Regexp fallback once SigNoz
// retention has aged past the branch_id rollout.
func branchScopeFilter(branchID string) filter.Expr {
	return filter.Or(
		filter.Eq("branch_id", branchID),
		filter.Regexp("k8s.pod.name", "^"+regexp.QuoteMeta(branchID)+"-"),
	)
}

func buildMetricQueries(metricName string, step int, aggregations []string, filterExpr string) ([]signoz.Querybuildertypesv5QueryEnvelope, map[string]string, error) {
	info := sigNozMetricName[metricName]
	stepInterval, err := buildStepInterval(step)
	if err != nil {
		return nil, nil, err
	}

	queries := make([]signoz.Querybuildertypesv5QueryEnvelope, 0, len(aggregations))
	queryToAgg := make(map[string]string, len(aggregations))
	for i, agg := range aggregations {
		var timeAgg, spaceAgg string
		if info.metricType == "counter" {
			timeAgg = info.temporalAgg
			spaceAgg = agg
		} else {
			timeAgg = agg
			spaceAgg = info.spaceAgg
		}

		// Queries are named A, B, C, etc. in order to be able to interpret the response properly
		queryName := string(rune(65 + i))
		queryToAgg[queryName] = agg

		spec := signoz.Querybuildertypesv5QueryBuilderQueryGithubComSigNozSignozPkgTypesQuerybuildertypesQuerybuildertypesv5MetricAggregation{
			Name:   &queryName,
			Signal: new(signoz.Metrics),
			Aggregations: &[]signoz.Querybuildertypesv5MetricAggregation{
				{
					MetricName:       new(info.name),
					TimeAggregation:  new(signoz.MetrictypesTimeAggregation(timeAgg)),
					SpaceAggregation: new(signoz.MetrictypesSpaceAggregation(spaceAgg)),
				},
			},
			Filter: &signoz.Querybuildertypesv5Filter{Expression: &filterExpr},
			GroupBy: &[]signoz.Querybuildertypesv5GroupByKey{
				{Name: "k8s.pod.name", FieldContext: new(signoz.Resource)},
			},
			StepInterval: stepInterval,
			Legend:       new("{{k8s.pod.name}}"),
			Disabled:     new(false),
		}

		envelope := signoz.Querybuildertypesv5QueryEnvelope{}
		if err := envelope.FromQuerybuildertypesv5QueryEnvelopeBuilderMetric(signoz.Querybuildertypesv5QueryEnvelopeBuilderMetric{
			Type: new(signoz.BuilderQuery),
			Spec: &spec,
		}); err != nil {
			return nil, nil, fmt.Errorf("encode query envelope: %w", err)
		}
		queries = append(queries, envelope)
	}

	return queries, queryToAgg, nil
}

func (sc *SigNozClient) GetLogs(ctx context.Context, _organizationID, _cellID string, start, end time.Time, branchID string, userFilters []LogFilter, limit int, cursor string) (*BranchLogs, error) {
	exprs, err := compileSigNozLogFilters(branchID, userFilters)
	if err != nil {
		return nil, fmt.Errorf("compile log filters: %w", err)
	}

	reqBody, err := buildLogsReq(sc.clustersNamespace, start, end, exprs, limit, cursor)
	if err != nil {
		return nil, fmt.Errorf("build logs request: %w", err)
	}

	results, err := sc.queryRange(ctx, reqBody)
	if err != nil {
		return nil, fmt.Errorf("query range: %w", err)
	}

	logs, nextCursor, err := parseLogsResults(results)
	if err != nil {
		return nil, fmt.Errorf("parse logs results: %w", err)
	}

	return &BranchLogs{
		Start:      start,
		End:        end,
		Logs:       logs,
		NextCursor: nextCursor,
	}, nil
}

func parseLogsResults(results []any) ([]LogEntry, *string, error) {
	logs := []LogEntry{}
	if len(results) == 0 {
		return logs, nil, nil
	}

	parsed, err := decodeResults[signoz.Querybuildertypesv5RawData](results)
	if err != nil {
		return nil, nil, fmt.Errorf("decode log results: %w", err)
	}

	var nextCursor *string
	if parsed[0].NextCursor != nil && *parsed[0].NextCursor != "" {
		nextCursor = parsed[0].NextCursor
	}

	for _, result := range parsed {
		for _, row := range ptr.Deref(result.Rows, nil) {
			if entry, ok := parseLogRow(row); ok {
				logs = append(logs, entry)
			}
		}
	}

	return logs, nextCursor, nil
}

var schemaLevelToSeverities = map[string][]string{
	"debug":   {"DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5"},
	"info":    {"INFO", "LOG", "NOTICE"},
	"warning": {"WARN", "WARNING"},
	"error":   {"ERROR", "FATAL", "PANIC", "CRITICAL"},
}

var severityToLevel = invertSeverityMap(schemaLevelToSeverities)

func invertSeverityMap(m map[string][]string) map[string]string {
	out := make(map[string]string, len(m)*4)
	for level, severities := range m {
		for _, s := range severities {
			out[s] = level
		}
	}

	return out
}

// Unknown levels are skipped because validation happens at the API edge.
func ExpandLevels(levels []string) []string {
	if len(levels) == 0 {
		return nil
	}

	out := make([]string, 0, len(levels)*2)
	for _, lvl := range levels {
		out = append(out, schemaLevelToSeverities[lvl]...)
	}

	return out
}

// CNPG wraps postgres CSV records as `{...,"record":{"message":"..."}}` and its own
// lifecycle logs as `{...,"msg":"..."}`. Falls back to the original on miss.
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

// Returns false when the row lacks the minimum data required to render a log entry (timestamp + non-empty body).
func parseLogRow(row signoz.Querybuildertypesv5RawRow) (LogEntry, bool) {
	if row.Timestamp == nil || row.Data == nil {
		return LogEntry{}, false
	}

	data := *row.Data
	message, _ := data["body"].(string)
	if message == "" {
		return LogEntry{}, false
	}
	message = unwrapCNPGBody(message)

	resources, _ := data["resources_string"].(map[string]any)
	instanceID, _ := resources["k8s.pod.name"].(string)

	entry := LogEntry{
		Timestamp:  *row.Timestamp,
		InstanceID: instanceID,
		Message:    message,
	}
	if severity, _ := data["severity_text"].(string); severity != "" {
		// SigNoz can ingest mixed-case severities depending on collector
		// config; normalise so we don't silently miss "info" (lowercase)
		// while observability/logs.go matches "INFO".
		if level, ok := severityToLevel[strings.ToUpper(severity)]; ok {
			entry.Level = &level
		}
	}
	attrs, _ := data["attributes_string"].(map[string]any)
	if process, ok := attrs["backend_type"].(string); ok && process != "" {
		entry.Process = &process
	}
	return entry, true
}

func buildLogsReq(clustersNamespace string, start, end time.Time, userFilters []filter.Expr, limit int, cursor string) (signoz.QueryRangeV5JSONRequestBody, error) {
	step := calculateStep(start, end)
	filterExpr := buildLogsFilterExpression(clustersNamespace, userFilters)

	envelope, err := buildLogQuery(step, filterExpr, limit, cursor)
	if err != nil {
		return signoz.QueryRangeV5JSONRequestBody{}, err
	}

	return buildRequestBody(start, end, []signoz.Querybuildertypesv5QueryEnvelope{envelope}, signoz.Raw), nil
}

func buildLogsFilterExpression(namespace string, userFilters []filter.Expr) string {
	exprs := []filter.Expr{
		filter.Eq("k8s.namespace.name", namespace),
		filter.Eq("k8s.container.name", "postgres"),
		filter.Eq("logger", "postgres"),
	}
	exprs = append(exprs, userFilters...)

	return filter.And(exprs...).Render()
}

func buildLogQuery(step int, filterExpr string, limit int, cursor string) (signoz.Querybuildertypesv5QueryEnvelope, error) {
	stepInterval, err := buildStepInterval(step)
	if err != nil {
		return signoz.Querybuildertypesv5QueryEnvelope{}, err
	}

	queryName := "A"
	spec := signoz.Querybuildertypesv5QueryBuilderQueryGithubComSigNozSignozPkgTypesQuerybuildertypesQuerybuildertypesv5LogAggregation{
		Name:   &queryName,
		Signal: new(signoz.Logs),
		Filter: &signoz.Querybuildertypesv5Filter{Expression: &filterExpr},
		Order: &[]signoz.Querybuildertypesv5OrderBy{
			{
				Key:       &signoz.Querybuildertypesv5OrderByKey{Name: "timestamp"},
				Direction: new(signoz.Desc),
			},
		},
		StepInterval: stepInterval,
		Limit:        new(limit),
		Disabled:     new(false),
	}
	if cursor != "" {
		spec.Cursor = &cursor
	}

	envelope := signoz.Querybuildertypesv5QueryEnvelope{}
	if err := envelope.FromQuerybuildertypesv5QueryEnvelopeBuilderLog(signoz.Querybuildertypesv5QueryEnvelopeBuilderLog{
		Type: new(signoz.BuilderQuery),
		Spec: &spec,
	}); err != nil {
		return signoz.Querybuildertypesv5QueryEnvelope{}, fmt.Errorf("encode query envelope: %w", err)
	}

	return envelope, nil
}

// buildRequestBody wraps query envelopes into a v5 request body of the given request type.
func buildRequestBody(start, end time.Time, queries []signoz.Querybuildertypesv5QueryEnvelope, requestType signoz.Querybuildertypesv5RequestType) signoz.QueryRangeV5JSONRequestBody {
	return signoz.QueryRangeV5JSONRequestBody{
		Start:          new(int(start.UnixMilli())),
		End:            new(int(end.UnixMilli())),
		CompositeQuery: &signoz.Querybuildertypesv5CompositeQuery{Queries: &queries},
		RequestType:    &requestType,
		SchemaVersion:  new("v1"),
	}
}

// buildPodFilters returns the standard k8s pod and namespace filter expressions.
// The pod filter is always emitted (even with an empty list) so callers default
// to "match no pods" rather than "match every pod in the namespace".
func buildPodFilters(namespace string, instances []string) []filter.Expr {
	return []filter.Expr{
		filter.MustIn("k8s.pod.name", instances),
		filter.Eq("k8s.namespace.name", namespace),
	}
}

// decodeResults reinterprets the untyped v5 result slice as a typed slice via JSON round-tripping.
func decodeResults[T any](results []any) ([]T, error) {
	data, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("marshal results: %w", err)
	}

	var parsed []T
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal results: %w", err)
	}

	return parsed, nil
}

// calculateStep determines the step interval based on the time difference between start and end.
func calculateStep(start, end time.Time) int {
	diff := end.Sub(start)
	switch {
	case diff < 24*time.Hour:
		return int(time.Minute.Seconds()) // Less than a day, use 1 minute step
	case diff < 3*24*time.Hour:
		return int((15 * time.Minute).Seconds()) // Less than 3 days, use 15 minutes step
	case diff < 7*24*time.Hour:
		return int((30 * time.Minute).Seconds()) // Less than a week, use 30 minutes step
	case diff < 30*24*time.Hour:
		return int((time.Hour).Seconds()) // Less than a month, use 1 hour step
	default: // For longer periods, use 6 hours step
		return int(6 * time.Hour.Seconds())
	}
}

func buildStepInterval(step int) (*signoz.Querybuildertypesv5Step, error) {
	stepInterval := &signoz.Querybuildertypesv5Step{}
	if err := stepInterval.FromQuerybuildertypesv5Step1(float32(step)); err != nil {
		return nil, fmt.Errorf("encode step interval: %w", err)
	}

	return stepInterval, nil
}

// compileSigNozLogFilters translates the backend-neutral []LogFilter shape used
// by the projects handler into SigNoz filter expressions. The branch-scope
// predicate is always prepended so a caller cannot read other branches' logs
// by omitting the instance filter.
func compileSigNozLogFilters(branchID string, userFilters []LogFilter) ([]filter.Expr, error) {
	exprs := make([]filter.Expr, 0, len(userFilters)+1)
	exprs = append(exprs, branchScopeFilter(branchID))
	for _, f := range userFilters {
		expr, err := compileSigNozLogFilter(f)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

func compileSigNozLogFilter(f LogFilter) (filter.Expr, error) {
	switch f.Field {
	case "instance":
		if f.Op != "in" {
			return nil, fmt.Errorf("op [%s] not allowed for field [instance]", f.Op)
		}
		return filter.MustIn("k8s.pod.name", f.Values), nil
	case "level":
		if f.Op != "in" {
			return nil, fmt.Errorf("op [%s] not allowed for field [level]", f.Op)
		}
		return filter.In("severity_text", ExpandLevels(f.Values)), nil
	case "process":
		if f.Op != "in" {
			return nil, fmt.Errorf("op [%s] not allowed for field [process]", f.Op)
		}
		return filter.In("backend_type", f.Values), nil
	case "body":
		switch f.Op {
		case "contains":
			return filter.Contains("body", f.Value), nil
		case "icontains":
			return filter.IContains("body", f.Value), nil
		case "regex":
			return filter.Regexp("body", f.Value), nil
		case "iregex":
			return filter.Regexp("body", "(?i)"+f.Value), nil
		default:
			return nil, fmt.Errorf("op [%s] not allowed for field [body]", f.Op)
		}
	}
	return nil, fmt.Errorf("unknown field [%s]", f.Field)
}

package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/utils/ptr"

	"xata/internal/signoz"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	k8sNamespace = "xata-clusters"
	apiKey       = "test-api-key"
)

func TestGetMetric(t *testing.T) {
	tests := []struct {
		name               string
		metric             string
		branchID           string
		startTime          time.Time
		endTime            time.Time
		instances          []string
		aggregations       []string
		unit               string
		mockStatusCode     int
		mockResponseStatus string
		mockResults        []signoz.Querybuildertypesv5TimeSeriesData
		expectError        bool
		assertResult       func(t *testing.T, result *BranchMetrics)
		assertRequest      func(t *testing.T, req *signoz.Querybuildertypesv5QueryRangeRequest)
	}{
		{
			name:           "AVG CPU on single instance",
			metric:         "cpu",
			branchID:       "br-123",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1"},
			aggregations:   []string{"avg"},
			unit:           "percentage",
			mockStatusCode: 200,
			mockResults: []signoz.Querybuildertypesv5TimeSeriesData{
				{
					QueryName: new("A"),
					Aggregations: &[]signoz.Querybuildertypesv5AggregationBucket{
						{
							Series: &[]signoz.Querybuildertypesv5TimeSeries{
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-1"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{
											Timestamp: new(int64(1715000000000)),
											Value:     new(float64(42.5)),
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
			assertRequest: func(t *testing.T, req *signoz.Querybuildertypesv5QueryRangeRequest) {
				require.NotNil(t, req.CompositeQuery)
				require.NotNil(t, req.CompositeQuery.Queries)
				require.Len(t, *req.CompositeQuery.Queries, 1)
				spec := unwrapBuilderMetricSpec(t, (*req.CompositeQuery.Queries)[0])
				require.NotNil(t, spec.Filter)
				require.NotNil(t, spec.Filter.Expression)
				assert.Contains(t, *spec.Filter.Expression, `k8s.pod.name IN ["pod-1"]`)
				assert.Contains(t, *spec.Filter.Expression, `k8s.namespace.name = "xata-clusters"`)
				assert.Contains(t, *spec.Filter.Expression, `branch_id = "br-123"`)
				assert.Contains(t, *spec.Filter.Expression, `k8s.pod.name REGEXP "^br-123-"`, "legacy fallback for pre-branch_id rows")
			},
		},
		{
			name:           "MIN, MAX CPU on 2 instances",
			metric:         "cpu",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1", "pod-2"},
			aggregations:   []string{"min", "max"},
			unit:           "percentage",
			mockStatusCode: 200,
			mockResults: []signoz.Querybuildertypesv5TimeSeriesData{
				{
					QueryName: new("B"),
					Aggregations: &[]signoz.Querybuildertypesv5AggregationBucket{
						{
							Series: &[]signoz.Querybuildertypesv5TimeSeries{
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-1"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1746776340000)), Value: new(float64(0.004387936))},
										{Timestamp: new(int64(1746776400000)), Value: new(float64(0.004595945))},
									},
								},
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-2"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1746776340000)), Value: new(float64(0.0029397))},
										{Timestamp: new(int64(1746776400000)), Value: new(float64(0.002136112))},
									},
								},
							},
						},
					},
				},
				{
					QueryName: new("A"),
					Aggregations: &[]signoz.Querybuildertypesv5AggregationBucket{
						{
							Series: &[]signoz.Querybuildertypesv5TimeSeries{
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-1"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1746776340000)), Value: new(float64(0.004387936))},
										{Timestamp: new(int64(1746776400000)), Value: new(float64(0.004595945))},
									},
								},
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-2"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1746776340000)), Value: new(float64(0.0029397))},
										{Timestamp: new(int64(1746776400000)), Value: new(float64(0.002136112))},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
			assertRequest: func(t *testing.T, req *signoz.Querybuildertypesv5QueryRangeRequest) {
				require.NotNil(t, req.CompositeQuery)
				require.NotNil(t, req.CompositeQuery.Queries)
				require.Len(t, *req.CompositeQuery.Queries, 2)
				spec := unwrapBuilderMetricSpec(t, (*req.CompositeQuery.Queries)[0])
				require.NotNil(t, spec.Filter)
				require.NotNil(t, spec.Filter.Expression)
				assert.Contains(t, *spec.Filter.Expression, `k8s.pod.name IN ["pod-1", "pod-2"]`)
				assert.Contains(t, *spec.Filter.Expression, `k8s.namespace.name = "xata-clusters"`)
			},
		},
		{
			name:          "unknown metric name",
			metric:        "invalid_metric",
			instances:     []string{"pod-1"},
			aggregations:  []string{"avg"},
			expectError:   true,
			assertRequest: nil,
		},
		{
			name:           "Empty data response doesn't return nil series",
			metric:         "cpu",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1"},
			aggregations:   []string{"avg"},
			unit:           "percentage",
			mockStatusCode: 200,
			mockResults:    []signoz.Querybuildertypesv5TimeSeriesData{},
		},
		{
			name:           "non-200 status code returns error",
			metric:         "cpu",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1"},
			aggregations:   []string{"avg"},
			mockStatusCode: 500,
			expectError:    true,
		},
		{
			name:               "non-success status returns error",
			metric:             "cpu",
			startTime:          time.UnixMilli(1715000000000),
			endTime:            time.UnixMilli(1715010000000),
			instances:          []string{"pod-1"},
			aggregations:       []string{"avg"},
			mockStatusCode:     200,
			mockResponseStatus: "error",
			mockResults:        []signoz.Querybuildertypesv5TimeSeriesData{},
			expectError:        true,
		},
		{
			name:           "counter metric uses temporalAgg for time and user agg for space",
			metric:         "network_ingress",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1"},
			aggregations:   []string{"avg"},
			unit:           "bytes",
			mockStatusCode: 200,
			mockResults: []signoz.Querybuildertypesv5TimeSeriesData{
				{
					QueryName: new("A"),
					Aggregations: &[]signoz.Querybuildertypesv5AggregationBucket{
						{
							Series: &[]signoz.Querybuildertypesv5TimeSeries{
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-1"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1715000000000)), Value: new(float64(1024))},
									},
								},
							},
						},
					},
				},
			},
			assertRequest: func(t *testing.T, req *signoz.Querybuildertypesv5QueryRangeRequest) {
				require.NotNil(t, req.CompositeQuery)
				require.NotNil(t, req.CompositeQuery.Queries)
				require.Len(t, *req.CompositeQuery.Queries, 1)
				spec := unwrapBuilderMetricSpec(t, (*req.CompositeQuery.Queries)[0])
				require.NotNil(t, spec.Aggregations)
				aggs := *spec.Aggregations
				require.Len(t, aggs, 1)
				assert.Equal(t, signoz.MetrictypesTimeAggregation("increase"), *aggs[0].TimeAggregation)
				assert.Equal(t, signoz.MetrictypesSpaceAggregation("avg"), *aggs[0].SpaceAggregation)
				require.NotNil(t, spec.Filter)
				require.NotNil(t, spec.Filter.Expression)
				assert.Contains(t, *spec.Filter.Expression, `direction = "receive"`)
			},
		},
		{
			name:           "empty instances still includes pod name filter",
			metric:         "cpu",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      nil,
			aggregations:   []string{"avg"},
			unit:           "percentage",
			mockStatusCode: 200,
			mockResults:    []signoz.Querybuildertypesv5TimeSeriesData{},
			assertRequest: func(t *testing.T, req *signoz.Querybuildertypesv5QueryRangeRequest) {
				spec := unwrapBuilderMetricSpec(t, (*req.CompositeQuery.Queries)[0])
				require.NotNil(t, spec.Filter)
				require.NotNil(t, spec.Filter.Expression)
				assert.Contains(t, *spec.Filter.Expression, "k8s.pod.name IN []")
			},
		},
		{
			name:           "nil timestamp and value points are skipped",
			metric:         "cpu",
			startTime:      time.UnixMilli(1715000000000),
			endTime:        time.UnixMilli(1715010000000),
			instances:      []string{"pod-1"},
			aggregations:   []string{"avg"},
			unit:           "percentage",
			mockStatusCode: 200,
			mockResults: []signoz.Querybuildertypesv5TimeSeriesData{
				{
					QueryName: new("A"),
					Aggregations: &[]signoz.Querybuildertypesv5AggregationBucket{
						{
							Series: &[]signoz.Querybuildertypesv5TimeSeries{
								{
									Labels: &[]signoz.Querybuildertypesv5Label{
										{
											Key:   &signoz.TelemetrytypesTelemetryFieldKey{Name: "k8s.pod.name"},
											Value: labelValue("pod-1"),
										},
									},
									Values: &[]signoz.Querybuildertypesv5TimeSeriesValue{
										{Timestamp: new(int64(1715000000000)), Value: new(float64(1.0))},
										{Timestamp: nil, Value: new(float64(2.0))},
										{Timestamp: new(int64(1715000060000)), Value: nil},
										{Timestamp: new(int64(1715000120000)), Value: new(float64(3.0))},
									},
								},
							},
						},
					},
				},
			},
			assertResult: func(t *testing.T, result *BranchMetrics) {
				require.Len(t, result.Series, 1)
				assert.Equal(t, "pod-1", result.Series[0].InstanceID)
				require.Len(t, result.Series[0].Values, 2)
				assert.Equal(t, int64(1715000000000), result.Series[0].Values[0].Timestamp.UnixMilli())
				assert.InDelta(t, float32(1.0), result.Series[0].Values[0].Value, 0.00001)
				assert.Equal(t, int64(1715000120000), result.Series[0].Values[1].Timestamp.UnixMilli())
				assert.InDelta(t, float32(3.0), result.Series[0].Values[1].Value, 0.00001)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedReq *signoz.Querybuildertypesv5QueryRangeRequest

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/api/v5/query_range", r.URL.Path)
				assert.Equal(t, apiKey, r.Header.Get("SigNoz-Api-Key"))

				if tt.mockResults != nil {
					var req signoz.Querybuildertypesv5QueryRangeRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
					capturedReq = &req
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.mockStatusCode)
				if tt.mockResults != nil {
					results := make([]any, len(tt.mockResults))
					for i, r := range tt.mockResults {
						results[i] = r
					}
					status := tt.mockResponseStatus
					if status == "" {
						status = "success"
					}
					wrapper := map[string]any{
						"status": status,
						"data": map[string]any{
							"type": "time_series",
							"data": map[string]any{
								"results": results,
							},
						},
					}
					require.NoError(t, json.NewEncoder(w).Encode(wrapper))
				}
			}))
			defer server.Close()

			client, err := NewSigNozClient(server.URL, apiKey, k8sNamespace)
			require.NoError(t, err)

			result, err := client.GetMetric(context.Background(), "", "", tt.startTime, tt.endTime, tt.branchID, tt.metric, tt.instances, tt.aggregations)

			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, tt.metric, result.Metric)
			assert.Equal(t, tt.startTime, result.Start)
			assert.Equal(t, tt.endTime, result.End)
			assert.Equal(t, tt.unit, result.Unit)
			assert.NotNil(t, result.Series)

			if tt.assertResult != nil {
				tt.assertResult(t, result)
			} else {
				if len(result.Series) > 0 {
					assert.Equal(t, len(tt.aggregations)*len(tt.instances), len(result.Series))
				}

				assertSeriesMatchResults(t, result.Series, tt.mockResults)

				// Verify all returned series have valid aggregation values
				if len(result.Series) > 0 {
					aggSet := make(map[string]bool, len(tt.aggregations))
					for _, a := range tt.aggregations {
						aggSet[a] = true
					}
					for _, s := range result.Series {
						assert.True(t, aggSet[s.Aggregation], "unexpected aggregation: %s", s.Aggregation)
					}
				}
			}

			if tt.assertRequest != nil && capturedReq != nil {
				tt.assertRequest(t, capturedReq)
			}
		})
	}
}

func labelValue(s string) *interface{} {
	var v interface{} = s
	return &v
}

func unwrapBuilderMetricSpec(t *testing.T, env signoz.Querybuildertypesv5QueryEnvelope) signoz.Querybuildertypesv5QueryBuilderQueryGithubComSigNozSignozPkgTypesQuerybuildertypesQuerybuildertypesv5MetricAggregation {
	t.Helper()
	builder, err := env.AsQuerybuildertypesv5QueryEnvelopeBuilderMetric()
	require.NoError(t, err)
	require.NotNil(t, builder.Spec)
	return *builder.Spec
}

func assertSeriesMatchResults(t *testing.T, got []MetricSeries, want []signoz.Querybuildertypesv5TimeSeriesData) {
	t.Helper()
	idx := 0
	for _, res := range want {
		if res.Aggregations == nil {
			continue
		}
		for _, bucket := range *res.Aggregations {
			if bucket.Series == nil {
				continue
			}
			for _, ts := range *bucket.Series {
				require.Less(t, idx, len(got))
				if ts.Labels != nil {
					for _, label := range *ts.Labels {
						if label.Key != nil && label.Key.Name == "k8s.pod.name" && label.Value != nil {
							if s, ok := (*label.Value).(string); ok {
								assert.Equal(t, s, got[idx].InstanceID)
							}
						}
					}
				}
				if ts.Values != nil {
					require.Equal(t, len(*ts.Values), len(got[idx].Values))
					for k, v := range *ts.Values {
						assert.InDelta(t, float32(ptr.Deref(v.Value, 0)), got[idx].Values[k].Value, 0.00001)
						assert.Equal(t, ptr.Deref(v.Timestamp, 0), got[idx].Values[k].Timestamp.UnixMilli())
					}
				}
				idx++
			}
		}
	}
	require.Equal(t, idx, len(got))
}

func TestParseLogRow(t *testing.T) {
	tests := map[string]struct {
		row     signoz.Querybuildertypesv5RawRow
		wantOK  bool
		wantLog LogEntry
	}{
		"complete row": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":             "hello",
					"resources_string": map[string]any{"k8s.pod.name": "branch-1"},
					"severity_text":    "INFO",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp:  time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				InstanceID: "branch-1",
				Message:    "hello",
				Level:      new("info"),
			},
		},
		"postgres severity LOG normalizes to info": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":          "checkpoint complete",
					"severity_text": "LOG",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "checkpoint complete",
				Level:     new("info"),
			},
		},
		"postgres severity FATAL normalizes to error": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":          "boom",
					"severity_text": "FATAL",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "boom",
				Level:     new("error"),
			},
		},
		"unknown severity leaves level nil": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":          "hello",
					"severity_text": "UNRECOGNIZED",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "hello",
			},
		},
		"missing timestamp drops the row": {
			row: signoz.Querybuildertypesv5RawRow{
				Data: &map[string]any{"body": "hello", "k8s.pod.name": "branch-1"},
			},
			wantOK: false,
		},
		"missing data drops the row": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
			},
			wantOK: false,
		},
		"empty body drops the row": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data:      &map[string]any{"body": "", "k8s.pod.name": "branch-1"},
			},
			wantOK: false,
		},
		"missing pod name keeps the row with empty instance": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data:      &map[string]any{"body": "hello"},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp:  time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				InstanceID: "",
				Message:    "hello",
			},
		},
		"CNPG postgres CSV envelope unwraps record.message": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":          `{"level":"info","logger":"postgres","msg":"record","record":{"error_severity":"ERROR","message":"division by zero"}}`,
					"severity_text": "ERROR",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "division by zero",
				Level:     new("error"),
			},
		},
		"CNPG instance-manager flat JSON unwraps msg": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":          `{"level":"info","logger":"postgres","msg":"PostgreSQL is ready to accept connections"}`,
					"severity_text": "INFO",
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "PostgreSQL is ready to accept connections",
				Level:     new("info"),
			},
		},
		"non-JSON body passes through unchanged": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data:      &map[string]any{"body": "plain text log"},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   "plain text log",
			},
		},
		"JSON body without record.message or msg falls back to original": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data:      &map[string]any{"body": `{"level":"info","other":"value"}`},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp: time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				Message:   `{"level":"info","other":"value"}`,
			},
		},
		"missing severity leaves level nil": {
			row: signoz.Querybuildertypesv5RawRow{
				Timestamp: new(time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC)),
				Data: &map[string]any{
					"body":             "hello",
					"resources_string": map[string]any{"k8s.pod.name": "branch-1"},
				},
			},
			wantOK: true,
			wantLog: LogEntry{
				Timestamp:  time.Date(2026, 4, 27, 0, 0, 0, 0, time.UTC),
				InstanceID: "branch-1",
				Message:    "hello",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := parseLogRow(tt.row)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			require.Equal(t, tt.wantLog.Timestamp, got.Timestamp)
			require.Equal(t, tt.wantLog.InstanceID, got.InstanceID)
			require.Equal(t, tt.wantLog.Message, got.Message)
			require.Equal(t, ptr.Deref(tt.wantLog.Level, ""), ptr.Deref(got.Level, ""))
		})
	}
}

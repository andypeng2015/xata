package metrics

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/projects/cells"
)

// CellsClient is a metrics.Client that routes per-branch metric and log
// queries to the clusters gRPC service running in the branch's cell.
type CellsClient struct {
	cells cells.Cells
}

func NewCellsClient(c cells.Cells) *CellsClient {
	return &CellsClient{cells: c}
}

func (c *CellsClient) GetMetrics(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, metrics []string, instances, aggregations []string) ([]BranchMetrics, error) {
	conn, err := c.cells.GetCellConnection(ctx, organizationID, cellID)
	if err != nil {
		return nil, fmt.Errorf("get cell connection: %w", err)
	}
	defer conn.Close()

	resp, err := conn.GetBranchMetrics(ctx, &clustersv1.GetBranchMetricsRequest{
		BranchId:     branchID,
		Start:        timestamppb.New(start),
		End:          timestamppb.New(end),
		Metrics:      metrics,
		Instances:    instances,
		Aggregations: aggregations,
	})
	if err != nil {
		return nil, fmt.Errorf("get branch metrics: %w", err)
	}

	return convertBranchMetrics(resp), nil
}

func (c *CellsClient) GetLogs(ctx context.Context, organizationID, cellID string, start, end time.Time, branchID string, filters []LogFilter, limit int, cursor string) (*BranchLogs, error) {
	conn, err := c.cells.GetCellConnection(ctx, organizationID, cellID)
	if err != nil {
		return nil, fmt.Errorf("get cell connection: %w", err)
	}
	defer conn.Close()

	pbFilters := make([]*clustersv1.LogFilter, 0, len(filters))
	for _, f := range filters {
		pbFilters = append(pbFilters, &clustersv1.LogFilter{
			Field:  f.Field,
			Op:     f.Op,
			Values: f.Values,
			Value:  f.Value,
		})
	}

	resp, err := conn.GetBranchLogs(ctx, &clustersv1.GetBranchLogsRequest{
		BranchId: branchID,
		Start:    timestamppb.New(start),
		End:      timestamppb.New(end),
		Filters:  pbFilters,
		Limit:    int32(limit),
		Cursor:   cursor,
	})
	if err != nil {
		return nil, fmt.Errorf("get branch logs: %w", err)
	}

	return convertBranchLogs(resp), nil
}

func convertBranchMetrics(resp *clustersv1.GetBranchMetricsResponse) []BranchMetrics {
	out := make([]BranchMetrics, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		series := make([]MetricSeries, 0, len(r.GetSeries()))
		for _, s := range r.GetSeries() {
			values := make([]Values, 0, len(s.GetValues()))
			for _, v := range s.GetValues() {
				values = append(values, Values{Timestamp: v.GetTimestamp().AsTime(), Value: v.GetValue()})
			}
			series = append(series, MetricSeries{
				Aggregation: s.GetAggregation(),
				InstanceID:  s.GetInstanceId(),
				Values:      values,
			})
		}
		out = append(out, BranchMetrics{
			Metric: r.GetMetric(),
			Unit:   r.GetUnit(),
			Series: series,
		})
	}
	return out
}

func convertBranchLogs(resp *clustersv1.GetBranchLogsResponse) *BranchLogs {
	out := &BranchLogs{
		Start: resp.GetStart().AsTime(),
		End:   resp.GetEnd().AsTime(),
		Logs:  make([]LogEntry, 0, len(resp.GetLogs())),
	}
	if resp.NextCursor != nil && *resp.NextCursor != "" {
		c := *resp.NextCursor
		out.NextCursor = &c
	}
	for _, l := range resp.GetLogs() {
		entry := LogEntry{
			Timestamp:  l.GetTimestamp().AsTime(),
			InstanceID: l.GetInstanceId(),
			Message:    l.GetMessage(),
		}
		if l.Level != nil {
			lvl := *l.Level
			entry.Level = &lvl
		}
		if l.Process != nil {
			p := *l.Process
			entry.Process = &p
		}
		out.Logs = append(out.Logs, entry)
	}
	return out
}

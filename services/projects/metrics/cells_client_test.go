package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	"xata/services/projects/cells/cellsmock"
)

type fakeCellClient struct {
	clustersv1.ClustersServiceClient
	closed bool
}

func (f *fakeCellClient) Close() error { f.closed = true; return nil }

func TestCellsClient_GetMetric_RoundTrips(t *testing.T) {
	ctx := context.Background()
	mockCells := cellsmock.NewCells(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	cellClient := &fakeCellClient{ClustersServiceClient: mockClusters}

	mockCells.EXPECT().GetCellConnection(mock.Anything, "org-1", "cell-A").Return(cellClient, nil).Once()

	t1 := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	mockClusters.EXPECT().GetBranchMetrics(mock.Anything, mock.MatchedBy(func(req *clustersv1.GetBranchMetricsRequest) bool {
		return req.GetMetric() == "cpu" &&
			req.GetStart().AsTime().Equal(t1) &&
			req.GetEnd().AsTime().Equal(t2) &&
			len(req.GetInstances()) == 1 &&
			req.GetInstances()[0] == "br-1-0" &&
			len(req.GetAggregations()) == 1
	})).Return(&clustersv1.GetBranchMetricsResponse{
		Start:  timestamppb.New(t1),
		End:    timestamppb.New(t2),
		Metric: "cpu",
		Unit:   "percentage",
		Series: []*clustersv1.MetricSeries{{
			Aggregation: "avg",
			InstanceId:  "br-1-0",
			Values: []*clustersv1.MetricValue{
				{Timestamp: timestamppb.New(t1), Value: 0.42},
			},
		}},
	}, nil).Once()

	c := NewCellsClient(mockCells)
	got, err := c.GetMetric(ctx, "org-1", "cell-A", t1, t2, "br-1", "cpu", []string{"br-1-0"}, []string{"avg"})
	require.NoError(t, err)
	require.Equal(t, "cpu", got.Metric)
	require.Equal(t, "percentage", got.Unit)
	require.Len(t, got.Series, 1)
	require.Equal(t, "br-1-0", got.Series[0].InstanceID)
	require.True(t, cellClient.closed, "cell connection should be closed after the call")
}

func TestCellsClient_GetLogs_PassesFiltersAndCursor(t *testing.T) {
	ctx := context.Background()
	mockCells := cellsmock.NewCells(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	cellClient := &fakeCellClient{ClustersServiceClient: mockClusters}
	mockCells.EXPECT().GetCellConnection(mock.Anything, "org-1", "cell-A").Return(cellClient, nil).Once()

	t1 := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	mockClusters.EXPECT().GetBranchLogs(mock.Anything, mock.MatchedBy(func(req *clustersv1.GetBranchLogsRequest) bool {
		if req.GetBranchId() != "br-1" || req.GetCursor() != "opaque" || req.GetLimit() != 50 {
			return false
		}
		if len(req.GetFilters()) != 1 {
			return false
		}
		f := req.GetFilters()[0]
		return f.GetField() == "level" && f.GetOp() == "in" && len(f.GetValues()) == 1 && f.GetValues()[0] == "error"
	})).Return(&clustersv1.GetBranchLogsResponse{
		Start: timestamppb.New(t1),
		End:   timestamppb.New(t2),
		Logs: []*clustersv1.LogEntry{
			{Timestamp: timestamppb.New(t1), InstanceId: "br-1-0", Message: "boom"},
		},
	}, nil).Once()

	c := NewCellsClient(mockCells)
	res, err := c.GetLogs(ctx, "org-1", "cell-A", t1, t2, "br-1", []LogFilter{{Field: "level", Op: "in", Values: []string{"error"}}}, 50, "opaque")
	require.NoError(t, err)
	require.Len(t, res.Logs, 1)
	require.Equal(t, "boom", res.Logs[0].Message)
}

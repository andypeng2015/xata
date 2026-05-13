package observability

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// VMClient is a thin wrapper around the upstream Prometheus HTTP client.
// VictoriaMetrics speaks the Prometheus API verbatim, so the same client
// works against either backend. Implements MetricsBackend.
type VMClient struct {
	api v1.API
}

// NewVMClient targets the given VictoriaMetrics base URL,
// e.g. http://victoria-metrics.xata-observability.svc.cluster.local:8428.
// httpClient is optional; nil falls back to promapi's default client.
func NewVMClient(baseURL string, httpClient *http.Client) (*VMClient, error) {
	cfg := promapi.Config{Address: baseURL}
	if httpClient != nil {
		cfg.Client = httpClient
	}
	c, err := promapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("new prometheus client: %w", err)
	}
	return &VMClient{api: v1.NewAPI(c)}, nil
}

// QueryRange calls /api/v1/query_range and returns the matrix as PromSeries.
func (c *VMClient) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]PromSeries, error) {
	val, _, err := c.api.QueryRange(ctx, query, v1.Range{Start: start, End: end, Step: step})
	if err != nil {
		return nil, fmt.Errorf("query_range: %w", err)
	}
	matrix, ok := val.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %s, want matrix", val.Type())
	}

	out := make([]PromSeries, 0, len(matrix))
	for _, stream := range matrix {
		labels := make(map[string]string, len(stream.Metric))
		for k, v := range stream.Metric {
			labels[string(k)] = string(v)
		}
		samples := make([]PromSample, 0, len(stream.Values))
		for _, s := range stream.Values {
			f := float64(s.Value)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				continue
			}
			samples = append(samples, PromSample{Timestamp: s.Timestamp.Time().UTC(), Value: f})
		}
		out = append(out, PromSeries{Labels: labels, Values: samples})
	}
	return out, nil
}

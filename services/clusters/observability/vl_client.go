package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// VLClient is a thin VictoriaLogs HTTP client. Implements LogsBackend.
type VLClient struct {
	baseURL string
	http    *http.Client
}

// NewVLClient targets the given VictoriaLogs base URL,
// e.g. http://victoria-logs.xata-observability.svc.cluster.local:9428.
func NewVLClient(baseURL string, httpClient *http.Client) *VLClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &VLClient{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

// Query calls /select/logsql/query and decodes the JSON-stream response.
// VictoriaLogs returns one JSON object per line ordered newest-first.
func (c *VLClient) Query(ctx context.Context, lqlQuery string, start, end time.Time, limit int) ([]LogRow, error) {
	v := url.Values{}
	v.Set("query", lqlQuery)
	v.Set("start", start.UTC().Format(time.RFC3339Nano))
	v.Set("end", end.UTC().Format(time.RFC3339Nano))
	v.Set("limit", fmt.Sprintf("%d", limit))

	endpoint := c.baseURL + "/select/logsql/query?" + v.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Read up to 200 bytes for the error snippet to keep gRPC messages
		// from inflating.
		buf := make([]byte, 200)
		n, _ := resp.Body.Read(buf)
		return nil, fmt.Errorf("victoria-logs responded %d: %s", resp.StatusCode, string(buf[:n]))
	}

	var rows []LogRow
	dec := json.NewDecoder(resp.Body)
	for {
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode response: %w", err)
		}
		row := decodeRow(raw)
		if row.Timestamp.IsZero() || row.Message == "" {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeRow(raw map[string]any) LogRow {
	out := LogRow{}
	if ts, ok := raw["_time"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			out.Timestamp = t.UTC()
		}
	}
	if msg, ok := raw["_msg"].(string); ok {
		out.Message = msg
	} else if msg, ok := raw["body"].(string); ok {
		out.Message = msg
	}
	out.Pod = stringField(raw, "kubernetes.pod_name")
	out.Severity = stringField(raw, "severity_text")
	out.Process = stringField(raw, "backend_type")
	return out
}

func stringField(raw map[string]any, key string) string {
	if v, ok := raw[key].(string); ok {
		return v
	}
	return ""
}

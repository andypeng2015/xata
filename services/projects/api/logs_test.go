package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"xata/services/projects/api/spec"
	"xata/services/projects/metrics"
)

const testBranch = "br-test"

func TestValidateLogFilter_Valid(t *testing.T) {
	tests := map[string]struct {
		filter spec.LogFilter
		want   metrics.LogFilter
	}{
		"instance in": {
			filter: spec.LogFilter{Field: spec.Instance, Op: spec.In, Values: new([]string{"br-test-0", "br-test-1"})},
			want:   metrics.LogFilter{Field: "instance", Op: "in", Values: []string{"br-test-0", "br-test-1"}},
		},
		"level in": {
			filter: spec.LogFilter{Field: spec.Level, Op: spec.In, Values: new([]string{"error", "warning"})},
			want:   metrics.LogFilter{Field: "level", Op: "in", Values: []string{"error", "warning"}},
		},
		"process in": {
			filter: spec.LogFilter{Field: spec.Process, Op: spec.In, Values: new([]string{"checkpointer"})},
			want:   metrics.LogFilter{Field: "process", Op: "in", Values: []string{"checkpointer"}},
		},
		"body contains": {
			filter: spec.LogFilter{Field: spec.Body, Op: spec.Contains, Value: new("checkpoint")},
			want:   metrics.LogFilter{Field: "body", Op: "contains", Value: "checkpoint"},
		},
		"body icontains": {
			filter: spec.LogFilter{Field: spec.Body, Op: spec.Icontains, Value: new("checkpoint")},
			want:   metrics.LogFilter{Field: "body", Op: "icontains", Value: "checkpoint"},
		},
		"body regex": {
			filter: spec.LogFilter{Field: spec.Body, Op: spec.Regex, Value: new("^conn")},
			want:   metrics.LogFilter{Field: "body", Op: "regex", Value: "^conn"},
		},
		"body iregex": {
			filter: spec.LogFilter{Field: spec.Body, Op: spec.Iregex, Value: new("^conn")},
			want:   metrics.LogFilter{Field: "body", Op: "iregex", Value: "^conn"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := validateLogFilter(testBranch, 0, tt.filter)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidateLogFilter_Rejected(t *testing.T) {
	longValue := strings.Repeat("a", maxLogFilterValueLen+1)
	tooManyValues := make([]string, maxLogFilterValuesPerList+1)
	for i := range tooManyValues {
		tooManyValues[i] = "v"
	}

	tests := map[string]struct {
		filter    spec.LogFilter
		wantParam string
	}{
		"instance rejects non-in op": {
			filter:    spec.LogFilter{Field: spec.Instance, Op: spec.Contains, Value: new("x")},
			wantParam: "filters[0].op",
		},
		"in op requires non-empty values": {
			filter:    spec.LogFilter{Field: spec.Instance, Op: spec.In, Values: new([]string{})},
			wantParam: "filters[0].values",
		},
		"in op rejects too many values": {
			filter:    spec.LogFilter{Field: spec.Process, Op: spec.In, Values: new(tooManyValues)},
			wantParam: "filters[0].values",
		},
		"in op rejects a stray value": {
			filter:    spec.LogFilter{Field: spec.Instance, Op: spec.In, Values: new([]string{"x"}), Value: new("y")},
			wantParam: "filters[0].value",
		},
		"level rejects unknown level name": {
			filter:    spec.LogFilter{Field: spec.Level, Op: spec.In, Values: new([]string{"trace"})},
			wantParam: "filters[0].values",
		},
		"body requires a non-empty value": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.Contains, Value: new("")},
			wantParam: "filters[0].value",
		},
		"body rejects an over-long value": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.Contains, Value: new(longValue)},
			wantParam: "filters[0].value",
		},
		"body rejects values alongside a scalar op": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.Contains, Value: new("x"), Values: new([]string{"y"})},
			wantParam: "filters[0].values",
		},
		"body rejects the in op": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.In, Values: new([]string{"x"})},
			wantParam: "filters[0].value",
		},
		"body rejects an invalid regex": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.Regex, Value: new("(")},
			wantParam: "filters[0].value",
		},
		"unknown op is rejected": {
			filter:    spec.LogFilter{Field: spec.Body, Op: spec.LogFilterOp("eq"), Value: new("x")},
			wantParam: "filters[0].op",
		},
		"unknown field is rejected": {
			filter:    spec.LogFilter{Field: spec.LogFilterField("trace_id"), Op: spec.Contains, Value: new("x")},
			wantParam: "filters[0].field",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := validateLogFilter(testBranch, 0, tt.filter)
			require.Error(t, err)
			var invalid ErrorInvalidParam
			require.ErrorAs(t, err, &invalid)
			require.Equal(t, tt.wantParam, invalid.Param)
		})
	}
}

func TestValidateLogFilters(t *testing.T) {
	t.Run("aggregates valid filters in order", func(t *testing.T) {
		got, err := validateLogFilters(testBranch, []spec.LogFilter{
			{Field: spec.Instance, Op: spec.In, Values: new([]string{"br-test-0"})},
			{Field: spec.Body, Op: spec.Icontains, Value: new("slow")},
		})
		require.NoError(t, err)
		require.Equal(t, []metrics.LogFilter{
			{Field: "instance", Op: "in", Values: []string{"br-test-0"}},
			{Field: "body", Op: "icontains", Value: "slow"},
		}, got)
	})

	t.Run("rejects more than the filter cap", func(t *testing.T) {
		filters := make([]spec.LogFilter, maxLogFilters+1)
		for i := range filters {
			filters[i] = spec.LogFilter{Field: spec.Body, Op: spec.Contains, Value: new("x")}
		}
		_, err := validateLogFilters(testBranch, filters)
		require.Error(t, err)
		var invalid ErrorInvalidParam
		require.ErrorAs(t, err, &invalid)
		require.Equal(t, "filters", invalid.Param)
	})

	t.Run("surfaces the offending filter index", func(t *testing.T) {
		_, err := validateLogFilters(testBranch, []spec.LogFilter{
			{Field: spec.Instance, Op: spec.In, Values: new([]string{"br-test-0"})},
			{Field: spec.Body, Op: spec.Contains, Value: new("")},
		})
		require.Error(t, err)
		var invalid ErrorInvalidParam
		require.ErrorAs(t, err, &invalid)
		require.Equal(t, "filters[1].value", invalid.Param)
	})
}

func TestValidateLogsLimit(t *testing.T) {
	tests := map[string]struct {
		limit   *int
		wantErr bool
	}{
		"nil limit defaults later":  {limit: nil, wantErr: false},
		"in-range limit":            {limit: new(500), wantErr: false},
		"max limit allowed":         {limit: new(MaxLogLimit), wantErr: false},
		"zero is rejected":          {limit: new(0), wantErr: true},
		"negative is rejected":      {limit: new(-1), wantErr: true},
		"above the cap is rejected": {limit: new(MaxLogLimit + 1), wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateLogsLimit(testBranch, tt.limit)
			if tt.wantErr {
				var invalid ErrorInvalidParam
				require.ErrorAs(t, err, &invalid)
				require.Equal(t, "limit", invalid.Param)
				return
			}
			require.NoError(t, err)
		})
	}
}

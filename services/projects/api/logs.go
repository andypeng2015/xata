package api

import (
	"fmt"
	"regexp"

	"k8s.io/utils/ptr"

	"xata/services/projects/api/spec"
	"xata/services/projects/metrics"
)

const (
	DefaultLogLimit = 100
	MaxLogLimit     = 1000

	maxLogFilters             = 16
	maxLogFilterValueLen      = 1024
	maxLogFilterValuesPerList = 100
)

var validLogLevels = map[spec.LogLevel]struct{}{
	spec.Debug:   {},
	spec.Info:    {},
	spec.Warning: {},
	spec.Error:   {},
}

func validateLogsLimit(branchID string, limit *int) error {
	if limit != nil && (*limit < 1 || *limit > MaxLogLimit) {
		return ErrorInvalidParam{BranchName: branchID, Param: "limit", Message: fmt.Sprintf("limit must be between 1 and %d", MaxLogLimit)}
	}
	return nil
}

// validateLogFilters checks user-supplied filters and produces a backend-neutral
// []metrics.LogFilter. Each metrics.Client implementation is responsible for
// translating these into its own backend's query language and for enforcing the
// branch-scope on pod names server-side.
func validateLogFilters(branchID string, filters []spec.LogFilter) ([]metrics.LogFilter, error) {
	if len(filters) > maxLogFilters {
		return nil, ErrorInvalidParam{BranchName: branchID, Param: "filters", Message: fmt.Sprintf("at most %d filters are allowed", maxLogFilters)}
	}

	out := make([]metrics.LogFilter, 0, len(filters))
	for i, f := range filters {
		validated, err := validateLogFilter(branchID, i, f)
		if err != nil {
			return nil, err
		}
		out = append(out, validated)
	}
	return out, nil
}

func validateLogFilter(branchID string, idx int, f spec.LogFilter) (metrics.LogFilter, error) {
	switch f.Field {
	case spec.Instance:
		values, err := requireInValues(branchID, idx, f)
		if err != nil {
			return metrics.LogFilter{}, err
		}
		return metrics.LogFilter{Field: "instance", Op: "in", Values: values}, nil

	case spec.Level:
		values, err := requireInValues(branchID, idx, f)
		if err != nil {
			return metrics.LogFilter{}, err
		}
		for _, v := range values {
			if _, ok := validLogLevels[spec.LogLevel(v)]; !ok {
				return metrics.LogFilter{}, ErrorInvalidParam{BranchName: branchID, Param: valuesParam(idx), Message: fmt.Sprintf("invalid log level [%s]", v)}
			}
		}
		return metrics.LogFilter{Field: "level", Op: "in", Values: values}, nil

	case spec.Process:
		values, err := requireInValues(branchID, idx, f)
		if err != nil {
			return metrics.LogFilter{}, err
		}
		return metrics.LogFilter{Field: "process", Op: "in", Values: values}, nil

	case spec.Body:
		value, err := requireValue(branchID, idx, f)
		if err != nil {
			return metrics.LogFilter{}, err
		}
		switch f.Op {
		case spec.Contains:
			return metrics.LogFilter{Field: "body", Op: "contains", Value: value}, nil
		case spec.Icontains:
			return metrics.LogFilter{Field: "body", Op: "icontains", Value: value}, nil
		case spec.Regex, spec.Iregex:
			if _, err := regexp.Compile(value); err != nil {
				return metrics.LogFilter{}, ErrorInvalidParam{BranchName: branchID, Param: valueParam(idx), Message: fmt.Sprintf("invalid regex: %s", err)}
			}
			op := "regex"
			if f.Op == spec.Iregex {
				op = "iregex"
			}
			return metrics.LogFilter{Field: "body", Op: op, Value: value}, nil
		default:
			return metrics.LogFilter{}, ErrorInvalidParam{BranchName: branchID, Param: fmt.Sprintf("filters[%d].op", idx), Message: fmt.Sprintf("op [%s] not allowed for field [%s]", f.Op, f.Field)}
		}
	}
	return metrics.LogFilter{}, ErrorInvalidParam{BranchName: branchID, Param: fmt.Sprintf("filters[%d].field", idx), Message: fmt.Sprintf("unknown field [%s]", f.Field)}
}

func requireInValues(branchID string, idx int, f spec.LogFilter) ([]string, error) {
	if f.Op != spec.In {
		return nil, ErrorInvalidParam{BranchName: branchID, Param: fmt.Sprintf("filters[%d].op", idx), Message: fmt.Sprintf("op [%s] not allowed for field [%s]", f.Op, f.Field)}
	}
	values := ptr.Deref(f.Values, nil)
	if len(values) == 0 {
		return nil, ErrorInvalidParam{BranchName: branchID, Param: valuesParam(idx), Message: "values must be non-empty for op [in]"}
	}
	if len(values) > maxLogFilterValuesPerList {
		return nil, ErrorInvalidParam{BranchName: branchID, Param: valuesParam(idx), Message: fmt.Sprintf("at most %d values are allowed", maxLogFilterValuesPerList)}
	}
	if f.Value != nil {
		return nil, ErrorInvalidParam{BranchName: branchID, Param: valueParam(idx), Message: "value must be unset for op [in]"}
	}
	return values, nil
}

func requireValue(branchID string, idx int, f spec.LogFilter) (string, error) {
	value := ptr.Deref(f.Value, "")
	if value == "" {
		return "", ErrorInvalidParam{BranchName: branchID, Param: valueParam(idx), Message: fmt.Sprintf("value must be non-empty for op [%s]", f.Op)}
	}
	if len(value) > maxLogFilterValueLen {
		return "", ErrorInvalidParam{BranchName: branchID, Param: valueParam(idx), Message: fmt.Sprintf("value must be at most %d characters", maxLogFilterValueLen)}
	}
	if len(ptr.Deref(f.Values, nil)) > 0 {
		return "", ErrorInvalidParam{BranchName: branchID, Param: valuesParam(idx), Message: fmt.Sprintf("values must be unset for op [%s]", f.Op)}
	}
	return value, nil
}

func valueParam(idx int) string  { return fmt.Sprintf("filters[%d].value", idx) }
func valuesParam(idx int) string { return fmt.Sprintf("filters[%d].values", idx) }

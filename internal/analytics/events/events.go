package events

import "time"

type Event struct {
	Name       string
	Properties map[string]any
	OrgID      string
	Timestamp  time.Time
}

func NewOrganizationCreatedEvent(organizationID string) Event {
	return Event{
		Name:  "organization created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
		},
	}
}

func NewProjectCreatedEvent(organizationID, projectID string) Event {
	return Event{
		Name:  "project created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
		},
	}
}

func NewProjectDeletedEvent(organizationID, projectID string) Event {
	return Event{
		Name:  "project deleted",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
		},
	}
}

func NewBranchFromConfigurationEvent(organizationID, projectID, branchID, region string, image, instanceType string, replicas int, storageSize *int32) Event {
	props := map[string]any{
		"organization":  organizationID,
		"project":       projectID,
		"branch":        branchID,
		"region":        region,
		"child_branch":  false,
		"image":         image,
		"instance_type": instanceType,
		"replicas":      replicas,
	}

	if storageSize != nil {
		props["storage_size"] = int(*storageSize)
	}

	return Event{
		Name:       "branch created",
		OrgID:      organizationID,
		Properties: props,
	}
}

func NewBranchFromParentEvent(organizationID, projectID, parentID, branchID, region string) Event {
	return Event{
		Name:  "branch created",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
			"branch":       branchID,
			"region":       region,
			"parent_id":    parentID,
			"child_branch": true,
		},
	}
}

func NewBranchDeletedEvent(organizationID, projectID, branchID string) Event {
	return Event{
		Name:  "branch deleted",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"project":      projectID,
			"branch":       branchID,
		},
	}
}

func NewProjectUpdatedEvent(organizationID, projectID string, changedFields []string, newValues map[string]any) Event {
	return Event{
		Name:  "project updated",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":   organizationID,
			"project":        projectID,
			"changed_fields": changedFields,
			"new_values":     newValues,
		},
	}
}

func NewBranchUpdatedEvent(organizationID, projectID, branchID string, changedFields []string, newValues map[string]any) Event {
	return Event{
		Name:  "branch updated",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":   organizationID,
			"project":        projectID,
			"branch":         branchID,
			"changed_fields": changedFields,
			"new_values":     newValues,
		},
	}
}

func NewPaymentMethodAttachedEvent(organizationID, provider string) Event {
	return Event{
		Name:  "payment method attached",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization": organizationID,
			"provider":     provider,
		},
	}
}

func NewInvoicePaidEvent(organizationID, marketplace, currency string, amountDue, total float64, paidAt time.Time) Event {
	return Event{
		Name:      "invoice paid",
		OrgID:     organizationID,
		Timestamp: paidAt,
		Properties: map[string]any{
			"organization": organizationID,
			"marketplace":  marketplace,
			"amount_due":   amountDue,
			"total":        total,
			"currency":     currency,
		},
	}
}

func NewMemberInvitedEvent(organizationID, email string) Event {
	return Event{
		Name:  "member invited",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":  organizationID,
			"invitee_email": email,
		},
	}
}

func NewBranchRestoredFromBackupEvent(organizationID, projectID, sourceBranchID, newBranchID string) Event {
	return Event{
		Name:  "branch restored from backup",
		OrgID: organizationID,
		Properties: map[string]any{
			"organization":    organizationID,
			"project":         projectID,
			"source_branch":   sourceBranchID,
			"restored_branch": newBranchID,
		},
	}
}

// CostSummaryMetric is used for synthetic PostHog summary events,
// not events generated directly by user interactions.
type CostSummaryMetric struct {
	AllTime     float64
	SevenDay    float64
	CostAllTime float64
	Cost7day    float64
}

type ActivationSummaryMetrics struct {
	TotalBranchesAllTime       int
	AiBranchesAllTime          int
	NonConsoleBranchesAllTime  int
	CliBranchesAllTime         int
	CiBranchesAllTime          int
	TotalBranches7day          int
	AiBranches7day             int
	NonConsoleBranches7day     int
	CliBranches7day            int
	CiBranches7day             int
	CostMetrics                map[string]CostSummaryMetric
	PaidInvoiceCount           int
	HasPaymentMethod           bool
	HasPaymentMethodAddedEvent bool
}

// This is a summary event that we generate from data warehouse data
func NewActivationSummaryEvent(organizationID string, metrics ActivationSummaryMetrics) Event {
	properties := map[string]any{
		"organization":               organizationID,
		"totalBranchesAllTime":       metrics.TotalBranchesAllTime,
		"aiBranchesAllTime":          metrics.AiBranchesAllTime,
		"nonConsoleBranchesAllTime":  metrics.NonConsoleBranchesAllTime,
		"cliBranchesAllTime":         metrics.CliBranchesAllTime,
		"ciBranchesAllTime":          metrics.CiBranchesAllTime,
		"totalBranches7day":          metrics.TotalBranches7day,
		"aiBranches7day":             metrics.AiBranches7day,
		"nonConsoleBranches7day":     metrics.NonConsoleBranches7day,
		"cliBranches7day":            metrics.CliBranches7day,
		"ciBranches7day":             metrics.CiBranches7day,
		"paidInvoiceCount":           metrics.PaidInvoiceCount,
		"hasPaymentMethod":           metrics.HasPaymentMethod,
		"hasPaymentMethodAddedEvent": metrics.HasPaymentMethodAddedEvent,
	}
	addCostSummaryProperties(properties, metrics.CostMetrics)
	return Event{
		Name:       "summary: activation",
		OrgID:      organizationID,
		Properties: properties,
	}
}

func addCostSummaryProperties(properties map[string]any, metrics map[string]CostSummaryMetric) {
	// This event creates properties for multiple orb "billable metrics" so that as we extend billing system to
	// add more billable metrics they will automatically be added to this event
	for metricName, metric := range metrics {
		properties[metricName+"AllTime"] = metric.AllTime
		properties[metricName+"7day"] = metric.SevenDay
		properties[metricName+"CostAllTime"] = metric.CostAllTime
		properties[metricName+"Cost7day"] = metric.Cost7day
	}
}

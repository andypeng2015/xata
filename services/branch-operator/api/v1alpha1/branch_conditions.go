package v1alpha1

// Condition types for Branch `status.conditions` fields
const (
	BranchReadyConditionType = "Ready"
)

// Reason strings for Branch conditions
const (
	// Reason strings for the `Ready` True condition
	ResourcesReadyReason = "ResourcesReady"

	// Reason strings for the `Ready` False condition
	ParentBranchNotFoundReason     = "ParentBranchNotFound"
	ParentBranchHasNoXVolReason    = "ParentBranchHasNoXVol"
	ParentClusterNotFoundReason    = "ParentClusterNotFound"
	ParentBranchHasNoClusterReason = "ParentBranchHasNoCluster"
	ParentClusterUnhealthyReason   = "ParentClusterUnhealthy"
	ParentClusterPVCNotFoundReason = "ParentClusterPVCNotFound"
	ReconciliationFailedReason     = "ReconciliationFailed"
	XVolNotFoundReason             = "XVolNotFound"

	// Reason strings for the `Ready` Unknown condition
	ReconciliationPausedReason = "ReconciliationPaused"

	// Shared reason strings for multiple conditions
	AwaitingReconciliationReason = "AwaitingReconciliation"
)

// BranchConditionMessages maps condition reasons to human-readable messages
var BranchConditionMessages = map[string]string{
	// Messages for the BranchReady condition
	ResourcesReadyReason:           "All resources created",
	ParentBranchNotFoundReason:     "The parent branch was not found",
	ParentBranchHasNoXVolReason:    "The parent branch has no XVol",
	ParentClusterNotFoundReason:    "The parent cluster for the branch was not found",
	ParentBranchHasNoClusterReason: "The parent branch has no cluster",
	ParentClusterUnhealthyReason:   "The parent cluster for the branch is not healthy",
	ParentClusterPVCNotFoundReason: "The PVC for the parent cluster was not found",
	AwaitingReconciliationReason:   "The branch is awaiting reconciliation",
	ReconciliationPausedReason:     "Reconciliation has been paused",
	ReconciliationFailedReason:     "An error occurred during reconciliation",
	XVolNotFoundReason:             "No XVol found for the primary volume",
}

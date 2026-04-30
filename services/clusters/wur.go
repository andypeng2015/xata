package clusters

import (
	"context"
	"fmt"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/branch-operator/api/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// createWakeupRequestFromUpdateClusterRequest creates a WakeupRequest for the branch if the
// `UpdatePostgresClusterRequest` is waking a branch that uses pool
// hibernation.
func (c *ClustersService) createWakeupRequestFromUpdateClusterRequest(
	ctx context.Context,
	branch *v1alpha1.Branch,
	req *clustersv1.UpdatePostgresClusterRequest,
) error {
	// If the update doesn't modify hibernation status, no WakeupRequest is
	// needed
	if req.UpdateConfiguration.Hibernate == nil {
		return nil
	}

	// If the update is hibernating the branch, no WakeupRequest is needed
	if req.UpdateConfiguration.GetHibernate() {
		return nil
	}

	// If the branch does not use pool hibernation, no WakeupRequest is needed
	if !branch.HasWakeupPoolAnnotation() {
		return nil
	}

	// Ensure that a WakeupRequest exists for the branch
	return c.ensureWakeupRequest(ctx, branch.Name)
}

// createWakeupRequestForNewBranch creates a WakeupRequest for a newly created
// branch if it requires one.
func (c *ClustersService) createWakeupRequestForNewBranch(ctx context.Context, branch *v1alpha1.Branch) error {
	// If the branch does not use the xvol clone restore type, no WakeupRequest
	// is needed
	if !branch.Spec.Restore.IsXVolCloneType() {
		return nil
	}

	// Ensure that a WakeupRequest exists for the branch
	return c.ensureWakeupRequest(ctx, branch.Name)
}

// ensureWakeupRequest ensures that a WakeupRequest exists for the given
// branch. If a WakeupRequest already exists for the branch, it checks its
// status. If the existing WakeupRequest is still in progress, it returns
// without creating a new one. If the existing WakeupRequest has succeeded or
// failed, it deletes it and creates a new one.
func (c *ClustersService) ensureWakeupRequest(ctx context.Context, branchName string) error {
	// Check for a WakeupRequest for this branch
	wur := &v1alpha1.WakeupRequest{}
	err := c.kubeClient.Get(ctx, types.NamespacedName{
		Name:      branchName,
		Namespace: c.config.ClustersNamespace,
	}, wur)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("get wakeup request: %w", err)
	}

	// If a WakeupRequest exists, check its status. If it's still in progress,
	// return without creating a new one. If it has succeeded or failed, delete
	// it so we can create a fresh one.
	if err == nil {
		cond := meta.FindStatusCondition(wur.Status.Conditions, v1alpha1.WakeupSucceededConditionType)

		// If the Succeeded condition is Unknown the wakeup is still in progress,
		// so return without creating a new one
		if cond == nil || cond.Status == metav1.ConditionUnknown {
			return nil
		}

		// Otherwise, delete the existing WakeupRequest so we can create a new one
		if err := c.kubeClient.Delete(ctx, wur); err != nil {
			return fmt.Errorf("delete wakeup request: %w", err)
		}
	}

	// Build the new WakeupRequest for the branch
	wur = &v1alpha1.WakeupRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branchName,
			Namespace: c.config.ClustersNamespace,
		},
		Spec: v1alpha1.WakeupRequestSpec{
			BranchName: branchName,
		},
	}

	// Create the WakeupRequest
	if err := c.kubeClient.Create(ctx, wur); err != nil {
		return fmt.Errorf("create wakeup request: %w", err)
	}
	return nil
}

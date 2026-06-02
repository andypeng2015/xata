package reconciler

import (
	"context"
	"fmt"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branch-operator/api/v1alpha1"
)

// updateXVolStatus updates the Branch status with information about the
// primary XVol for the Branch. It does nothing for non-pool branches because
// primary XVols are only relevant for pool branches.
func (r *BranchReconciler) updateXVolStatus(ctx context.Context, branch *v1alpha1.Branch) error {
	// If the Branch is not a pool branch, XVol info is not relevant
	if !branch.HasWakeupPoolAnnotation() {
		return nil
	}

	// If the Branch has no cluster (ie it's pool hibernated), there is no XVol
	// info to report
	if !branch.HasClusterName() {
		return nil
	}

	// Get the Cluster associated with the Branch
	cluster := &apiv1.Cluster{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      branch.ClusterName(),
		Namespace: r.ClustersNamespace,
	}, cluster)
	if err != nil {
		return err
	}

	// Get the primary PVC name for the Cluster
	pvcName, err := getClusterPVC(cluster)
	if err != nil {
		return fmt.Errorf("update XVol status: %w", err)
	}

	// Look up the XVol for the PV
	xVol, err := r.getXVolForPVC(ctx, pvcName)
	if err != nil {
		return &ConditionError{
			ConditionType:   v1alpha1.BranchReadyConditionType,
			ConditionReason: v1alpha1.XVolNotFoundReason,
			Err:             fmt.Errorf("find XVol for PVC %s: %w", pvcName, err),
		}
	}

	// The XVol exists, record its name on the Branch's status
	branch.Status.PrimaryXVolName = xVol.GetName()
	err = r.Status().Update(ctx, branch)
	if err != nil {
		return err
	}

	return nil
}

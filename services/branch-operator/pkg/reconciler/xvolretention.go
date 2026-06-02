package reconciler

import (
	"context"
	"fmt"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
)

// reconcileXVolOwnership ensures that the XVol for the Branch Cluster's
// primary instance is set to have a reclaim policy of Retain and is owned by
// the `Branch`, and that XVols for any non-primary PVCs have their reclaim
// policy set to Delete and the Branch owner reference removed.
//
// Retaining the primary XVol ensures it survives the Cluster deletion in a
// subsequent reconciliation step (reconcileOwnedClusters), so the Branch can
// be woken up later. Un-retaining replica XVols ensures they do not leak
// after a replica is demoted following a primary switchover.
func (r *BranchReconciler) reconcileXVolOwnership(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// If the Branch has a Cluster name, there is nothing to do - the Cluster
	// will not be deleted later during reconciliation, so there is no need to
	// protect the XVols.
	if branch.HasClusterName() {
		return controllerutil.OperationResultNone, nil
	}

	// If the Branch is not a pool branch, there is no need to manage XVol
	// ownership
	if !branch.HasWakeupPoolAnnotation() {
		return controllerutil.OperationResultNone, nil
	}

	// List all Clusters owned by the Branch.
	var clusterList apiv1.ClusterList
	err := r.List(ctx, &clusterList,
		client.InNamespace(r.ClustersNamespace),
		client.MatchingFields{ClusterOwnerKey: branch.Name},
	)
	if err != nil {
		return "", fmt.Errorf("list owned clusters: %w", err)
	}

	// For each owned Cluster, retain the primary XVol and un-retain any replica
	// XVols. Only one Cluster is expected here; the one that was previously
	// assigned to `spec.cluster.name`
	result := controllerutil.OperationResultNone
	for _, cluster := range clusterList.Items {
		// Ignore any Clusters that are already being deleted
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}

		// If the Cluster has no primary PVC we don't know which XVol to keep,
		// so skip the Cluster rather than risk un-retaining the wrong one.
		primaryPVCName, err := getClusterPVC(&cluster)
		if err != nil {
			continue
		}

		for _, pvcName := range cluster.Status.HealthyPVC {
			// For the primary PVC, ensure the XVol is retained and owned by the
			// Branch.
			// For non-primary PVCs, ensure the XVol is not retained and not owned by
			// the Branch.
			ensure := r.ensureXVolNotRetained
			if pvcName == primaryPVCName {
				ensure = r.ensureXVolRetained
			}

			patched, err := ensure(ctx, branch, pvcName)
			if err != nil {
				return "", fmt.Errorf("ensure XVol ownership for PVC %s: %w", pvcName, err)
			}
			if patched {
				result = controllerutil.OperationResultUpdated
			}
		}
	}

	return result, nil
}

// ensureXVolRetained patches the XVol backing the given PVC to have a reclaim
// policy of Retain and sets the Branch as an owner of the XVol. The owner
// reference ensures the XVol is cleaned up by Kubernetes garbage collection
// when the Branch is deleted. It returns true if the XVol was patched, or
// false if the XVol did not need to be patched.
func (r *BranchReconciler) ensureXVolRetained(ctx context.Context, branch *v1alpha1.Branch, pvcName string) (bool, error) {
	xvol, err := r.getXVolForPVC(ctx, pvcName)
	if err != nil {
		return false, err
	}

	// Get the current reclaim policy and check if the Branch is already an
	// owner of the XVol.
	reclaimPolicy, _, _ := unstructured.NestedString(xvol.Object, "spec", "xvolReclaimPolicy")
	hasRetainReclaimPolicy := reclaimPolicy == "Retain"
	hasOwnerRef, err := controllerutil.HasOwnerReference(xvol.GetOwnerReferences(), branch, r.Scheme)
	if err != nil {
		return false, fmt.Errorf("check owner reference on XVol %s: %w", xvol.GetName(), err)
	}

	// If the XVol already has Retain reclaim policy and the Branch owner
	// reference there is nothing to do
	if hasRetainReclaimPolicy && hasOwnerRef {
		return false, nil
	}

	// Create a merge patch to update the XVol
	patch := client.MergeFrom(xvol.DeepCopy())

	// Patch the XVol to have a reclaim policy of Retain.
	err = unstructured.SetNestedField(xvol.Object, "Retain", "spec", "xvolReclaimPolicy")
	if err != nil {
		return false, fmt.Errorf("set reclaim policy on XVol %s: %w", xvol.GetName(), err)
	}

	// Set the Branch as an owner of the XVol so it is cleaned up by Kubernetes
	// garbage collection if the branch is deleted before it is woken up
	err = controllerutil.SetOwnerReference(branch, xvol, r.Scheme)
	if err != nil {
		return false, fmt.Errorf("set owner reference on XVol %s: %w", xvol.GetName(), err)
	}

	// Apply the patch to update the XVol
	err = r.Patch(ctx, xvol, patch)
	if err != nil {
		return false, fmt.Errorf("patch XVol %s: %w", xvol.GetName(), err)
	}

	return true, nil
}

// ensureXVolNotRetained patches the XVol backing the given PVC to have a
// reclaim policy of Delete and removes the Branch from its owner references.
// It is the inverse of ensureXVolRetained and is used for XVols backing
// non-primary PVCs (e.g. after a switchover demotes a former primary). It
// returns true if the XVol was patched, or false if no patch was needed.
func (r *BranchReconciler) ensureXVolNotRetained(ctx context.Context, branch *v1alpha1.Branch, pvcName string) (bool, error) {
	xvol, err := r.getXVolForPVC(ctx, pvcName)
	if err != nil {
		return false, err
	}

	// Get the current reclaim policy and check if the Branch is an owner of
	// the XVol.
	reclaimPolicy, _, _ := unstructured.NestedString(xvol.Object, "spec", "xvolReclaimPolicy")
	hasDeleteReclaimPolicy := reclaimPolicy == "Delete"
	hasOwnerRef, err := controllerutil.HasOwnerReference(xvol.GetOwnerReferences(), branch, r.Scheme)
	if err != nil {
		return false, fmt.Errorf("check owner reference on XVol %s: %w", xvol.GetName(), err)
	}

	// If the XVol already has Delete reclaim policy and the Branch is not an
	// owner there is nothing to do.
	if hasDeleteReclaimPolicy && !hasOwnerRef {
		return false, nil
	}

	// Create a merge patch to update the XVol
	patch := client.MergeFrom(xvol.DeepCopy())

	// Patch the XVol to have a reclaim policy of Delete.
	err = unstructured.SetNestedField(xvol.Object, "Delete", "spec", "xvolReclaimPolicy")
	if err != nil {
		return false, fmt.Errorf("set reclaim policy on XVol %s: %w", xvol.GetName(), err)
	}

	// Remove the Branch owner reference if present.
	if hasOwnerRef {
		err = controllerutil.RemoveOwnerReference(branch, xvol, r.Scheme)
		if err != nil {
			return false, fmt.Errorf("remove owner reference on XVol %s: %w", xvol.GetName(), err)
		}
	}

	// Apply the patch to update the XVol
	err = r.Patch(ctx, xvol, patch)
	if err != nil {
		return false, fmt.Errorf("patch XVol %s: %w", xvol.GetName(), err)
	}

	return true, nil
}

package reconciler

import (
	"context"
	"fmt"
	"slices"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileVolumeSnapshot ensures that the correct VolumeSnapshot exists for
// the given Branch when it has a Restore field of type VolumeSnapshot.
func (r *BranchReconciler) reconcileVolumeSnapshot(ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// If there is no VolumeSnapshot-based restore configured there is nothing to
	// do. There can be no pre-existing VolumeSnapshot for the branch to clean
	// up, because the `restore` field is immutable after creation.
	if !branch.Spec.Restore.IsVolumeSnapshotType() {
		return controllerutil.OperationResultNone, nil
	}

	// If the child branch has no cluster name, there is nothing do do.
	if !branch.HasClusterName() {
		return controllerutil.OperationResultNone, nil
	}

	// Get the child Cluster
	cluster := &apiv1.Cluster{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      branch.ClusterName(),
		Namespace: r.ClustersNamespace,
	}, cluster)
	if err != nil && !apierrors.IsNotFound(err) {
		return controllerutil.OperationResultNone, err
	}

	// If the Cluster doesn't exist, ensure the VolumeSnapshot exists
	if apierrors.IsNotFound(err) {
		return r.ensureVolumeSnapshotExists(ctx, branch)
	}

	// If the Cluster exists and is healthy, ensure no VolumeSnapshot exists. The
	// VolumeSnapshot serves no purpose beyond bootstrapping the Cluster. Once
	// the Cluster reaches a healthy state, there is no need to retain the
	// VolumeSnapshot.
	if cluster.Status.Phase == apiv1.PhaseHealthy {
		return r.ensureNoVolumeSnapshotExists(ctx, branch)
	}

	// The Cluster exists but is either not yet healthy or has been healthy and
	// has become unhealthy. In either case we do not ensure either the existence
	// or non-existence of the VolumeSnapshot.
	return controllerutil.OperationResultNone, nil
}

// ensureVolumeSnapshotExists ensures that the VolumeSnapshot for the given
// Branch exists, creating it if necessary.
func (r *BranchReconciler) ensureVolumeSnapshotExists(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// Fetch the parent Cluster
	parent, err := r.getClusterForBranch(ctx, branch.Spec.Restore.Name)
	if err != nil {
		return controllerutil.OperationResultNone, &ConditionError{
			ConditionType:   v1alpha1.BranchReadyConditionType,
			ConditionReason: v1alpha1.ParentClusterNotFoundReason,
			Err:             err,
		}
	}
	if parent == nil {
		return controllerutil.OperationResultNone, &ConditionError{
			ConditionType:   v1alpha1.BranchReadyConditionType,
			ConditionReason: v1alpha1.ParentBranchHasNoClusterReason,
			Err:             fmt.Errorf("parent branch %q has no cluster", branch.Spec.Restore.Name),
		}
	}

	// Define the phases in which the parent cluster can safely be snapshotted
	snapshotableClusterPhases := []string{
		apiv1.PhaseHealthy,
		apiv1.PhaseUpgradeDelayed,
	}

	// Check that the parent cluster is in a snapshotable phase
	if !slices.Contains(snapshotableClusterPhases, parent.Status.Phase) {
		return controllerutil.OperationResultNone, &ConditionError{
			ConditionType:   v1alpha1.BranchReadyConditionType,
			ConditionReason: v1alpha1.ParentClusterUnhealthyReason,
			Err: fmt.Errorf("parent cluster %q is in non-snapshottable phase %s",
				parent.Name,
				parent.Status.Phase),
		}
	}

	// Get the name of the parent cluster's PVC
	sourcePVC, err := getClusterPVC(parent)
	if err != nil {
		return controllerutil.OperationResultNone, &ConditionError{
			ConditionType:   v1alpha1.BranchReadyConditionType,
			ConditionReason: v1alpha1.ParentClusterPVCNotFoundReason,
			Err:             err,
		}
	}

	vs := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.Spec.Restore.Name + "-" + branch.Name,
			Namespace: r.ClustersNamespace,
		},
	}

	// Create or update the VolumeSnapshot
	return controllerutil.CreateOrUpdate(ctx, r.Client, vs, func() error {
		// Ensure the owner reference is set on the VolumeSnapshot
		if err := controllerutil.SetControllerReference(branch, vs, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the VolumeSnapshot
		ensureLabels(vs, branch.Spec.InheritedMetadata)

		// Set the spec for the VolumeSnapshot
		vs.Spec = resources.VolumeSnapshotSpec(sourcePVC,
			branch.Spec.ClusterSpec.Storage.GetVolumeSnapshotClass())

		return nil
	})
}

// ensureNoVolumeSnapshotExists ensures that the VolumeSnapshot for the given
// Branch does not exist, deleting it if necessary.
func (r *BranchReconciler) ensureNoVolumeSnapshotExists(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// Get the VolumeSnapshot
	vs := &snapshotv1.VolumeSnapshot{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      branch.Spec.Restore.Name + "-" + branch.Name,
		Namespace: r.ClustersNamespace,
	}, vs)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, nil
		}
		return controllerutil.OperationResultNone, err
	}

	// VolumeSnapshot exists but shouldn't so delete it
	if err := r.Delete(ctx, vs); err != nil {
		return controllerutil.OperationResultNone, client.IgnoreNotFound(err)
	}

	return controllerutil.OperationResultUpdated, nil
}

// getClusterPVC returns the PVC name for a CNPG cluster, preferring the
// current primary's PVC, falling back to dangling PVCs if available.
func getClusterPVC(cluster *apiv1.Cluster) (string, error) {
	if cluster.Status.CurrentPrimary != "" {
		return cluster.Status.CurrentPrimary, nil
	}

	// If there's no current primary set, check the dangling PVCs for the cluster
	// and pick one
	if len(cluster.Status.DanglingPVC) > 0 {
		return cluster.Status.DanglingPVC[0], nil
	}

	// If there's no current primary and no dangling PVCs the cluster can't be
	// used for snapshots
	return "", fmt.Errorf("no PVC found for cluster %s", cluster.Name)
}

// getClusterForBranch retrieves the CNPG cluster for the given Branch ID or
// nil if the Branch has no Cluster
func (r *BranchReconciler) getClusterForBranch(ctx context.Context, branchID string) (*apiv1.Cluster, error) {
	// Get the Branch CR by ID
	branch := &v1alpha1.Branch{}
	err := r.Get(ctx, types.NamespacedName{Name: branchID}, branch)
	if err != nil {
		return nil, err
	}

	// Return nil if the Branch has no Cluster
	if !branch.HasClusterName() {
		return nil, nil
	}

	// Get the Cluster using the name specified in the Branch CR
	cluster := &apiv1.Cluster{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      branch.ClusterName(),
		Namespace: r.ClustersNamespace,
	}, cluster)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

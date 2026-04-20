package reconciler

import (
	"context"
	"strconv"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

const (
	BranchAnnotation                = "xata.io/branch"
	ScaleToZeroEnabledAnnotation    = "xata.io/scale-to-zero-enabled"
	ScaleToZeroInactivityAnnotation = "xata.io/scale-to-zero-inactivity-minutes"
	HibernationAnnotation           = "cnpg.io/hibernation"
	PodPatchAnnotation              = "cnpg.io/podPatch"
	//nolint: gosec
	SkipWALArchivingAnnotation = "cnpg.io/skipWalArchiving"
)

// createOrUpdateCluster creates or updates the CNPG Cluster for the given Branch.
func (r *BranchReconciler) reconcileCluster(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// Skip reconciliation if the Branch has no Cluster name
	if !branch.HasClusterName() {
		return controllerutil.OperationResultNone, nil
	}

	cluster := &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.ClusterName(),
			Namespace: r.ClustersNamespace,
		},
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cluster, func() error {
		// Ensure the owner reference is set on the Cluster
		if err := controllerutil.SetControllerReference(branch, cluster, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the Cluster
		ensureLabels(&cluster.ObjectMeta, branch.Spec.InheritedMetadata)

		// Ensure annotations are set on the Cluster
		reconcileClusterAnnotations(&cluster.ObjectMeta, branch)

		// Build the complete cluster configuration from the Branch spec and the
		// reconciler configuration
		cfg := resources.ClusterConfig{
			ClusterSpec:       branch.Spec.ClusterSpec,
			BackupSpec:        branch.Spec.BackupSpec,
			InheritedMetadata: branch.Spec.InheritedMetadata,
			RestoreSpec:       branch.Spec.Restore,
			Tolerations:       r.Tolerations,
			EnforceZone:       r.EnforceZone,
			ImagePullSecrets:  r.ImagePullSecrets,
		}

		// Set the spec for the Cluster
		cluster.Spec = resources.ClusterSpec(branch.Name, branch.ClusterName(), cfg)

		return nil
	})

	return result, err
}

// reconcileOwnedClusters ensures that only the Cluster named in the Branch
// spec is owned by the Branch, deleting any other Clusters owned by the
// Branch.
func (r *BranchReconciler) reconcileOwnedClusters(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	var clusterList apiv1.ClusterList

	// List all Clusters owned by the Branch
	err := r.List(ctx, &clusterList,
		client.InNamespace(r.ClustersNamespace),
		client.MatchingFields{ClusterOwnerKey: branch.Name},
	)
	if err != nil {
		return "", err
	}

	// Delete any owned Clusters not named in the Branch spec
	result := controllerutil.OperationResultNone
	for _, cluster := range clusterList.Items {
		// Don't delete the Cluster we should keep
		if cluster.Name == branch.ClusterName() {
			continue
		}

		// Don't delete Clusters that are already being deleted
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}

		// Delete the Cluster
		if err := r.Delete(ctx, &cluster); err != nil {
			return "", err
		}
		result = controllerutil.OperationResultUpdated
	}

	return result, nil
}

// reconcileClusterAnnotations reconciles the cluster-level annotations for a
// Branch
func reconcileClusterAnnotations(m *metav1.ObjectMeta, branch *v1alpha1.Branch) {
	if m.Annotations == nil {
		m.Annotations = make(map[string]string)
	}

	cSpec := branch.Spec.ClusterSpec
	// Reconcile the scale-to-zero annotations
	if cSpec.ScaleToZero != nil && cSpec.ScaleToZero.Enabled {
		m.Annotations[ScaleToZeroEnabledAnnotation] = kubeTrue
		m.Annotations[ScaleToZeroInactivityAnnotation] = strconv.Itoa(int(cSpec.ScaleToZero.InactivityPeriodMinutes))
	} else {
		m.Annotations[ScaleToZeroEnabledAnnotation] = kubeFalse
		delete(m.Annotations, ScaleToZeroInactivityAnnotation)
	}

	// Reconcile the hibernation annotation
	if cSpec.Hibernation != nil {
		switch *cSpec.Hibernation {
		case v1alpha1.HibernationModeEnabled:
			m.Annotations[HibernationAnnotation] = "on"
		case v1alpha1.HibernationModeDisabled:
			m.Annotations[HibernationAnnotation] = "off"
		}
	} else {
		delete(m.Annotations, HibernationAnnotation)
	}

	bSpec := branch.Spec.BackupSpec
	// Reconcile the WAL archiving annotation
	if bSpec.IsWALArchivingDisabled() {
		m.Annotations[SkipWALArchivingAnnotation] = "enabled"
	} else {
		delete(m.Annotations, SkipWALArchivingAnnotation)
	}

	// Reconcile the branch name annotation
	m.Annotations[BranchAnnotation] = branch.Name

	// Reconcile the podPatch annotation
	m.Annotations[PodPatchAnnotation] = `[{"op": "add", "path": "/spec/enableServiceLinks", "value": false}]`
}

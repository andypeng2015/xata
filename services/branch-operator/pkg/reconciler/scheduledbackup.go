package reconciler

import (
	"context"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileScheduledBackup ensures that the correct ScheduledBackup exists for the
// given Branch when backups are configured. When BackupConfiguration is nil,
// it ensures no ScheduledBackup exists.
func (r *BranchReconciler) reconcileScheduledBackup(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	sb := &apiv1.ScheduledBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		},
	}

	// If scheduled backup is not configured, ensure ScheduledBackup doesn't exist
	if !branch.Spec.BackupSpec.IsScheduledBackupEnabled() {
		// Try to get the ScheduledBackup
		err := r.Get(ctx, types.NamespacedName{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		}, sb)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return controllerutil.OperationResultNone, nil
			}
			return controllerutil.OperationResultNone, err
		}

		// ScheduledBackup exists but shouldn't so delete it
		if err := r.Delete(ctx, sb); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultUpdated, nil
	}

	// BackupConfiguration is set, create or update the ScheduledBackup
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, sb, func() error {
		// Ensure the owner reference is set on the ScheduledBackup
		if err := controllerutil.SetControllerReference(branch, sb, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the ScheduledBackup
		ensureLabels(sb, branch.Spec.InheritedMetadata)

		// Set the spec for the ScheduledBackup
		sb.Spec = resources.ScheduledBackupSpec(
			branch.ClusterName(),
			branch.Spec.BackupSpec.ScheduledBackup.Schedule,
			!branch.HasClusterName() || branch.Spec.ClusterSpec.Hibernation.IsEnabled(),
		)

		return nil
	})

	return result, err
}

package reconciler

import (
	"context"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileObjectStore ensures that the correct ObjectStore exists for the
// given Branch when backups are configured. When BackupConfiguration is nil,
// it ensures no ObjectStore exists.
func (r *BranchReconciler) reconcileObjectStore(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	os := &barmanPluginApi.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		},
	}

	// If no backup configuration, ensure ObjectStore doesn't exist
	if branch.Spec.BackupSpec == nil {
		// Try to get the ObjectStore
		err := r.Get(ctx, types.NamespacedName{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		}, os)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return controllerutil.OperationResultNone, nil
			}
			return controllerutil.OperationResultNone, err
		}

		// ObjectStore exists but shouldn't so delete it
		if err := r.Delete(ctx, os); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultUpdated, nil
	}

	// BackupConfiguration is set, create or update the ObjectStore
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, os, func() error {
		// Ensure the owner reference is set on the ObjectStore
		if err := controllerutil.SetControllerReference(branch, os, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the ObjectStore
		ensureLabels(os, branch.Spec.InheritedMetadata)

		// Set the spec for the ObjectStore
		os.Spec = resources.ObjectStoreSpec(
			r.BackupsBucket,
			r.BackupsEndpoint,
			branch.Spec.BackupSpec.Retention,
		)

		return nil
	})

	return result, err
}

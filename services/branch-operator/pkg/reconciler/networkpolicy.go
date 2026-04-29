package reconciler

import (
	"context"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileNetworkPolicy ensures that the correct NetworkPolicy exists for the
// given Branch.
func (r *BranchReconciler) reconcileNetworkPolicy(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		},
	}

	// If the branch has no cluster defined, ensure its NetworkPolicy doesn't
	// exist
	if !branch.HasClusterName() {
		// Attempt to get the NetworkPolicy. If it doesn't exist, there is nothing
		// to do
		err := r.Get(ctx, types.NamespacedName{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		}, np)
		if err != nil {
			return controllerutil.OperationResultNone, client.IgnoreNotFound(err)
		}

		// Delete the NetworkPolicy
		if err := r.Delete(ctx, np); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultUpdated, nil
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		// Ensure the owner reference is set on the NetworkPolicy
		if err := controllerutil.SetControllerReference(branch, np, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the NetworkPolicy
		ensureLabels(np, branch.Spec.InheritedMetadata)

		// Set the spec for the NetworkPolicy
		np.Spec = resources.NetworkPolicySpec(branch.ClusterName())

		return nil
	})

	return result, err
}

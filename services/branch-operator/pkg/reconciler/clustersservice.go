package reconciler

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

const (
	ClustersServiceNamePrefix = "clusters-"
	XataNamespace             = "xata"
)

// reconcileClustersService ensures that the correct clusters Service exists
// for the given Branch.
func (r *BranchReconciler) reconcileClustersService(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ClustersServiceNamePrefix + branch.Name,
			Namespace: XataNamespace,
		},
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		// Ensure the owner reference is set on the Service
		if err := controllerutil.SetControllerReference(branch, svc, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the Service
		ensureLabels(svc, branch.Spec.InheritedMetadata)

		// Ensure the Cilium global annotation is set on the Service
		if svc.Annotations == nil {
			svc.Annotations = make(map[string]string)
		}
		svc.Annotations["service.cilium.io/global"] = kubeTrue

		// Set the spec for the Service
		svc.Spec = resources.ClustersServiceSpec()

		return nil
	})

	return result, err
}

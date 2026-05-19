package wakeup

import (
	"context"
	"fmt"

	"xata/services/branch-operator/api/v1alpha1"
	v1alpha1ac "xata/services/branch-operator/applyconfiguration/api/v1alpha1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// getBranch retrieves the Branch resource associated with the given name. If
// the Branch is not found, a ConditionError is returned indicating that the
// Branch was not found. Any other errors encountered during retrieval are
// returned as-is.
func (r *WakeupReconciler) getBranch(ctx context.Context, name string) (*v1alpha1.Branch, error) {
	branch := &v1alpha1.Branch{}

	// Get the Branch resource by name.
	err := r.Get(ctx, client.ObjectKey{Name: name}, branch)
	if err != nil && apierrors.IsNotFound(err) {
		err = &ConditionError{
			ConditionReason: v1alpha1.BranchNotFoundReason,
			Err:             err,
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// wakeupPoolName retrieves the name of the wakeup pool from the Branch's
// annotations. If the annotation is not present, a ConditionError is returned
// indicating that the required annotation is missing
func (r *WakeupReconciler) wakeupPoolName(branch *v1alpha1.Branch) (string, error) {
	if !branch.HasWakeupPoolAnnotation() {
		err := &ConditionError{
			ConditionReason: v1alpha1.NoPoolAnnotationReason,
			Err:             fmt.Errorf("missing %q annotation", v1alpha1.WakeupPoolAnnotation),
			Terminal:        true,
		}
		return "", err
	}

	return branch.WakeupPoolName(), nil
}

// assignClusterToBranch updates the Branch resource to set the given Cluster
// name in its spec.
func (r *WakeupReconciler) assignClusterToBranch(ctx context.Context, branch *v1alpha1.Branch, clusterName string) error {
	// Create the SSA applyconfiguration to set the Cluster name in the Branch
	// spec. This will assign the Cluster to the Branch
	ac := v1alpha1ac.Branch(branch.Name, "").
		WithSpec(v1alpha1ac.BranchSpec().
			WithClusterSpec(v1alpha1ac.ClusterSpec().
				WithName(clusterName)))

	// Apply the Branch spec update using SSA
	err := r.Apply(ctx, ac, client.FieldOwner(ReconcilerName), client.ForceOwnership)
	if err != nil {
		return err
	}

	return nil
}

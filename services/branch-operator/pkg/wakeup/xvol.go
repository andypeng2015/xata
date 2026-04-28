package wakeup

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branch-operator/api/v1alpha1"
)

var xvolGVK = schema.GroupVersionKind{
	Group:   "xata.io",
	Version: "v1alpha1",
	Kind:    "Xvol",
}

const (
	xvolStateAvailable = "Available"
	xvolStateBound     = "Bound"
)

// validateXVolStatus checks the status of the specified XVol. It returns a
// non-terminal ConditionError if the XVol is not in a state that can be used
// as the target of a wakeup operation
func (r *WakeupReconciler) validateXVolStatus(ctx context.Context, xvolName string) error {
	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(xvolGVK)

	// Get the XVol resource by name
	err := r.Get(ctx, client.ObjectKey{Name: xvolName}, xvol)
	if apierrors.IsNotFound(err) {
		return &ConditionError{
			ConditionReason: v1alpha1.XVolNotFoundReason,
			Err:             fmt.Errorf("XVol %q not found", xvolName),
		}
	}
	if err != nil {
		return fmt.Errorf("get XVol %q: %w", xvolName, err)
	}

	// Read the XVol's state from its status field
	state, _, _ := unstructured.NestedString(xvol.Object, "status", "volumeState")

	// Define the states in which the XVol can be used for wakeup
	wakeableStates := []string{xvolStateAvailable, xvolStateBound}

	// Check if the XVol is in a state that can be used for wakeup
	if !slices.Contains(wakeableStates, state) {
		return &ConditionError{
			ConditionReason: v1alpha1.XVolNotReadyReason,
			Err:             fmt.Errorf("XVol %q is in state %q, not wakeable", xvolName, state),
		}
	}

	return nil
}

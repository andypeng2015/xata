package wakeup

import (
	"context"
	"fmt"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branch-operator/api/v1alpha1"
)

// getPersistentVolume returns the PV backing the Cluster's primary instance by
// resolving the Cluster's healthy PVC and following its bound PV reference.
func (r *WakeupReconciler) getPersistentVolume(ctx context.Context, cluster *apiv1.Cluster) (*v1.PersistentVolume, error) {
	// Ensure the Cluster has at least one healthy PVC
	if len(cluster.Status.HealthyPVC) == 0 {
		return nil, pvConditionError(fmt.Errorf("no healthy PVCs found for cluster %q", cluster.Name))
	}

	// Get the PVC
	pvcName := cluster.Status.HealthyPVC[0]
	pvc := &v1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: cluster.Namespace}, pvc)
	if err != nil {
		return nil, fmt.Errorf("get pvc %q: %w", pvcName, err)
	}

	// Read the PV name from the PVC
	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		return nil, pvConditionError(fmt.Errorf("PVC %q is not bound to a PV", pvcName))
	}

	// Get the PV
	pv := &v1.PersistentVolume{}
	err = r.Get(ctx, client.ObjectKey{Name: pvName}, pv)
	if err != nil {
		return nil, fmt.Errorf("get pv %q: %w", pvName, err)
	}

	return pv, nil
}

// pvConditionError constructs a terminal ConditionError for errors encountered
// while resolving the Cluster's primary PV.
func pvConditionError(err error) *ConditionError {
	return &ConditionError{
		ConditionReason: v1alpha1.PVNotAvailableReason,
		Err:             err,
		Terminal:        true,
	}
}

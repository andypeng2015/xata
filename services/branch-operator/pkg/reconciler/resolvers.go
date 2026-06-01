package reconciler

import (
	"context"
	"fmt"

	"xata/services/branch-operator/api/v1alpha1"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//
// Functions and methods used by multiple reconciliation steps
//

var xvolGVK = schema.GroupVersionKind{
	Group:   "xata.io",
	Version: "v1alpha1",
	Kind:    "Xvol",
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

	return "", fmt.Errorf("no PVC found for cluster %s", cluster.Name)
}

// xVolNameForPV determines the name of the XVol corresponding to the given PV
func (r *BranchReconciler) xVolNameForPV(ctx context.Context, pvName string) (string, error) {
	pv := &corev1.PersistentVolume{}
	err := r.Get(ctx, client.ObjectKey{Name: pvName}, pv)
	if err != nil {
		return "", err
	}

	// Determine the name of the XVol corresponding to the PV. This defaults to
	// the PV name but can be overridden by an annotation on the PV
	xVolName := pvName
	if n, ok := pv.Annotations[v1alpha1.AwokenByXVolAnnotation]; ok {
		xVolName = n
	}
	return xVolName, nil
}

// getXVolForPVC looks up the XVol backing the given PVC. It returns (nil, nil)
// if the PVC has no bound PV, if the PV has no XVol, or if the XVol CRD is not
// installed - in all of these cases there is no XVol to act on.
func (r *BranchReconciler) getXVolForPVC(ctx context.Context, pvcName string) (*unstructured.Unstructured, error) {
	// Get the PVC
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, client.ObjectKey{
		Name:      pvcName,
		Namespace: r.ClustersNamespace,
	}, &pvc); err != nil {
		return nil, client.IgnoreNotFound(err)
	}

	// If the PVC does not have a bound PV there is nothing to do
	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		return nil, nil
	}

	// Get the name of the XVol corresponding to the PV
	xVolName, err := r.xVolNameForPV(ctx, pvName)
	if err != nil {
		return nil, fmt.Errorf("get xvol name for pv %q: %w", pvName, err)
	}

	// Get the XVol. We have to use Unstructured here because the XVol types are
	// in the private xatastor repository so we can't import them directly.
	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(xvolGVK)
	err = r.Get(ctx, client.ObjectKey{Name: xVolName}, xvol)
	if err != nil {
		// If the XVol CRD is not installed, treat it the same as if the XVol did
		// not exist - there is nothing to protect.
		if meta.IsNoMatchError(err) {
			return nil, nil
		}
		if client.IgnoreNotFound(err) == nil {
			return nil, nil
		}
		return nil, err
	}

	return xvol, nil
}

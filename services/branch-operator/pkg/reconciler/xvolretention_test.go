package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var xvolGVK = schema.GroupVersionKind{
	Group:   "xata.io",
	Version: "v1alpha1",
	Kind:    "Xvol",
}

func TestXVolReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("only the primary XVol is set to Retain and owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithWakeupPool("pg18-micro").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			// Model a Cluster with a primary and a replica.
			// The primary uses a PV that is annotated with the XVol name, modelling the xatastor-slot case.
			// The replica uses a PV where PV name == XVol name, modelling the non-slot case.
			primaryPVCName := clusterName + "-1"
			primaryXVolName := clusterName + "-primary-xvol"
			primaryPVName := clusterName + "-primary-pv"
			primaryXVol := createBoundPVCAndXVol(ctx, t, primaryPVCName, primaryXVolName, primaryPVName)

			replicaPVCName := clusterName + "-2"
			replicaXVolName := clusterName + "-replica-xvol"
			replicaXVol := createBoundPVCAndXVol(ctx, t, replicaPVCName, replicaXVolName, "")

			// Set the Cluster's status to identify the primary PVC and list both
			// PVCs as healthy
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: primaryPVCName,
				HealthyPVC:     []string{primaryPVCName, replicaPVCName},
			})

			// Unset the cluster name on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the primary XVol to be patched to Retain and owned by the Branch
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: primaryXVolName}, primaryXVol)
				if err != nil {
					return false
				}
				dp, _, _ := unstructured.NestedString(primaryXVol.Object, "spec", "xvolReclaimPolicy")
				return dp == "Retain"
			})
			require.Len(t, primaryXVol.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, primaryXVol.GetOwnerReferences()[0].Name)

			// Get the replica XVol
			err = k8sClient.Get(ctx, client.ObjectKey{Name: replicaXVolName}, replicaXVol)
			require.NoError(t, err)

			// Expect the replica XVol to be unpatched
			dp, _, _ := unstructured.NestedString(replicaXVol.Object, "spec", "xvolReclaimPolicy")
			require.Equal(t, "Delete", dp)
			require.Empty(t, replicaXVol.GetOwnerReferences())
		})
	})

	t.Run("a previously-retained replica XVol is un-retained", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithWakeupPool("pg18-micro").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			// Model a Cluster with a primary and a replica.
			primaryPVCName := clusterName + "-1"
			primaryXVolName := clusterName + "-primary-xvol"
			primaryXVol := createBoundPVCAndXVol(ctx, t, primaryPVCName, primaryXVolName, "")

			replicaPVCName := clusterName + "-2"
			replicaXVolName := clusterName + "-replica-xvol"
			replicaXVol := createBoundPVCAndXVol(ctx, t, replicaPVCName, replicaXVolName, "")

			// Simulate the replica XVol having been retained and owned by the
			// Branch from a previous reconciliation
			err := unstructured.SetNestedField(replicaXVol.Object, "Retain", "spec", "xvolReclaimPolicy")
			require.NoError(t, err)
			err = controllerutil.SetOwnerReference(br, replicaXVol, k8sClient.Scheme())
			require.NoError(t, err)
			err = k8sClient.Update(ctx, replicaXVol)
			require.NoError(t, err)

			// Set the Cluster's status to identify the primary PVC and list
			// both PVCs as healthy.
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: primaryPVCName,
				HealthyPVC:     []string{primaryPVCName, replicaPVCName},
			})

			// Unset the cluster name on the Branch
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the primary XVol to be patched to Retain and owned by the Branch
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: primaryXVolName}, primaryXVol)
				if err != nil {
					return false
				}
				dp, _, _ := unstructured.NestedString(primaryXVol.Object, "spec", "xvolReclaimPolicy")
				return dp == "Retain"
			})
			require.Len(t, primaryXVol.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, primaryXVol.GetOwnerReferences()[0].Name)

			// Expect the replica XVol to be flipped back to Delete with the
			// Branch owner reference removed.
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: replicaXVolName}, replicaXVol)
				if err != nil {
					return false
				}
				dp, _, _ := unstructured.NestedString(replicaXVol.Object, "spec", "xvolReclaimPolicy")
				return dp == "Delete" && len(replicaXVol.GetOwnerReferences()) == 0
			})
		})
	})

	t.Run("XVol is not patched when branch has a cluster", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			xvolName, pvcName, xvol := createPVCAndXVol(ctx, t, clusterName)

			// Set the Cluster's status.HealthyPVC to reference the PVC
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				HealthyPVC: []string{pvcName},
			})

			// Wait for reconciliation to complete (branch still has its cluster)
			requireEventuallyTrue(t, func() bool {
				b := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &b); err != nil {
					return false
				}
				return b.Status.ObservedGeneration == b.Generation
			})

			// Expect the XVol reclaim policy to still be Delete
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: xvolName}, xvol))
			dp, _, _ := unstructured.NestedString(xvol.Object, "spec", "xvolReclaimPolicy")
			require.Equal(t, "Delete", dp)

			// Expect the XVol to have no owner references
			require.Empty(t, xvol.GetOwnerReferences())
		})
	})

	t.Run("XVol is not patched when branch is not a pool branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// A Branch with no wakeup pool annotation
		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			xvolName, pvcName, xvol := createPVCAndXVol(ctx, t, clusterName)

			// Set the Cluster's status to reference the PVC as the primary
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcName,
				HealthyPVC:     []string{pvcName},
			})

			// Unset the cluster name on the Branch so that the HasClusterName
			// guard does not short-circuit reconcileXVolOwnership. Only the
			// missing wakeup pool annotation should prevent XVol retention.
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Wait for reconciliation to observe the updated generation
			requireEventuallyTrue(t, func() bool {
				b := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &b); err != nil {
					return false
				}
				return b.Status.ObservedGeneration == b.Generation
			})

			// Expect the XVol reclaim policy to still be Delete
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: xvolName}, xvol))
			dp, _, _ := unstructured.NestedString(xvol.Object, "spec", "xvolReclaimPolicy")
			require.Equal(t, "Delete", dp)

			// Expect the XVol to have no owner references
			require.Empty(t, xvol.GetOwnerReferences())
		})
	})
}

// createPVCAndXVol creates a PersistentVolumeClaim, a PersistentVolume, and an
// XVol in the test environment, named after clusterName. The PV's name matches
// the XVol's name (modelling the non-slot case where XVols are named after
// their PV) and the PVC's spec.volumeName binds to the PV. It returns the
// XVol name, PVC name, and the XVol object.
func createPVCAndXVol(ctx context.Context, t *testing.T, clusterName string) (string, string, *unstructured.Unstructured) {
	t.Helper()

	xvolName := clusterName + "-xvol"
	pvcName := clusterName + "-1"
	xvol := createBoundPVCAndXVol(ctx, t, pvcName, xvolName, "")
	return xvolName, pvcName, xvol
}

// createBoundPVCAndXVol creates a PVC bound to a PV bound to an XVol in the
// test environment. If pvName is empty the PV is named after the XVol (the
// non-slot case); otherwise the PV is given the supplied name and annotated
// with the XVol name (the xatastor-slot case).
func createBoundPVCAndXVol(ctx context.Context, t *testing.T, pvcName, xvolName, pvName string) *unstructured.Unstructured {
	t.Helper()

	annotations := map[string]string{}
	if pvName == "" {
		pvName = xvolName
	} else {
		annotations[v1alpha1.AwokenByXVolAnnotation] = xvolName
	}

	// Construct an XVol with the Delete reclaim policy
	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(xvolGVK)
	xvol.SetName(xvolName)
	err := unstructured.SetNestedField(xvol.Object, "Delete", "spec", "xvolReclaimPolicy")
	require.NoError(t, err)
	err = unstructured.SetNestedField(xvol.Object, "1Gi", "spec", "size")
	require.NoError(t, err)

	// Create the XVol
	err = k8sClient.Create(ctx, xvol)
	require.NoError(t, err)

	// Set the XVol status to Bound so that the xvolReclaimPolicy field is
	// mutable
	err = unstructured.SetNestedField(xvol.Object, "Bound", "status", "volumeState")
	require.NoError(t, err)
	err = unstructured.SetNestedField(xvol.Object, pvName, "status", "pvName")
	require.NoError(t, err)
	err = k8sClient.Status().Update(ctx, xvol)
	require.NoError(t, err)

	// Construct the PV
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvName,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "test.csi.driver",
					VolumeHandle: pvName,
				},
			},
		},
	}
	err = k8sClient.Create(ctx, pv)
	require.NoError(t, err)

	// Construct a PVC that binds to the PV via spec.volumeName
	storageClass := "test-storage"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: XataClustersNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeName:       pvName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	err = k8sClient.Create(ctx, pvc)
	require.NoError(t, err)

	return xvol
}

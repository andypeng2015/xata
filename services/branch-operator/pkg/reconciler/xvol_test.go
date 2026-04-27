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
)

var xvolGVK = schema.GroupVersionKind{
	Group:   "xata.io",
	Version: "v1alpha1",
	Kind:    "Xvol",
}

func TestXVolReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("XVol is set to Retain and Branch is set as owner when cluster name is unset", func(t *testing.T) {
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

			// Unset the cluster name on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the XVol reclaim policy to be set to Retain
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: xvolName}, xvol)
				if err != nil {
					return false
				}
				dp, _, _ := unstructured.NestedString(xvol.Object, "spec", "xvolReclaimPolicy")
				return dp == "Retain"
			})

			// Expect the Branch to be an owner of the XVol
			require.Len(t, xvol.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, xvol.GetOwnerReferences()[0].Name)
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
}

// createPVCAndXVol creates a PersistentVolumeClaim and an XVol in the test
// environment. The PVC's spec.volumeName is set to the XVol's name, simulating
// a bound PV backed by an XVol. It returns the XVol name, PVC name, and the
// XVol object.
func createPVCAndXVol(ctx context.Context, t *testing.T, clusterName string) (string, string, *unstructured.Unstructured) {
	t.Helper()

	xvolName := clusterName + "-xvol"
	pvcName := clusterName + "-1"

	// Construct an XVol with the Delete reclaim policy
	xvol := &unstructured.Unstructured{}
	xvol.SetGroupVersionKind(xvolGVK)
	xvol.SetName(xvolName)
	unstructured.SetNestedField(xvol.Object, "Delete", "spec", "xvolReclaimPolicy")
	unstructured.SetNestedField(xvol.Object, "1Gi", "spec", "size")

	// Create the XVol
	err := k8sClient.Create(ctx, xvol)
	require.NoError(t, err)

	// Set the XVol status to Bound so that the xvolReclaimPolicy field is
	// mutable
	unstructured.SetNestedField(xvol.Object, "Bound", "status", "volumeState")
	unstructured.SetNestedField(xvol.Object, xvolName, "status", "pvName")
	err = k8sClient.Status().Update(ctx, xvol)
	require.NoError(t, err)

	// Construct a PVC that references the XVol via spec.volumeName
	storageClass := "test-storage"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: XataClustersNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeName:       xvolName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// Create the PVC
	err = k8sClient.Create(ctx, pvc)
	require.NoError(t, err)

	return xvolName, pvcName, xvol
}

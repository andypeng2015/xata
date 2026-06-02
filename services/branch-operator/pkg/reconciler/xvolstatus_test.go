package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestXVolStatus(t *testing.T) {
	t.Parallel()

	t.Run("primary XVol is not set when branch has no cluster", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch with no associated Cluster
		branch := NewBranchBuilder().
			WithClusterName(nil).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for the Branch to be reconciled
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, br)
				if err != nil {
					return false
				}
				c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
				if c == nil {
					return false
				}
				return c.Status == metav1.ConditionTrue
			})

			// Assert PrimaryXVolName is empty
			require.Empty(t, br.Status.PrimaryXVolName)
		})
	})

	t.Run("primary XVol is not set for a non-pool branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch without a wakeup pool annotation
		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for the Branch to be reconciled
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, br)
				if err != nil {
					return false
				}
				c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
				if c == nil {
					return false
				}
				return c.Status == metav1.ConditionTrue
			})

			// Assert PrimaryXVolName is empty
			require.Empty(t, br.Status.PrimaryXVolName)
		})
	})

	t.Run("PrimaryXVolName is set when Cluster exists", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Build a branch with a wakeup pool annotation
		branch := NewBranchBuilder().
			WithWakeupPool("pg18-tiny").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			xvolName, pvcName, _ := createPVCAndXVol(ctx, t, clusterName)

			// Set the Cluster's CurrentPrimary so getClusterPVC resolves
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcName,
			})

			// Trigger re-reconciliation by updating a spec field
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(60))
			})
			require.NoError(t, err)

			// Assert status.primaryXVolName is set
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br)
				if err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolName
			})
		})
	})

	t.Run("primaryXVolName is retained when cluster name is removed", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithWakeupPool("pg18-tiny").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			xvolName, pvcName, _ := createPVCAndXVol(ctx, t, clusterName)

			// Set the Cluster's CurrentPrimary so getClusterPVC resolves
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcName,
			})

			// Trigger re-reconciliation by updating a spec field
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(60))
			})
			require.NoError(t, err)

			// Wait for PrimaryXVolName to be set
			requireEventuallyTrue(t, func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br)
				if err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolName
			})

			// Remove the cluster name from the branch
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Wait for the Branch to be reconciled again
			requireEventuallyTrue(t, func() bool {
				branch := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &branch); err != nil {
					return false
				}
				return branch.Status.ObservedGeneration == branch.Generation
			})

			// PrimaryXVolName is retained after the cluster name is removed
			require.Equal(t, xvolName, br.Status.PrimaryXVolName)
		})
	})

	t.Run("PrimaryXVolName is updated when primary PVC changes", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithWakeupPool("pg18-tiny").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			// Initial primary: PVC/PV/XVol set A.
			xvolNameA, pvcNameA, _ := createPVCAndXVol(ctx, t, clusterName)
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcNameA,
			})

			// Trigger a reconcile by updating a spec field
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(60))
			})
			require.NoError(t, err)

			// Wait for PrimaryXVolName to be set to the XVol for PVC A
			requireEventuallyTrue(t, func() bool {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br); err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolNameA
			})

			// Create a second PVC/PV/XVol set B and point the Cluster's
			// CurrentPrimary at it.
			xvolNameB := clusterName + "-xvol-b"
			pvcNameB := clusterName + "-2"
			createBoundPVCAndXVol(ctx, t, pvcNameB, xvolNameB, "")

			// Flip the Cluster's CurrentPrimary to PVC B and trigger a reconcile.
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcNameB,
			})

			// Trigger re-reconciliation by updating a spec field
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Instances = 2
			})
			require.NoError(t, err)

			// PrimaryXVolName re-reconciles to the XVol for PVC B
			requireEventuallyTrue(t, func() bool {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br); err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolNameB
			})
		})
	})

	t.Run("PrimaryXVolName is taken from PV annotation when present", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithWakeupPool("pg-18-tiny").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			// The XVol name differs from the PV name; the PV carries an annotation
			// that records the XVol it is backed by. This models xatastor-slot mode,
			// where the PV/XVol name relationship is not implicit.
			pvName := clusterName + "-pv"
			xvolName := clusterName + "-slot-xvol"
			pvcName := clusterName + "-1"
			createBoundPVCAndXVol(ctx, t, pvcName, xvolName, pvName)

			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcName,
			})

			// Trigger re-reconciliation by updating a spec field
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(60))
			})
			require.NoError(t, err)

			// PrimaryXVolName is the annotated name, not the PV name.
			requireEventuallyTrue(t, func() bool {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br); err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolName
			})
		})
	})

	t.Run("reconciliation fails when a pool branch has no XVol", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithWakeupPool("pg18-tiny").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterName := br.Name

			// Wait for the reconciler to create the CNPG Cluster
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, clusterName, &cluster)
			})

			// Create the PVC/PV/XVol set and point the Cluster's primary at it
			xvolName, pvcName, xvol := createPVCAndXVol(ctx, t, clusterName)
			setClusterStatus(ctx, t, &cluster, apiv1.ClusterStatus{
				CurrentPrimary: pvcName,
			})

			// Trigger reconciliation by updating a spec field
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(60))
			})
			require.NoError(t, err)

			// Wait for PrimaryXVolName to be set
			requireEventuallyTrue(t, func() bool {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br); err != nil {
					return false
				}
				return br.Status.PrimaryXVolName == xvolName
			})

			// Delete the XVol, leaving the PVC bound to a PV with no XVol
			err = k8sClient.Delete(ctx, xvol)
			require.NoError(t, err)

			// Trigger re-reconciliation by updating a spec field
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.SmartShutdownTimeout = new(int32(120))
			})
			require.NoError(t, err)

			// Assert that the Ready condition flips to False with the XVolNotFound
			// reason
			requireEventuallyTrue(t, func() bool {
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(br), br); err != nil {
					return false
				}
				c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
				if c == nil {
					return false
				}
				return c.Status == metav1.ConditionFalse && c.Reason == v1alpha1.XVolNotFoundReason
			})
		})
	})
}

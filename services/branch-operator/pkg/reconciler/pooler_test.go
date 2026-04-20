package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPoolerReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("pooler is created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPooler().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the Pooler to be created
			requireEventuallyNoErr(t, func() error {
				pooler := apiv1.Pooler{}
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})
		})
	})

	t.Run("pooler is owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPooler().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			pooler := apiv1.Pooler{}

			// Expect the Pooler to be created with the correct owner reference
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})
			require.Len(t, pooler.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, pooler.GetOwnerReferences()[0].Name)
		})
	})

	t.Run("pooler direct changes are reverted", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPooler().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			pooler := apiv1.Pooler{}

			// Expect the Pooler to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})

			// Directly modify the Pooler's max_client_conn
			err := retryOnConflict(ctx, &pooler, func(p *apiv1.Pooler) {
				p.Spec.PgBouncer.Parameters["max_client_conn"] = "9999"
			})
			require.NoError(t, err)

			// Expect the Pooler spec to be reverted to the correct max_client_conn
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
				if err != nil {
					return false
				}
				return pooler.Spec.PgBouncer.Parameters["max_client_conn"] == "100"
			})
		})
	})

	t.Run("pooler hibernation is reconciled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithPooler().
			WithHibernationMode(v1alpha1.HibernationModeDisabled).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			pooler := apiv1.Pooler{}

			// Expect the Pooler to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})

			// Expect the Pooler to have 1 instance
			require.Equal(t, int32(1), *pooler.Spec.Instances)

			// Enable hibernation on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			})
			require.NoError(t, err)

			// Expect the Pooler to be scaled to 0 instances
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
				if err != nil {
					return false
				}
				return pooler.Spec.Instances != nil && *pooler.Spec.Instances == 0
			})

			// Disable hibernation on the Branch
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeDisabled)
			})
			require.NoError(t, err)

			// Expect the Pooler to be scaled back to 1 instance
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
				if err != nil {
					return false
				}
				return pooler.Spec.Instances != nil && *pooler.Spec.Instances == 1
			})
		})
	})

	t.Run("pooler inherits labels from branch metadata", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithPooler().
			WithInheritedMetadata(&v1alpha1.InheritedMetadata{
				Labels: map[string]string{
					"xata.io/organizationID": "org-123",
					"xata.io/projectID":      "proj-456",
				},
			}).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			pooler := apiv1.Pooler{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})

			// Verify labels are set on the Pooler resource
			require.Equal(t, "org-123", pooler.Labels["xata.io/organizationID"])
			require.Equal(t, "proj-456", pooler.Labels["xata.io/projectID"])

			// Verify labels are propagated to the pod template
			require.NotNil(t, pooler.Spec.Template)
			require.Equal(t, "org-123", pooler.Spec.Template.ObjectMeta.Labels["xata.io/organizationID"])
			require.Equal(t, "proj-456", pooler.Spec.Template.ObjectMeta.Labels["xata.io/projectID"])
		})
	})

	t.Run("branch without pooler spec does not create a pooler", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for reconciliation to complete by checking the Branch is ready
			requireEventuallyTrue(t, func() bool {
				branch := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &branch); err != nil {
					return false
				}
				return branch.Status.ObservedGeneration == branch.Generation
			})

			// Verify no Pooler was created
			pooler := apiv1.Pooler{}
			err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			require.True(t, apierrors.IsNotFound(err))
		})
	})

	t.Run("pooler is not created when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch spec with a pooler but without a cluster name
		branch := NewBranchBuilder().WithPooler().WithClusterName(nil).Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Wait for reconciliation to complete
			requireEventuallyTrue(t, func() bool {
				branch := v1alpha1.Branch{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &branch); err != nil {
					return false
				}
				return branch.Status.ObservedGeneration == branch.Generation
			})

			// Verify no Pooler was created
			pooler := apiv1.Pooler{}
			err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			require.True(t, apierrors.IsNotFound(err))
		})
	})

	t.Run("pooler is deleted when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a Branch spec with a pooler and a cluster name
		branch := NewBranchBuilder().WithPooler().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			pooler := apiv1.Pooler{}

			// Expect the Pooler to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
			})

			// Unset the cluster name on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the Pooler to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name+reconciler.PoolerSuffix, &pooler)
				return apierrors.IsNotFound(err)
			})
		})
	})
}

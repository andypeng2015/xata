package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestObjectStoreReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("objectstore is created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the ObjectStore to be created
			requireEventuallyNoErr(t, func() error {
				os := barmanPluginApi.ObjectStore{}
				return getK8SObject(ctx, br.Name, &os)
			})
		})
	})

	t.Run("objectstore is owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			os := barmanPluginApi.ObjectStore{}

			// Expect the ObjectStore to be created with the correct owner reference
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &os)
			})
			require.Len(t, os.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, os.GetOwnerReferences()[0].Name)
		})
	})

	t.Run("objectstore references the chart-managed dummy region secret", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			os := barmanPluginApi.ObjectStore{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &os)
			})

			require.NotNil(t, os.Spec.Configuration.AWS)
			require.NotNil(t, os.Spec.Configuration.AWS.RegionReference)
			require.Equal(t, "barman-dummy-secret", os.Spec.Configuration.AWS.RegionReference.Name)
			require.Equal(t, "dummy", os.Spec.Configuration.AWS.RegionReference.Key)
		})
	})

	t.Run("objectstore is updated when Branch spec is updated", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			os := barmanPluginApi.ObjectStore{}

			// Expect the ObjectStore to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &os)
			})

			// Expect the ObjectStore to have the initial retention period
			require.Equal(t, "1d", os.Spec.RetentionPolicy)

			// Update the Branch's backup retention period
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Retention = "200d"
			})
			require.NoError(t, err)

			// Expect the ObjectStore to be updated with the new retention
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &os)
				if err != nil {
					return false
				}
				return os.Spec.RetentionPolicy == "200d"
			})
		})
	})

	t.Run("objectstore is deleted when Branch spec is removed", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("100d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			os := barmanPluginApi.ObjectStore{}

			// Expect the ObjectStore to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &os)
			})

			// Remove the backup configuration from the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec = nil
			})
			require.NoError(t, err)

			// Expect the ObjectStore to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &os)
				return apierrors.IsNotFound(err)
			})
		})
	})

	t.Run("objectstore is not created for pgbackrest branches", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithPgBackRest("test-bucket", "us-east-1").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			requireEventuallyTrue(t, func() bool {
				os := barmanPluginApi.ObjectStore{}
				err := getK8SObject(ctx, br.Name, &os)
				return apierrors.IsNotFound(err)
			})
		})
	})

	t.Run("objectstore is deleted when switching from barman to pgbackrest", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the ObjectStore to be created for barman
			requireEventuallyNoErr(t, func() error {
				os := barmanPluginApi.ObjectStore{}
				return getK8SObject(ctx, br.Name, &os)
			})

			// Switch to pgbackrest
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					Bucket:             "test-bucket",
					Region:             "us-east-1",
					InheritFromIAMRole: true,
				}
			})
			require.NoError(t, err)

			// Expect the ObjectStore to be deleted
			requireEventuallyTrue(t, func() bool {
				os := barmanPluginApi.ObjectStore{}
				err := getK8SObject(ctx, br.Name, &os)
				return apierrors.IsNotFound(err)
			})
		})
	})

	t.Run("objectstore direct changes are reverted", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			os := barmanPluginApi.ObjectStore{}

			// Expect the ObjectStore to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &os)
			})

			// Directly modify the ObjectStore's retention policy
			err := retryOnConflict(ctx, &os, func(o *barmanPluginApi.ObjectStore) {
				o.Spec.RetentionPolicy = "300d"
			})
			require.NoError(t, err)

			// Expect the ObjectStore spec to be reverted to the correct retention
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &os)
				if err != nil {
					return false
				}
				return os.Spec.RetentionPolicy == "1d"
			})
		})
	})
}

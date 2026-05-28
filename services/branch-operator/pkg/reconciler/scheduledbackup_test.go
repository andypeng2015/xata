package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
)

func TestScheduledBackupReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("scheduledbackup is created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the ScheduledBackup to be created
			requireEventuallyNoErr(t, func() error {
				sb := apiv1.ScheduledBackup{}
				return getK8SObject(ctx, br.Name, &sb)
			})
		})
	})

	t.Run("scheduledbackup is owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created with the correct owner reference
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})
			require.Len(t, sb.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, sb.GetOwnerReferences()[0].Name)
		})
	})

	t.Run("scheduledbackup is updated when Branch spec is updated", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			// Expect the ScheduledBackup to have the initial schedule
			require.Equal(t, "0 0 0 * * *", sb.Spec.Schedule)

			// Update the Branch's backup schedule
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.ScheduledBackup.Schedule = "0 0 2 * * *"
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to be updated with the new schedule
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Schedule == "0 0 2 * * *"
			})
		})
	})

	t.Run("scheduledbackup is deleted when BackupConfiguration is removed", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			// Remove the backup configuration from the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec = nil
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				return apierrors.IsNotFound(err)
			})
		})
	})

	t.Run("scheduledbackup is suspended iff Branch is hibernated", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			// Expect the ScheduledBackup to not be suspended
			require.False(t, *sb.Spec.Suspend)

			// Hibernate the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to be suspended
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Suspend != nil && *sb.Spec.Suspend
			})

			// Un-hibernate the Branch
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeDisabled)
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to not be suspended
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Suspend != nil && !*sb.Spec.Suspend
			})
		})
	})

	t.Run("scheduledbackup is suspended when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created and not suspended
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})
			require.False(t, *sb.Spec.Suspend)

			// Remove the cluster name from the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to be suspended
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Suspend != nil && *sb.Spec.Suspend
			})

			// Re-set the cluster name on the Branch
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new(br.Name)
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup to not be suspended
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Suspend != nil && !*sb.Spec.Suspend
			})
		})
	})

	t.Run("scheduledbackup direct changes are reverted", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithBackupRetention("1d").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			// Expect the ScheduledBackup to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			// Directly modify the ScheduledBackup's schedule
			err := retryOnConflict(ctx, &sb, func(s *apiv1.ScheduledBackup) {
				s.Spec.Schedule = "0 0 4 * * *"
			})
			require.NoError(t, err)

			// Expect the ScheduledBackup spec to be reverted to the correct schedule
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Schedule == "0 0 0 * * *"
			})
		})
	})

	t.Run("pgbackrest scheduledbackup uses correct method and type", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithPgBackRest("test-bucket", "us-east-1").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			require.Equal(t, apiv1.BackupMethodPgBackRest, sb.Spec.Method)
			require.Equal(t, apiv1.PgBackRestBackupTypeFull, sb.Spec.PgBackRestBackupType)
			require.Nil(t, sb.Spec.PluginConfiguration)
		})
	})

	t.Run("pgbackrest scheduledbackup is updated when schedule changes", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithPgBackRest("test-bucket", "us-east-1").
			WithBackupSchedule("0 0 0 * * *").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sb := apiv1.ScheduledBackup{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})
			require.Equal(t, "0 0 0 * * *", sb.Spec.Schedule)

			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.ScheduledBackup.Schedule = "0 0 2 * * *"
			})
			require.NoError(t, err)

			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &sb)
				if err != nil {
					return false
				}
				return sb.Spec.Schedule == "0 0 2 * * *" &&
					sb.Spec.Method == apiv1.BackupMethodPgBackRest
			})
		})
	})
}

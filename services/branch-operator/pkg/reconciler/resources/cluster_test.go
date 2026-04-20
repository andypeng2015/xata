package resources_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

func TestClusterSpec(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		cfgModifier func(*resources.ClusterConfig)
		expected    apiv1.ClusterSpec
	}{
		{
			name:        "basic - minimal configuration",
			cfgModifier: nil,
			expected:    NewClusterSpecBuilder().Build(),
		},
		{
			name: "instances - number of instances set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Instances = 3
			},
			expected: NewClusterSpecBuilder().
				WithInstances(3).
				WithEnablePDB(true).
				Build(),
		},
		{
			name: "enforce zone - pod anti-affinity enabled",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.EnforceZone = true
			},
			expected: NewClusterSpecBuilder().
				WithAffinity(apiv1.AffinityConfiguration{
					TopologyKey:         "topology.kubernetes.io/zone",
					PodAntiAffinityType: "preferred",
				}).
				Build(),
		},
		{
			name: "storage size - storage size is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.Size = "100Gi"
			},
			expected: NewClusterSpecBuilder().
				WithStorageSize("100Gi").
				Build(),
		},
		{
			name: "storage class - storage class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.StorageClass = new("some-new-storage-class")
			},
			expected: NewClusterSpecBuilder().
				WithStorageClass(new("some-new-storage-class")).
				Build(),
		},
		{
			name: "volume snapshot class - volume snapshot class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.VolumeSnapshotClass = new("some-other-snapshot-class")
			},
			expected: NewClusterSpecBuilder().
				WithVolumeSnapshotClass("some-other-snapshot-class").
				Build(),
		},
		{
			name: "postgres image - postgres image is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Image = "ghcr.io/some/postgres:latest"
			},
			expected: NewClusterSpecBuilder().
				WithPostgresImage("ghcr.io/some/postgres:latest").
				Build(),
		},
		{
			name: "resources - cpu and memory specified",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2000m"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				}
			},
			expected: NewClusterSpecBuilder().
				WithResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2000m"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				}).
				Build(),
		},
		{
			name: "labels - custom labels in inherited metadata",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.InheritedMetadata = &v1alpha1.InheritedMetadata{
					Labels: map[string]string{"app": "my-app"},
				}
			},
			expected: NewClusterSpecBuilder().
				WithLabels(map[string]string{"app": "my-app"}).
				Build(),
		},
		{
			name: "backup configuration - barman plugin enabled when backup schedule is configured",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention: "30d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 0 0 * * *",
					},
				}
			},
			expected: NewClusterSpecBuilder().
				WithBarmanPluginEnabled(true).
				Build(),
		},
		{
			name: "backup configuration - barman plugin enabled when WAL archiving is enabled",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
				}
			},
			expected: NewClusterSpecBuilder().
				WithBarmanPluginEnabled(true).
				Build(),
		},
		{
			name: "backup configuration - barman plugin disabled when no WAL archiving or backup schedule configured",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention: "30d",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBarmanPluginEnabled(false).
				Build(),
		},
		{
			name: "backup configuration - serverName set when different from branch name",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
					ServerName:   "custom-server",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBarmanPluginEnabled(true).
				WithBarmanServerName("custom-server").
				Build(),
		},
		{
			name: "backup configuration - serverName omitted when matching branch name",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
					ServerName:   "test-branch",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBarmanPluginEnabled(true).
				Build(),
		},
		{
			name: "node selector - scheduling constraints applied",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Affinity = &v1alpha1.AffinitySpec{
					NodeSelector: map[string]string{
						"node.kubernetes.io/instance-type": "m5.2xlarge",
					},
				}
			},
			expected: NewClusterSpecBuilder().
				WithNodeSelector(map[string]string{
					"node.kubernetes.io/instance-type": "m5.2xlarge",
				}).
				Build(),
		},
		{
			name: "tolerations - pod tolerations for tainted nodes",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Tolerations = []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "database",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				}
			},
			expected: NewClusterSpecBuilder().
				WithTolerations([]corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "database",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				}).
				Build(),
		},
		{
			name: "image pull secrets - multiple secrets configured for private registries",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.ImagePullSecrets = []string{"ghcr-secret", "ecr-secret"}
			},
			expected: NewClusterSpecBuilder().
				WithImagePullSecrets([]apiv1.LocalObjectReference{
					{Name: "ghcr-secret"},
					{Name: "ecr-secret"},
				}).
				Build(),
		},
		{
			name: "postgres configuration - postgres parameters and shared libraries are set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Postgres = &v1alpha1.PostgresConfiguration{
					Parameters: []v1alpha1.PostgresParameter{
						{Name: "max_connections", Value: "200"},
					},
					SharedPreloadLibraries: []string{"pg_stat_statements"},
				}
			},
			expected: NewClusterSpecBuilder().
				WithSharedPreloadLibraries([]string{"pg_stat_statements"}).
				WithPostgresParameters(map[string]string{"max_connections": "200"}).
				Build(),
		},
		{
			name: "recovery - recovery from volume snapshot",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeVolumeSnapshot,
					Name: "some-parent-cluster",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBootstrapConfig(&apiv1.BootstrapConfiguration{
					Recovery: &apiv1.BootstrapRecovery{
						Database: "xata",
						Owner:    "xata",
						Secret: &apiv1.LocalObjectReference{
							Name: "test-branch-app",
						},
						VolumeSnapshots: &apiv1.DataSource{
							Storage: corev1.TypedLocalObjectReference{
								Name:     "some-parent-cluster-test-branch",
								APIGroup: new("snapshot.storage.k8s.io"),
								Kind:     "VolumeSnapshot",
							},
						},
						RecoveryTarget: &apiv1.RecoveryTarget{
							TargetImmediate: new(true),
						},
					},
				}).
				Build(),
		},
		{
			name: "recovery - recovery from object store (PITR) without timestamp",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBootstrapConfig(&apiv1.BootstrapConfiguration{
					Recovery: &apiv1.BootstrapRecovery{
						Database: "xata",
						Owner:    "xata",
						Secret: &apiv1.LocalObjectReference{
							Name: "test-branch-app",
						},
						Source: "source-cluster",
					},
				}).
				WithExternalClusters([]apiv1.ExternalCluster{
					{
						Name: "source-cluster",
						PluginConfiguration: &apiv1.PluginConfiguration{
							Name: "barman-cloud.cloudnative-pg.io",
							Parameters: map[string]string{
								"barmanObjectName": "source-cluster",
								"serverName":       "source-cluster",
							},
						},
					},
				}).
				Build(),
		},
		{
			name: "recovery - recovery from object store (PITR) with timestamp",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				timestamp := metav1.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type:      v1alpha1.RestoreTypeObjectStore,
					Name:      "source-cluster",
					Timestamp: &timestamp,
				}
			},
			expected: NewClusterSpecBuilder().
				WithBootstrapConfig(&apiv1.BootstrapConfiguration{
					Recovery: &apiv1.BootstrapRecovery{
						Database: "xata",
						Owner:    "xata",
						Secret: &apiv1.LocalObjectReference{
							Name: "test-branch-app",
						},
						Source: "source-cluster",
						RecoveryTarget: &apiv1.RecoveryTarget{
							TargetTime: "2025-01-15 10:30:00.000000",
						},
					},
				}).
				WithExternalClusters([]apiv1.ExternalCluster{
					{
						Name: "source-cluster",
						PluginConfiguration: &apiv1.PluginConfiguration{
							Name: "barman-cloud.cloudnative-pg.io",
							Parameters: map[string]string{
								"barmanObjectName": "source-cluster",
								"serverName":       "source-cluster",
							},
						},
					},
				}).
				Build(),
		},
		{
			name: "recovery - object store with custom serverName",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type:       v1alpha1.RestoreTypeObjectStore,
					Name:       "source-cluster",
					ServerName: "some-other-servername",
				}
			},
			expected: NewClusterSpecBuilder().
				WithBootstrapConfig(&apiv1.BootstrapConfiguration{
					Recovery: &apiv1.BootstrapRecovery{
						Database: "xata",
						Owner:    "xata",
						Secret: &apiv1.LocalObjectReference{
							Name: "test-branch-app",
						},
						Source: "source-cluster",
					},
				}).
				WithExternalClusters([]apiv1.ExternalCluster{
					{
						Name: "source-cluster",
						PluginConfiguration: &apiv1.PluginConfiguration{
							Name: "barman-cloud.cloudnative-pg.io",
							Parameters: map[string]string{
								"barmanObjectName": "source-cluster",
								"serverName":       "some-other-servername",
							},
						},
					},
				}).
				Build(),
		},
		{
			name: "smart shutdown timeout - custom value set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.SmartShutdownTimeout = ptr.To[int32](300)
			},
			expected: NewClusterSpecBuilder().
				WithSmartShutdownTimeout(300).
				Build(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := baseClusterConfig()
			if tc.cfgModifier != nil {
				tc.cfgModifier(&cfg)
			}

			spec := resources.ClusterSpec("test-branch", "test-branch", cfg)

			require.Equal(t, tc.expected, spec)
		})
	}
}

// baseClusterConfig provides a base ClusterConfig for tests on which each
// testcase can apply modifications
func baseClusterConfig() resources.ClusterConfig {
	return resources.ClusterConfig{
		ClusterSpec: v1alpha1.ClusterSpec{
			Instances: 1,
			Storage: v1alpha1.StorageSpec{
				Size:                "10Gi",
				VolumeSnapshotClass: new("snapshot-class"),
			},
			Image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7",
		},
	}
}

func TestGeneratePostInitSQL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		majorVersion       int
		expectedPrivileges []string
		shouldNotContain   []string
		description        string
	}{
		{
			name:         "pg 14 - base privileges only",
			majorVersion: 14,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
			},
			shouldNotContain: []string{
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "PG 14 should only have base privileges",
		},
		{
			name:         "pg 15 - base + pg_checkpoint",
			majorVersion: 15,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
			},
			shouldNotContain: []string{
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "PG 15 should have base privileges plus pg_checkpoint",
		},
		{
			name:         "pg 16 - all privileges",
			majorVersion: 16,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			description:      "PG 16 should have all privileges",
		},
		{
			name:         "pg 17 - all privileges",
			majorVersion: 17,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			description:      "PG 17 should have all privileges",
		},
		{
			name:         "pg 18 - all privileges",
			majorVersion: 18,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			description:      "PG 18 should have all privileges",
		},
		{
			name:         "unknown version - defaults to base only",
			majorVersion: 0,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
			},
			shouldNotContain: []string{
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "Unknown version should default to base privileges only",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql := resources.GeneratePostInitSQL(tc.majorVersion)

			// Verify basic structure
			require.GreaterOrEqual(t, len(sql), 6, "Should have at least 6 SQL statements")
			require.Contains(t, sql[0], "CREATE ROLE xata_superuser")
			require.Contains(t, sql[1], "GRANT")
			require.Contains(t, sql[2], "GRANT xata_superuser TO xata")

			// Extract GRANT statement and verify privileges
			grantStatement := sql[1]
			for _, privilege := range tc.expectedPrivileges {
				require.Contains(t, grantStatement, privilege, "GRANT statement should contain %s for %s", privilege, tc.description)
			}

			for _, privilege := range tc.shouldNotContain {
				require.NotContains(t, grantStatement, privilege, "GRANT statement should not contain %s for %s", privilege, tc.description)
			}
		})
	}
}

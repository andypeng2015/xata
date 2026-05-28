package resources_test

import (
	"testing"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"

	barmanApi "github.com/cloudnative-pg/barman-cloud/pkg/api"
	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestNetworkPolicySpec(t *testing.T) {
	t.Parallel()

	testcases := []string{
		"test-branch-1",
		"test-branch-2",
	}

	dnsPort := intstr.FromInt(53)
	dnsProtocolUDP := v1.ProtocolUDP
	dnsProtocolTCP := v1.ProtocolTCP

	for _, branchName := range testcases {
		t.Run(branchName, func(t *testing.T) {
			spec := resources.NetworkPolicySpec(branchName)

			matchLabels := map[string]string{
				"cnpg.io/cluster": branchName,
			}

			expected := networkingv1.NetworkPolicySpec{
				PodSelector: metav1.LabelSelector{MatchLabels: matchLabels},
				Ingress: []networkingv1.NetworkPolicyIngressRule{
					{
						From: []networkingv1.NetworkPolicyPeer{
							{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
						},
					},
				},
				Egress: []networkingv1.NetworkPolicyEgressRule{
					{
						To: []networkingv1.NetworkPolicyPeer{
							{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
						},
					},
					{
						Ports: []networkingv1.NetworkPolicyPort{
							{Port: &dnsPort, Protocol: &dnsProtocolUDP},
							{Port: &dnsPort, Protocol: &dnsProtocolTCP},
						},
					},
				},
				PolicyTypes: []networkingv1.PolicyType{
					networkingv1.PolicyTypeIngress,
					networkingv1.PolicyTypeEgress,
				},
			}

			require.Equal(t, expected, spec)
		})
	}
}

func TestClustersServiceSpec(t *testing.T) {
	t.Parallel()

	spec := resources.ClustersServiceSpec()

	expected := v1.ServiceSpec{
		Type: v1.ServiceTypeClusterIP,
		Ports: []v1.ServicePort{
			{
				Name:       "grpc",
				Port:       5002,
				TargetPort: intstr.FromInt(5002),
				Protocol:   v1.ProtocolTCP,
			},
		},
		Selector: map[string]string{
			"app": "clusters",
		},
	}

	require.Equal(t, expected, spec)
}

func TestAdditionalServiceSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		clusterName  string
		selectorType string
		want         v1.ServiceSpec
	}{
		"rw targets primary instance": {
			clusterName:  "test-cluster",
			selectorType: "rw",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster":      "test-cluster",
					"cnpg.io/instanceRole": "primary",
				},
			},
		},
		"r targets all instances": {
			clusterName:  "test-cluster",
			selectorType: "r",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster": "test-cluster",
					"cnpg.io/podRole": "instance",
				},
			},
		},
		"ro targets replica instances": {
			clusterName:  "test-cluster",
			selectorType: "ro",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "postgres",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/cluster":      "test-cluster",
					"cnpg.io/instanceRole": "replica",
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.AdditionalServiceSpec(tc.clusterName, tc.selectorType)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestPoolerServiceSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		poolerName string
		want       v1.ServiceSpec
	}{
		"routes to pooler pods": {
			poolerName: "test-branch-1-pooler",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "pgbouncer",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/podRole":    "pooler",
					"cnpg.io/poolerName": "test-branch-1-pooler",
				},
			},
		},
		"different pooler name": {
			poolerName: "test-branch-2-pooler",
			want: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
				Ports: []v1.ServicePort{
					{
						Name:       "pgbouncer",
						Port:       5432,
						TargetPort: intstr.FromInt(5432),
						Protocol:   v1.ProtocolTCP,
					},
				},
				Selector: map[string]string{
					"cnpg.io/podRole":    "pooler",
					"cnpg.io/poolerName": "test-branch-2-pooler",
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.PoolerServiceSpec(tc.poolerName)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestObjectStoreSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name             string
		backupsBucket    string
		backupsEndpoint  string
		regionSecretName string
		regionSecretKey  string
		retention        string
		want             barmanPluginApi.ObjectStoreSpec
	}{
		{
			name:             "production mode with IAM role - bucket A",
			backupsBucket:    "s3://prod-backup-bucket/path/to/backups",
			backupsEndpoint:  "",
			regionSecretName: "barman-dummy-secret",
			regionSecretKey:  "dummy",
			retention:        "60d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "60d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://prod-backup-bucket/path/to/backups",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
							InheritFromIAMRole: true,
							RegionReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "barman-dummy-secret",
								},
								Key: "dummy",
							},
						},
					},
					Wal: &apiv1.WalBackupConfiguration{
						Compression: barmanApi.CompressionTypeGzip,
					},
					Data: &apiv1.DataBackupConfiguration{
						Compression:           barmanApi.CompressionTypeGzip,
						AdditionalCommandArgs: []string{"--min-chunk-size=5MB", "--read-timeout=60", "-vv"},
					},
				},
				InstanceSidecarConfiguration: barmanPluginApi.InstanceSidecarConfiguration{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("100m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					RetentionPolicyIntervalSeconds: 86400,
				},
			},
		},
		{
			name:             "production mode with IAM role - bucket without region suffix",
			backupsBucket:    "s3://another-prod-bucket/different/path",
			backupsEndpoint:  "",
			regionSecretName: "custom-barman-region-secret",
			regionSecretKey:  "region",
			retention:        "30d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "30d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://another-prod-bucket/different/path",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
							InheritFromIAMRole: true,
							RegionReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "custom-barman-region-secret",
								},
								Key: "region",
							},
						},
					},
					Wal: &apiv1.WalBackupConfiguration{
						Compression: barmanApi.CompressionTypeGzip,
					},
					Data: &apiv1.DataBackupConfiguration{
						Compression:           barmanApi.CompressionTypeGzip,
						AdditionalCommandArgs: []string{"--min-chunk-size=5MB", "--read-timeout=60", "-vv"},
					},
				},
				InstanceSidecarConfiguration: barmanPluginApi.InstanceSidecarConfiguration{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("100m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					RetentionPolicyIntervalSeconds: 86400,
				},
			},
		},
		{
			name:            "local mode with MinIO - bucket C",
			backupsBucket:   "s3://dev-bucket/backups",
			backupsEndpoint: "http://minio.local:9000",
			retention:       "7d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "7d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://dev-bucket/backups",
					EndpointURL:     "http://minio.local:9000",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
							AccessKeyIDReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "minio-eu",
								},
								Key: "rootUser",
							},
							SecretAccessKeyReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "minio-eu",
								},
								Key: "rootPassword",
							},
							InheritFromIAMRole: false,
						},
					},
					Wal: &apiv1.WalBackupConfiguration{
						Compression: barmanApi.CompressionTypeGzip,
					},
					Data: &apiv1.DataBackupConfiguration{
						Compression:           barmanApi.CompressionTypeGzip,
						AdditionalCommandArgs: []string{"--min-chunk-size=5MB", "--read-timeout=60", "-vv"},
					},
				},
				InstanceSidecarConfiguration: barmanPluginApi.InstanceSidecarConfiguration{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("100m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					RetentionPolicyIntervalSeconds: 86400,
				},
			},
		},
		{
			name:            "local mode with MinIO - bucket D",
			backupsBucket:   "s3://test-bucket/test/path",
			backupsEndpoint: "http://minio-test.cluster.local:9000",
			retention:       "14d",
			want: barmanPluginApi.ObjectStoreSpec{
				RetentionPolicy: "14d",
				Configuration: apiv1.BarmanObjectStoreConfiguration{
					DestinationPath: "s3://test-bucket/test/path",
					EndpointURL:     "http://minio-test.cluster.local:9000",
					BarmanCredentials: apiv1.BarmanCredentials{
						AWS: &apiv1.S3Credentials{
							AccessKeyIDReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "minio-eu",
								},
								Key: "rootUser",
							},
							SecretAccessKeyReference: &apiv1.SecretKeySelector{
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "minio-eu",
								},
								Key: "rootPassword",
							},
							InheritFromIAMRole: false,
						},
					},
					Wal: &apiv1.WalBackupConfiguration{
						Compression: barmanApi.CompressionTypeGzip,
					},
					Data: &apiv1.DataBackupConfiguration{
						Compression:           barmanApi.CompressionTypeGzip,
						AdditionalCommandArgs: []string{"--min-chunk-size=5MB", "--read-timeout=60", "-vv"},
					},
				},
				InstanceSidecarConfiguration: barmanPluginApi.InstanceSidecarConfiguration{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("100m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("250m"),
							v1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					RetentionPolicyIntervalSeconds: 86400,
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.ObjectStoreSpec(
				tc.backupsBucket,
				tc.backupsEndpoint,
				tc.regionSecretName,
				tc.regionSecretKey,
				tc.retention,
			)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestScheduledBackupSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name       string
		method     v1alpha1.BackupMethod
		branchName string
		schedule   string
		suspend    bool
		want       apiv1.ScheduledBackupSpec
	}{
		{
			name:       "barman scheduled backup with hourly schedule",
			method:     v1alpha1.BackupMethodBarman,
			branchName: "test-branch-1",
			schedule:   "0 0 * * * *",
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-1",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 0 * * * *",
				Immediate: new(true),
				Suspend:   new(false),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
		{
			name:       "barman scheduled backup with very frequent schedule",
			method:     v1alpha1.BackupMethodBarman,
			branchName: "test-branch-2",
			schedule:   "0 * * * * *",
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 * * * * *",
				Immediate: new(true),
				Suspend:   new(false),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
		{
			name:       "barman suspended scheduled backup",
			method:     v1alpha1.BackupMethodBarman,
			branchName: "test-branch-2",
			schedule:   "0 * * * * *",
			suspend:    true,
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Method:    apiv1.BackupMethodPlugin,
				Schedule:  "0 * * * * *",
				Immediate: new(true),
				Suspend:   new(true),
				PluginConfiguration: &apiv1.BackupPluginConfiguration{
					Name: "barman-cloud.cloudnative-pg.io",
				},
			},
		},
		{
			name:       "pgbackrest scheduled backup",
			method:     v1alpha1.BackupMethodPgBackRest,
			branchName: "test-branch-1",
			schedule:   "0 0 0 * * *",
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-1",
				},
				Method:               apiv1.BackupMethodPgBackRest,
				Schedule:             "0 0 0 * * *",
				Immediate:            new(true),
				Suspend:              new(false),
				PgBackRestBackupType: apiv1.PgBackRestBackupTypeFull,
			},
		},
		{
			name:       "pgbackrest suspended scheduled backup",
			method:     v1alpha1.BackupMethodPgBackRest,
			branchName: "test-branch-2",
			schedule:   "0 0 2 * * 0",
			suspend:    true,
			want: apiv1.ScheduledBackupSpec{
				Cluster: apiv1.LocalObjectReference{
					Name: "test-branch-2",
				},
				Method:               apiv1.BackupMethodPgBackRest,
				Schedule:             "0 0 2 * * 0",
				Immediate:            new(true),
				Suspend:              new(true),
				PgBackRestBackupType: apiv1.PgBackRestBackupTypeFull,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.ScheduledBackupSpec(tc.branchName, tc.schedule, tc.suspend, tc.method)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestVolumeSnapshotSpec(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name          string
		sourcePVC     string
		snapshotClass string
		want          snapshotv1.VolumeSnapshotSpec
	}{
		{
			name:          "source PVC and snapshot class",
			sourcePVC:     "some-pvc-name",
			snapshotClass: "some-snapshot-class",
			want: snapshotv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: new("some-snapshot-class"),
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: new("some-pvc-name"),
				},
			},
		},
		{
			name:          "different PVC and snapshot class",
			sourcePVC:     "some-other-pvc-name",
			snapshotClass: "some-other-snapshot-class",
			want: snapshotv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: new("some-other-snapshot-class"),
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: new("some-other-pvc-name"),
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := resources.VolumeSnapshotSpec(tc.sourcePVC, tc.snapshotClass)

			require.Equal(t, tc.want, got)
		})
	}
}

func TestSecret(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		name      string
		namespace string
		username  string
		password  string
		want      *v1.Secret
	}{
		"superuser secret": {
			name:      "branch-1-superuser",
			namespace: "xata-clusters",
			username:  "postgres",
			password:  "supersecret",
			want: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "branch-1-superuser",
					Namespace: "xata-clusters",
				},
				Type: v1.SecretTypeBasicAuth,
				Data: map[string][]byte{
					v1.BasicAuthUsernameKey: []byte("postgres"),
					v1.BasicAuthPasswordKey: []byte("supersecret"),
				},
			},
		},
		"app secret": {
			name:      "branch-1-app",
			namespace: "xata-clusters",
			username:  "xata",
			password:  "appsecret",
			want: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "branch-1-app",
					Namespace: "xata-clusters",
				},
				Type: v1.SecretTypeBasicAuth,
				Data: map[string][]byte{
					v1.BasicAuthUsernameKey: []byte("xata"),
					v1.BasicAuthPasswordKey: []byte("appsecret"),
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := resources.Secret(tc.name, tc.namespace, tc.username, tc.password)

			require.Equal(t, tc.want, got)
		})
	}
}

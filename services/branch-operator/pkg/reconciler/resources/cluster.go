package resources

import (
	"fmt"
	"strings"

	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"

	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
)

// InheritedAnnotations are defined on the Cluster; CNPG will propagate them to
// all resources it creates
var InheritedAnnotations = map[string]string{
	// TLS is enabled on the metrics endpoint
	"prometheus.io/scheme": "https",

	// Mark cluster services as Cilium global services
	"service.cilium.io/global": "true",
}

// PostgreSQL version-specific privileges for xata_superuser role
var (
	// pg14Privileges contains privileges available in PostgreSQL 14 and earlier
	pg14Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_read_all_settings",
		"pg_read_all_stats",
	}

	// pg15Privileges contains privileges available in PostgreSQL 15 (adds pg_checkpoint)
	pg15Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
	}

	// pg16Privileges contains privileges available in PostgreSQL 16 (adds pg_create_subscription, pg_use_reserved_connections)
	pg16Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
		"pg_create_subscription",
		"pg_use_reserved_connections",
	}

	// pg17Privileges contains privileges available in PostgreSQL 17+ (adds pg_maintain)
	pg17Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_maintain",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
		"pg_create_subscription",
		"pg_use_reserved_connections",
	}
)

// GeneratePostInitSQL generates PostInitSQL statements based on PostgreSQL major version
func GeneratePostInitSQL(majorVersion int) []string {
	// Select privileges based on version
	var privileges []string
	switch {
	case majorVersion >= 17:
		privileges = pg17Privileges
	case majorVersion >= 16:
		privileges = pg16Privileges
	case majorVersion >= 15:
		privileges = pg15Privileges
	default:
		// PG 14 or unknown version - use base privileges
		privileges = pg14Privileges
	}

	grantStatement := fmt.Sprintf("GRANT %s TO xata_superuser;", strings.Join(privileges, ", "))

	return []string{
		// These are executed as the superuser.
		// Create a pseudo-superuser role that we then assign to the `xata` user.
		"CREATE ROLE xata_superuser NOLOGIN;",
		grantStatement,
		// make `xata` the pseudo-superuser
		"GRANT xata_superuser TO xata;",
		// Allow the xata role to own schemas in the `postgres` database, but not the database itself, so it cannot drop it.
		"ALTER ROLE xata LOGIN INHERIT CREATEDB CREATEROLE BYPASSRLS REPLICATION;",
		"ALTER SCHEMA public OWNER TO xata;",
		"GRANT CREATE ON DATABASE postgres TO xata;",
	}
}

// ClusterConfig defines all the configuration needed to build a CNPG Cluster
// resource. It combines the user-defined config from the Branch spec together
// with environmental config defined in the operator
type ClusterConfig struct {
	v1alpha1.ClusterSpec
	*v1alpha1.BackupSpec
	InheritedMetadata *v1alpha1.InheritedMetadata
	RestoreSpec       *v1alpha1.RestoreSpec
	Tolerations       []corev1.Toleration
	EnforceZone       bool
	ImagePullSecrets  []string
}

// ClusterSpec generates the CNPG Cluster spec from the provided configuration
func ClusterSpec(
	branchName string,
	clusterName string,
	cfg ClusterConfig,
) apiv1.ClusterSpec {
	// Pod anti-affinity configuration
	var podAntiAffinityType string
	var topologyKey string
	if cfg.EnforceZone {
		podAntiAffinityType = "preferred"
		topologyKey = "topology.kubernetes.io/zone"
	}

	// Build image pull secrets
	//nolint:prealloc
	var imagePullSecrets []apiv1.LocalObjectReference
	for _, secretName := range cfg.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, apiv1.LocalObjectReference{
			Name: secretName,
		})
	}

	// Build Bootstrap configuration based on restore type:
	// - VolumeSnapshot: recover from a volume snapshot
	// - ObjectStore: recover from object storage (PITR)
	// - Otherwise: initialize a new database
	var recovery *apiv1.BootstrapRecovery
	var initDB *apiv1.BootstrapInitDB

	var restoreType v1alpha1.RestoreType
	if cfg.RestoreSpec != nil {
		restoreType = cfg.RestoreSpec.Type
	}

	switch restoreType {
	case v1alpha1.RestoreTypeVolumeSnapshot:
		recovery = VolumeSnapshotBootstrapRecovery(branchName, cfg.RestoreSpec.Name)
	case v1alpha1.RestoreTypeObjectStore:
		recovery = ObjectStoreBootstrapRecovery(branchName, cfg.RestoreSpec)
	default:
		// No restore specified - initialize a new database
		majorVersion := postgresversions.ExtractMajorVersionFromImage(cfg.Image)
		initDB = BootstrapInitDB(branchName, majorVersion)
	}
	bootstrapCfg := &apiv1.BootstrapConfiguration{InitDB: initDB, Recovery: recovery}

	instances := int(cfg.Instances)
	spec := apiv1.ClusterSpec{
		Instances: instances,
		EnablePDB: EnablePDB(instances),
		StorageConfiguration: apiv1.StorageConfiguration{
			Size:             cfg.Storage.Size,
			StorageClass:     cfg.Storage.StorageClass,
			MountPropagation: mountPropagationMode(cfg.Storage.MountPropagation),
		},
		ImageName:             cfg.Image,
		ImagePullSecrets:      imagePullSecrets,
		EnableSuperuserAccess: new(true),
		SuperuserSecret: &apiv1.LocalObjectReference{
			Name: branchName + "-superuser",
		},
		Bootstrap: bootstrapCfg,
		PostgresConfiguration: apiv1.PostgresConfiguration{
			Parameters:          PostgresParametersToMap(cfg.Postgres),
			AdditionalLibraries: cfg.Postgres.GetSharedPreloadLibraries(),
		},
		Plugins: []apiv1.PluginConfiguration{
			{
				Name: "cnpg-i-scale-to-zero.xata.io",
			},
			{
				Name:          "barman-cloud.cloudnative-pg.io",
				Enabled:       new(cfg.RequiresBarmanPlugin()),
				IsWALArchiver: new(true),
				Parameters:    BarmanPluginParameters(branchName, cfg.GetServerName()),
			},
		},
		Resources: cfg.Resources,
		Backup: &apiv1.BackupConfiguration{
			VolumeSnapshot: &apiv1.VolumeSnapshotConfiguration{
				ClassName: cfg.Storage.GetVolumeSnapshotClass(),
				Online:    new(true),
				OnlineConfiguration: apiv1.OnlineConfiguration{
					ImmediateCheckpoint: new(true),
				},
			},
		},
		Probes: &apiv1.ProbesConfiguration{
			Startup: &apiv1.ProbeWithStrategy{
				Probe: apiv1.Probe{
					TimeoutSeconds:   5,
					PeriodSeconds:    1,
					SuccessThreshold: 1,
					FailureThreshold: 3600,
				},
			},
		},
		Managed: &apiv1.ManagedConfiguration{
			Services: &apiv1.ManagedServices{
				DisabledDefaultServices: []apiv1.ServiceSelectorType{
					apiv1.ServiceSelectorTypeR,
					apiv1.ServiceSelectorTypeRO,
				},
			},
		},
		Monitoring: &apiv1.MonitoringConfiguration{
			TLSConfig: &apiv1.ClusterMonitoringTLSConfiguration{
				Enabled: true,
			},
			CustomQueriesConfigMap: []machineryapi.ConfigMapKeySelector{{
				Key: "metrics.yaml",
				LocalObjectReference: machineryapi.LocalObjectReference{
					Name: "cnpg-custom-metrics",
				},
			}},
		},
		Affinity: apiv1.AffinityConfiguration{
			NodeSelector:        cfg.Affinity.GetNodeSelector(),
			PodAntiAffinityType: podAntiAffinityType,
			Tolerations:         cfg.Tolerations,
			TopologyKey:         topologyKey,
		},
		InheritedMetadata: &apiv1.EmbeddedObjectMetadata{
			Annotations: InheritedAnnotations,
			Labels:      LabelsFromInheritedMetadata(cfg.InheritedMetadata),
		},
		PrimaryUpdateStrategy: "unsupervised",
		PrimaryUpdateMethod:   "switchover",
		ExternalClusters:      ExternalClusters(cfg),
		SmartShutdownTimeout:  smartShutdownTimeout(cfg.SmartShutdownTimeout),
	}

	return spec
}

// BarmanPluginParameters builds the parameter map for the barman-cloud plugin.
func BarmanPluginParameters(branchName, serverName string) map[string]string {
	params := map[string]string{
		"barmanObjectName": branchName,
	}
	if serverName != "" && serverName != branchName {
		params["serverName"] = serverName
	}
	return params
}

// VolumeSnapshotBootstrapRecovery creates a BootstrapRecovery using the provided
// branch, cluster and target cluster names.
func VolumeSnapshotBootstrapRecovery(branchName, targetBranchName string) *apiv1.BootstrapRecovery {
	return &apiv1.BootstrapRecovery{
		Owner:    "xata",
		Database: "xata",
		Secret: &apiv1.LocalObjectReference{
			Name: branchName + "-app",
		},
		VolumeSnapshots: &apiv1.DataSource{
			Storage: corev1.TypedLocalObjectReference{
				Name:     targetBranchName + "-" + branchName,
				Kind:     "VolumeSnapshot",
				APIGroup: new("snapshot.storage.k8s.io"),
			},
		},
		RecoveryTarget: &apiv1.RecoveryTarget{
			TargetImmediate: new(true),
		},
	}
}

// ObjectStoreBootstrapRecovery creates a BootstrapRecovery configuration for
// point-in-time recovery from object storage.
// The restoreSpec contains the source cluster name and optional timestamp.
// This function always returns a non-nil recovery config (matching VolumeSnapshotBootstrapRecovery).
// If barman parameters are missing, CNPG will fail during bootstrap with a proper error.
func ObjectStoreBootstrapRecovery(branchName string, restoreSpec *v1alpha1.RestoreSpec) *apiv1.BootstrapRecovery {
	recovery := &apiv1.BootstrapRecovery{
		Owner:    "xata",
		Database: "xata",
		Secret: &apiv1.LocalObjectReference{
			Name: branchName + "-app",
		},
		Source: restoreSpec.Name,
	}

	// Set recovery target based on timestamp
	if restoreSpec.Timestamp != nil {
		recovery.RecoveryTarget = &apiv1.RecoveryTarget{
			TargetTime: restoreSpec.Timestamp.Format("2006-01-02 15:04:05.000000"),
		}
	}

	return recovery
}

// ExternalClusters creates the ExternalClusters configuration for
// point-in-time recovery from object storage using barman-cloud plugin.
// Returns nil if the restore spec is not for ObjectStore type.
func ExternalClusters(cfg ClusterConfig) []apiv1.ExternalCluster {
	if !cfg.RestoreSpec.IsObjectStoreType() {
		return nil
	}

	pluginParams := map[string]string{
		"barmanObjectName": cfg.RestoreSpec.Name,
		"serverName":       cfg.RestoreSpec.GetServerName(),
	}

	return []apiv1.ExternalCluster{
		{
			Name: cfg.RestoreSpec.Name,
			PluginConfiguration: &apiv1.PluginConfiguration{
				Name:       "barman-cloud.cloudnative-pg.io",
				Parameters: pluginParams,
			},
		},
	}
}

// BootstrapInitDB returns a BootstrapInitDB configuration with version-specific SQL
func BootstrapInitDB(branchName string, majorVersion int) *apiv1.BootstrapInitDB {
	return &apiv1.BootstrapInitDB{
		Database:      "xata",
		Encoding:      "UTF8",
		LocaleCType:   "C",
		LocaleCollate: "C",
		Owner:         "xata",
		Secret: &apiv1.LocalObjectReference{
			Name: branchName + "-app",
		},
		PostInitSQL: GeneratePostInitSQL(majorVersion),
	}
}

// EnablePDB returns true if instances > 1, false otherwise. With an instance
// count of 1 and a PDB enabled, Kubernetes will be unable to drain nodes for
// maintenance. Therefore, we disable it for single instance clusters.
func EnablePDB(instances int) *bool {
	if instances > 1 {
		return new(true)
	}
	return new(false)
}

// PostgresParametersToMap converts a list of PostgresParameter to a map
func PostgresParametersToMap(cfg *v1alpha1.PostgresConfiguration) map[string]string {
	if cfg == nil {
		return nil
	}

	params := cfg.Parameters
	result := make(map[string]string, len(params))
	for _, param := range params {
		result[param.Name] = param.Value
	}
	return result
}

// DefaultSmartShutdownTimeout is set to 1 second, overriding the CNPG default of
// 180 seconds. The main reason is that pool-hibernation happens quickly.
const DefaultSmartShutdownTimeout int32 = 1

// smartShutdownTimeout returns the provided timeout or the default (1s)
func smartShutdownTimeout(t *int32) *int32 {
	if t != nil {
		return t
	}
	v := DefaultSmartShutdownTimeout
	return &v
}

// mountPropagationMode converts a *string to *corev1.MountPropagationMode
func mountPropagationMode(s *string) *corev1.MountPropagationMode {
	if s == nil {
		return nil
	}
	mode := corev1.MountPropagationMode(*s)
	return &mode
}

// LabelsFromInheritedMetadata extracts labels from InheritedMetadata, handling nil
func LabelsFromInheritedMetadata(m *v1alpha1.InheritedMetadata) map[string]string {
	if m == nil {
		return nil
	}
	return m.Labels
}

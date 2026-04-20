package resources_test

import (
	"slices"

	"xata/services/branch-operator/pkg/reconciler/resources"

	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"

	"xata/internal/postgresversions"
)

// ClusterSpecBuilder constructs CNPG Cluster specs using the Builder pattern
type ClusterSpecBuilder struct {
	ClusterSpec apiv1.ClusterSpec
	branchName  string
}

// NewClusterSpecBuilder initializes a new ClusterSpecBuilder with default
// values.
func NewClusterSpecBuilder() *ClusterSpecBuilder {
	defaultImage := "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7"
	branchName := "test-branch"
	majorVersion := postgresversions.ExtractMajorVersionFromImage(defaultImage)

	spec := apiv1.ClusterSpec{
		Instances: 1,
		EnablePDB: new(false),
		StorageConfiguration: apiv1.StorageConfiguration{
			Size: "10Gi",
		},
		ImageName:             defaultImage,
		ImagePullSecrets:      nil,
		EnableSuperuserAccess: new(true),
		SuperuserSecret: &apiv1.LocalObjectReference{
			Name: branchName + "-superuser",
		},
		Bootstrap: &apiv1.BootstrapConfiguration{
			InitDB: resources.BootstrapInitDB(branchName, majorVersion),
		},
		Plugins: []apiv1.PluginConfiguration{
			{
				Name: "cnpg-i-scale-to-zero.xata.io",
			},
			{
				Name:          "barman-cloud.cloudnative-pg.io",
				Enabled:       new(false),
				IsWALArchiver: new(true),
				Parameters: map[string]string{
					"barmanObjectName": branchName,
				},
			},
		},
		Resources: corev1.ResourceRequirements{},
		Backup: &apiv1.BackupConfiguration{
			VolumeSnapshot: &apiv1.VolumeSnapshotConfiguration{
				ClassName: "snapshot-class",
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
		SmartShutdownTimeout: new(resources.DefaultSmartShutdownTimeout),
		InheritedMetadata: &apiv1.EmbeddedObjectMetadata{
			Annotations: resources.InheritedAnnotations,
			Labels:      nil,
		},
		PrimaryUpdateStrategy: "unsupervised",
		PrimaryUpdateMethod:   "switchover",
	}

	return &ClusterSpecBuilder{
		ClusterSpec: spec,
		branchName:  branchName,
	}
}

// WithPostgresImage sets the ImageName field for the ClusterSpec and updates
// the Bootstrap InitDB PostInitSQL to match the version in the image
func (b *ClusterSpecBuilder) WithPostgresImage(i string) *ClusterSpecBuilder {
	b.ClusterSpec.ImageName = i
	// Update Bootstrap InitDB to use the correct version from the image
	if b.ClusterSpec.Bootstrap != nil && b.ClusterSpec.Bootstrap.InitDB != nil {
		majorVersion := postgresversions.ExtractMajorVersionFromImage(i)
		b.ClusterSpec.Bootstrap.InitDB = resources.BootstrapInitDB(b.branchName, majorVersion)
	}
	return b
}

// WithStorageClass sets the StorageClass field for the ClusterSpec
func (b *ClusterSpecBuilder) WithStorageClass(s *string) *ClusterSpecBuilder {
	b.ClusterSpec.StorageConfiguration.StorageClass = s
	return b
}

// WithVolumeSnapshotClass sets the VolumeSnapshotClass field for the ClusterSpec
func (b *ClusterSpecBuilder) WithVolumeSnapshotClass(s string) *ClusterSpecBuilder {
	b.ClusterSpec.Backup.VolumeSnapshot.ClassName = s
	return b
}

// WithStorageSize sets the StorageSize field for the ClusterSpec
func (b *ClusterSpecBuilder) WithStorageSize(s string) *ClusterSpecBuilder {
	b.ClusterSpec.StorageConfiguration.Size = s
	return b
}

// WithResources sets the Resources field for the ClusterSpec
func (b *ClusterSpecBuilder) WithResources(r corev1.ResourceRequirements) *ClusterSpecBuilder {
	b.ClusterSpec.Resources = r
	return b
}

// WithEnablePDB sets the EnablePDB field for the ClusterSpec
func (b *ClusterSpecBuilder) WithEnablePDB(enabled bool) *ClusterSpecBuilder {
	b.ClusterSpec.EnablePDB = new(enabled)
	return b
}

// WithInstances sets the number of instances for the ClusterSpec
func (b *ClusterSpecBuilder) WithInstances(i int) *ClusterSpecBuilder {
	b.ClusterSpec.Instances = i
	return b
}

// WithAffinity sets the Affinity configuration for the ClusterSpec
func (b *ClusterSpecBuilder) WithAffinity(a apiv1.AffinityConfiguration) *ClusterSpecBuilder {
	b.ClusterSpec.Affinity = a
	return b
}

// WithPostgresParameters sets the PostgreSQL parameters for the ClusterSpec
func (b *ClusterSpecBuilder) WithPostgresParameters(params map[string]string) *ClusterSpecBuilder {
	b.ClusterSpec.PostgresConfiguration.Parameters = params
	return b
}

// WithSharedPreloadLibraries sets the shared preload libraries for the ClusterSpec
func (b *ClusterSpecBuilder) WithSharedPreloadLibraries(libs []string) *ClusterSpecBuilder {
	b.ClusterSpec.PostgresConfiguration.AdditionalLibraries = libs
	return b
}

// WithLabels sets the labels in InheritedMetadata for the ClusterSpec
func (b *ClusterSpecBuilder) WithLabels(labels map[string]string) *ClusterSpecBuilder {
	if b.ClusterSpec.InheritedMetadata == nil {
		b.ClusterSpec.InheritedMetadata = &apiv1.EmbeddedObjectMetadata{}
	}
	b.ClusterSpec.InheritedMetadata.Labels = labels
	return b
}

// WithBarmanPluginEnabled enables or disables the Barman Cloud plugin for the ClusterSpec
func (b *ClusterSpecBuilder) WithBarmanPluginEnabled(enabled bool) *ClusterSpecBuilder {
	idx := slices.IndexFunc(b.ClusterSpec.Plugins, func(p apiv1.PluginConfiguration) bool {
		return p.Name == "barman-cloud.cloudnative-pg.io"
	})

	if idx != -1 {
		b.ClusterSpec.Plugins[idx].Enabled = new(enabled)
	}
	return b
}

func (b *ClusterSpecBuilder) WithBarmanServerName(name string) *ClusterSpecBuilder {
	idx := slices.IndexFunc(b.ClusterSpec.Plugins, func(p apiv1.PluginConfiguration) bool {
		return p.Name == "barman-cloud.cloudnative-pg.io"
	})

	if idx != -1 {
		b.ClusterSpec.Plugins[idx].Parameters["serverName"] = name
	}
	return b
}

// WithNodeSelector sets the node selector in Affinity configuration for the ClusterSpec
func (b *ClusterSpecBuilder) WithNodeSelector(nodeSelector map[string]string) *ClusterSpecBuilder {
	b.ClusterSpec.Affinity.NodeSelector = nodeSelector
	return b
}

// WithTolerations sets the tolerations in Affinity configuration for the ClusterSpec
func (b *ClusterSpecBuilder) WithTolerations(tolerations []corev1.Toleration) *ClusterSpecBuilder {
	b.ClusterSpec.Affinity.Tolerations = tolerations
	return b
}

// WithImagePullSecrets sets the ImagePullSecrets for the ClusterSpec
func (b *ClusterSpecBuilder) WithImagePullSecrets(secrets []apiv1.LocalObjectReference) *ClusterSpecBuilder {
	b.ClusterSpec.ImagePullSecrets = secrets
	return b
}

// WithBootstrapConfig sets the Bootstrap configuration for the ClusterSpec
func (b *ClusterSpecBuilder) WithBootstrapConfig(c *apiv1.BootstrapConfiguration) *ClusterSpecBuilder {
	b.ClusterSpec.Bootstrap = c
	return b
}

// WithExternalClusters sets the ExternalClusters configuration for the ClusterSpec
func (b *ClusterSpecBuilder) WithExternalClusters(ec []apiv1.ExternalCluster) *ClusterSpecBuilder {
	b.ClusterSpec.ExternalClusters = ec
	return b
}

// WithSmartShutdownTimeout sets the SmartShutdownTimeout for the ClusterSpec
func (b *ClusterSpecBuilder) WithSmartShutdownTimeout(t int32) *ClusterSpecBuilder {
	b.ClusterSpec.SmartShutdownTimeout = &t
	return b
}

// Build returns the constructed ClusterSpec
func (b *ClusterSpecBuilder) Build() apiv1.ClusterSpec {
	return b.ClusterSpec
}

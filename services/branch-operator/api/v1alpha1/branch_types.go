package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BranchSpec defines the desired state of a Branch
// +kubebuilder:validation:XValidation:rule="has(oldSelf.restore) == has(self.restore)",message="restore is immutable"
type BranchSpec struct {
	// Restore specifies how to restore the branch from a backup source.
	// +kubebuilder:validation:Optional
	Restore *RestoreSpec `json:"restore,omitempty"`

	// ClusterSpec defines the CNPG cluster configuration for the branch
	// +kubebuilder:validation:Required
	ClusterSpec ClusterSpec `json:"cluster"`

	// BackupSpec configures backups for the branch. When set, an
	// ObjectStore resource is created to manage backup storage and retention.
	// When nil, no ObjectStore is created.
	// +optional
	BackupSpec *BackupSpec `json:"backup,omitempty"`

	// Pooler configures a PgBouncer connection pooler for the branch.
	// When nil, no pooler is created.
	// +optional
	Pooler *PoolerSpec `json:"pooler,omitempty"`

	// InheritedMetadata defines metadata to be inherited by all resources
	// created by the operator
	// +optional
	InheritedMetadata *InheritedMetadata `json:"inheritedMetadata,omitempty"`
}

// RestoreSpec defines how to restore the branch from a backup source
type RestoreSpec struct {
	// Type specifies the type of restore source
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.type is immutable"
	// +kubebuilder:validation:Enum=VolumeSnapshot;ObjectStore;BaseBackup;XVolClone
	Type RestoreType `json:"type"`

	// Name is the name of the restore source (for volume snapshot, object store, base backup or XVol clone)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.name is immutable"
	// +kubebuilder:validation:MaxLength=256
	Name string `json:"name"`

	// Timestamp specifies the point-in-time to restore to.
	// Only applicable when Type is ObjectStore.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.timestamp is immutable"
	Timestamp *metav1.Time `json:"timestamp,omitempty"`

	// ServerName overrides the barman serverName parameter in the external
	// cluster plugin configuration. Only applicable when Type is ObjectStore.
	// When empty, defaults to the restore name.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.serverName is immutable"
	ServerName string `json:"serverName,omitempty"`
}

// RestoreType defines the type of restore source
type RestoreType string

const (
	// RestoreTypeVolumeSnapshot restores from a Kubernetes volume snapshot
	RestoreTypeVolumeSnapshot RestoreType = "VolumeSnapshot"
	// RestoreTypeXVolClone restores from a clone of an existing XVol
	RestoreTypeXVolClone RestoreType = "XVolClone"
	// RestoreTypeObjectStore restores from object storage (supports PITR with timestamp)
	RestoreTypeObjectStore RestoreType = "ObjectStore"
	// RestoreTypeBaseBackup restores from a base backup
	RestoreTypeBaseBackup RestoreType = "BaseBackup"
)

func (r *RestoreSpec) IsVolumeSnapshotType() bool {
	return r != nil && r.Type == RestoreTypeVolumeSnapshot
}

func (r *RestoreSpec) IsObjectStoreType() bool {
	return r != nil && r.Type == RestoreTypeObjectStore
}

func (r *RestoreSpec) GetServerName() string {
	if r == nil || r.ServerName == "" {
		return r.Name
	}
	return r.ServerName
}

// ClusterSpec defines the PostgreSQL cluster configuration
type ClusterSpec struct {
	// Name is the name of the CNPG Cluster resource.
	// If nil, the branch has no cluster associated with it.
	// +optional
	Name *string `json:"name,omitempty"`

	// Image is the PostgreSQL container image to use
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Instances is the number of PostgreSQL instances in the cluster
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	// +optional
	// +kubebuilder:default:=1
	Instances int32 `json:"instances,omitempty"`

	// Resources defines the compute resources for the PostgreSQL instances
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage defines the persistent volume configuration for each instance
	// +kubebuilder:default:={size:"1Gi"}
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Postgres defines PostgreSQL-specific configuration
	// +kubebuilder:validation:Optional
	Postgres *PostgresConfiguration `json:"postgres,omitempty"`

	// ScaleToZero defines scale-to-zero configuration for the cluster
	// +optional
	ScaleToZero *ScaleToZeroConfiguration `json:"scaleToZero,omitempty"`

	// Hibernation specifies whether the cluster should be hibernated
	// +optional
	// +kubebuilder:validation:Enum=Enabled;Disabled
	Hibernation *HibernationMode `json:"hibernation,omitempty"`

	// Affinity defines scheduling constraints for the cluster pods
	// +optional
	Affinity *AffinitySpec `json:"affinity,omitempty"`

	// SmartShutdownTimeout is the time in seconds reserved for the smart
	// shutdown of Postgres to complete. If not set, the CNPG default (180s)
	// is used.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SmartShutdownTimeout *int32 `json:"smartShutdownTimeout,omitempty"`
}

// ClusterName returns the CNPG Cluster name for this branch. If
// Spec.ClusterSpec.Name is nil, the branch has no associated cluster and an
// empty string is returned.
func (b *Branch) ClusterName() string {
	if b.Spec.ClusterSpec.Name == nil {
		return ""
	}
	return *b.Spec.ClusterSpec.Name
}

// HasClusterName returns true iff the branch has a CNPG Cluster name specified
func (b *Branch) HasClusterName() bool {
	return b.Spec.ClusterSpec.Name != nil
}

// AffinitySpec defines pod scheduling constraints
type AffinitySpec struct {
	// NodeSelector specifies node labels that pods must match to be scheduled
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

func (a *AffinitySpec) GetNodeSelector() map[string]string {
	if a == nil {
		return nil
	}
	return a.NodeSelector
}

// StorageSpec defines storage configuration for the cluster
type StorageSpec struct {
	// Size is the size of the persistent volume for each instance
	// +kubebuilder:validation:Pattern=`^\d+(\.\d+)?(Gi|Ti)$`
	// +kubebuilder:default:="1Gi"
	// +optional
	Size string `json:"size,omitempty"`

	// StorageClass is the Kubernetes storage class for the cluster PVCs
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`

	// VolumeSnapshotClass is the Kubernetes volume snapshot class for snapshots
	// +optional
	VolumeSnapshotClass *string `json:"volumeSnapshotClass,omitempty"`

	// MountPropagation sets the mount propagation mode for the storage volume.
	// Valid values are "None", "HostToContainer", and "Bidirectional".
	// +optional
	// +kubebuilder:validation:Enum=None;HostToContainer;Bidirectional
	MountPropagation *string `json:"mountPropagation,omitempty"`
}

func (s *StorageSpec) GetVolumeSnapshotClass() string {
	if s.VolumeSnapshotClass == nil {
		return ""
	}
	return *s.VolumeSnapshotClass
}

// PostgresConfiguration defines PostgreSQL-specific settings
type PostgresConfiguration struct {
	// Parameters are PostgreSQL configuration parameters
	// +optional
	// +listType=map
	// +listMapKey=name
	Parameters []PostgresParameter `json:"parameters,omitempty"`

	// SharedPreloadLibraries are PostgreSQL shared preload libraries
	// +optional
	// +listType=set
	SharedPreloadLibraries []string `json:"sharedPreloadLibraries,omitempty"`
}

func (p *PostgresConfiguration) GetSharedPreloadLibraries() []string {
	if p == nil {
		return nil
	}
	return p.SharedPreloadLibraries
}

// PostgresParameter defines a PostgreSQL configuration parameter
type PostgresParameter struct {
	// Name is the parameter name
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Value is the parameter value
	// +kubebuilder:validation:Required
	Value string `json:"value"`
}

// PoolerSpec configures a PgBouncer connection pooler for the branch.
type PoolerSpec struct {
	// Instances is the number of PgBouncer instances
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +optional
	// +kubebuilder:default:=1
	Instances int32 `json:"instances,omitempty"`

	// Mode is the PgBouncer pool mode
	// +kubebuilder:validation:Enum=session;transaction
	// +optional
	// +kubebuilder:default:="session"
	Mode PoolMode `json:"mode,omitempty"`

	// MaxClientConn is the maximum number of client connections to PgBouncer
	// +optional
	// +kubebuilder:default:="100"
	MaxClientConn string `json:"maxClientConn,omitempty"`

	// DefaultPoolSize overrides the PgBouncer default_pool_size parameter.
	// When empty, default_pool_size is derived as floor(0.9 * max_connections)
	// from the branch's Postgres configuration.
	// +optional
	// +kubebuilder:validation:Pattern="^[1-9][0-9]*$"
	DefaultPoolSize string `json:"defaultPoolSize,omitempty"`
}

// PoolMode is the PgBouncer pool mode
type PoolMode string

const (
	PoolModeSession     PoolMode = "session"
	PoolModeTransaction PoolMode = "transaction"
)

func (p *PoolerSpec) IsEnabled() bool {
	return p != nil
}

// InheritedMetadata defines metadata to be inherited by all resources created
// by the operator
type InheritedMetadata struct {
	// Labels are additional labels to apply to resources created by the operator
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

func (im *InheritedMetadata) GetLabels() map[string]string {
	if im == nil {
		return nil
	}
	return im.Labels
}

// ScaleToZeroConfiguration defines scale-to-zero settings
type ScaleToZeroConfiguration struct {
	// Enabled controls whether scale-to-zero is active.
	// When false the configuration (e.g. InactivityPeriodMinutes) is preserved
	// but scale-to-zero is disabled.
	Enabled bool `json:"enabled"`
	// InactivityPeriodMinutes is the period of inactivity before scaling to zero
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1440
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=30
	InactivityPeriodMinutes int32 `json:"inactivityPeriodMinutes,omitempty"`
}

// HibernationMode describes the hibernation state for a branch
type HibernationMode string

const (
	HibernationModeEnabled  HibernationMode = "Enabled"
	HibernationModeDisabled HibernationMode = "Disabled"

	WakeupPoolAnnotation = "xata.io/wakeup-pool"
)

// IsEnabled returns true if hibernation mode is enabled
func (hm *HibernationMode) IsEnabled() bool {
	return hm != nil && *hm == HibernationModeEnabled
}

// HasWakeupPoolAnnotation returns true if the Branch has the wakeup pool
// annotation
func (b *Branch) HasWakeupPoolAnnotation() bool {
	_, ok := b.Annotations[WakeupPoolAnnotation]
	return ok
}

// WakeupPoolName returns the name of the wakeup pool from the Branch's
// annotations. If the annotation is not present, an empty string is returned.
func (b *Branch) WakeupPoolName() string {
	return b.Annotations[WakeupPoolAnnotation]
}

type WALArchivingMode string

const (
	WALArchivingModeEnabled  WALArchivingMode = "Enabled"
	WALArchivingModeDisabled WALArchivingMode = "Disabled"
)

// BackupSpec defines backup settings for a Branch
type BackupSpec struct {
	// Retention specifies how long to retain backups
	// Examples: "60d", "4w", "2m"
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Pattern=`^\d+[dwm]$`
	// +kubebuilder:default:="2d"
	Retention string `json:"retention,omitempty"`

	// WALArchiving specifies whether WAL archiving is enabled
	// +kubebuilder:validation:Enum=Enabled;Disabled
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="Enabled"
	WALArchiving WALArchivingMode `json:"walArchiving,omitempty"`

	// ScheduledBackup configures periodic base backups.
	// +kubebuilder:validation:Optional
	ScheduledBackup *ScheduledBackupSpec `json:"scheduledBackup,omitempty"`

	// ServerName overrides the barman serverName plugin parameter.
	// When empty, serverName defaults to the branch name.
	// +kubebuilder:validation:Optional
	ServerName string `json:"serverName,omitempty"`
}

// ScheduledBackupSpec configures periodic base backups
type ScheduledBackupSpec struct {
	// Schedule in CNPG 6-field cron format.
	// The format is : "second minute hour day month dayofweek"
	// Examples: "0 0 0 * * *" (daily at midnight), "0 0 2 * * 0" (weekly on Sunday at 2am)
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="0 0 0 * * 0"
	Schedule string `json:"schedule,omitempty"`
}

func (b *BackupSpec) IsWALArchivingDisabled() bool {
	return b != nil && b.WALArchiving == WALArchivingModeDisabled
}

func (b *BackupSpec) IsScheduledBackupEnabled() bool {
	return b != nil && b.ScheduledBackup != nil
}

func (b *BackupSpec) RequiresBarmanPlugin() bool {
	return b != nil && (b.WALArchiving == WALArchivingModeEnabled || b.ScheduledBackup != nil)
}

// GetServerName returns the serverName for the Barman cloud plugin.
func (b *BackupSpec) GetServerName() string {
	if b == nil {
		return ""
	}
	return b.ServerName
}

// BranchStatus defines the observed state of a Branch
type BranchStatus struct {
	// ObservedGeneration reflects the generation of the most recently observed Branch spec
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the Branch's state
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastError contains the last error message encountered during
	// reconciliation, if any.
	// +optional
	LastError string `json:"lastError,omitempty"`

	// PrimaryXVolName is the name of the XVol backing the primary instance's
	// PersistentVolume. Retained after cluster removal.
	// +optional
	PrimaryXVolName string `json:"primaryXVolName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=br
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.cluster.name"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].message"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// Branch is the Schema for the branches API
type Branch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BranchSpec   `json:"spec,omitempty"`
	Status BranchStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// BranchList contains a list of Branch
type BranchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Branch `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Branch{}, &BranchList{})
}

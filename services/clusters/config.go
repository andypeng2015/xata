package clusters

import (
	"fmt"
	"slices"
)

type Config struct {
	KubeConfig                  string            `env:"KUBECONFIG" env-default:"" env-description:"path to the kube config file"`
	ClustersNamespace           string            `env:"XATA_CLUSTERS_NAMESPACE" env-default:"xata-clusters" env-description:"namespace for creating the clusters"`
	XataNamespace               string            `env:"XATA_NAMESPACE" env-default:"xata" env-description:"namespace for xata services"`
	ClustersNodeSelector        map[string]string `env:"XATA_CLUSTERS_NODE_SELECTOR" env-default:"" env-description:"node selector for the clusters"`
	ClustersStorageRequest      int32             `env:"XATA_CLUSTERS_STORAGE_REQUEST_GB" env-default:"250" env-description:"default storage size of the cluster in Gb"`
	ClustersStorageClass        string            `env:"XATA_CLUSTERS_STORAGE_CLASS" env-description:"storageclass to use for clusters"`
	ClustersVolumeSnapshotClass string            `env:"XATA_CLUSTERS_VOLUME_SNAPSHOT_CLASS" env-description:"volumesnapshotclass to use for clusters"`
	EnablePooler                bool              `env:"XATA_ENABLE_POOLER" env-default:"true" env-description:"enable PgBouncer connection pooler for new branches"`
	XVolStorageClasses          []string          `env:"XATA_XVOL_STORAGE_CLASSES" env-separator:"," env-default:"xatastor,xatastor-slot" env-description:"storage classes that use XVols"`
	XVolChildStorageClass       string            `env:"XATA_XVOL_CHILD_STORAGE_CLASS" env-default:"xatastor-slot" env-description:"storage class assigned to child branches whose parent uses an XVol-capable storage class"`
}

func (cfg *Config) Validate() error {
	if cfg.ClustersStorageClass == "" {
		return fmt.Errorf("storage class is required but not set")
	}
	if cfg.ClustersVolumeSnapshotClass == "" {
		return fmt.Errorf("volume snapshot class is required but not set")
	}
	if cfg.XVolChildStorageClass != "" && !slices.Contains(cfg.XVolStorageClasses, cfg.XVolChildStorageClass) {
		return fmt.Errorf("xvol child storage class %q must be listed in xvol storage classes", cfg.XVolChildStorageClass)
	}
	return nil
}

package clusters

import "fmt"

type Config struct {
	KubeConfig                  string            `env:"KUBECONFIG" env-default:"" env-description:"path to the kube config file"`
	ClustersNamespace           string            `env:"XATA_CLUSTERS_NAMESPACE" env-default:"xata-clusters" env-description:"namespace for creating the clusters"`
	XataNamespace               string            `env:"XATA_NAMESPACE" env-default:"xata" env-description:"namespace for xata services"`
	ClustersNodeSelector        map[string]string `env:"XATA_CLUSTERS_NODE_SELECTOR" env-default:"" env-description:"node selector for the clusters"`
	ClustersStorageRequest      int32             `env:"XATA_CLUSTERS_STORAGE_REQUEST_GB" env-default:"250" env-description:"default storage size of the cluster in Gb"`
	ClustersStorageClass        string            `env:"XATA_CLUSTERS_STORAGE_CLASS" env-description:"storageclass to use for clusters"`
	ClustersVolumeSnapshotClass string            `env:"XATA_CLUSTERS_VOLUME_SNAPSHOT_CLASS" env-description:"volumesnapshotclass to use for clusters"`
	EnablePooler                bool              `env:"XATA_ENABLE_POOLER" env-default:"true" env-description:"enable PgBouncer connection pooler for new branches"`
}

func (cfg *Config) Validate() error {
	if cfg.ClustersStorageClass == "" {
		return fmt.Errorf("storage class is required but not set")
	}
	if cfg.ClustersVolumeSnapshotClass == "" {
		return fmt.Errorf("volume snapshot class is required but not set")
	}
	return nil
}

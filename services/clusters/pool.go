package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
)

// findPoolCluster finds a healthy, pre-provisioned cluster from a ClusterPool
// that matches the requested configuration (storage class, Postgres major
// version, CPU, and memory).
func findPoolCluster(ctx context.Context, kubeClient client.Client, clusterReader client.Reader, namespace, storageClass, image, cpuRequest, memory string) (string, *apiv1.Cluster, error) {
	var pools cpv1alpha1.ClusterPoolList
	if err := kubeClient.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return "", nil, fmt.Errorf("list cluster pools: %w", err)
	}

	requestedMajor := extractPostgresMajor(image)
	requestedCPU := resource.MustParse(cpuRequest)
	requestedMemory := resource.MustParse(memory)

	for i := range pools.Items {
		pool := &pools.Items[i]
		spec := &pool.Spec.ClusterSpec

		poolSC := ""
		if spec.StorageConfiguration.StorageClass != nil {
			poolSC = *spec.StorageConfiguration.StorageClass
		}
		if poolSC != storageClass {
			continue
		}

		if extractPostgresMajor(spec.ImageName) != requestedMajor {
			continue
		}

		poolCPU := spec.Resources.Requests[corev1.ResourceCPU]
		poolMemory := spec.Resources.Requests[corev1.ResourceMemory]
		if !poolCPU.Equal(requestedCPU) || !poolMemory.Equal(requestedMemory) {
			continue
		}

		cluster, err := findAvailableClusterInPool(ctx, clusterReader, namespace, pool)
		if err != nil {
			return "", nil, fmt.Errorf("find available cluster in pool %s: %w", pool.Name, err)
		}
		if cluster != nil {
			return pool.Name, cluster, nil
		}
	}

	return "", nil, nil
}

// wakeupPoolName derives the wakeup pool name from a pool name. Each create
// pool (used for new branches) has a companion wakeup pool (used when waking
// hibernated branches). A name convention is used to match the create pool
// with its wakeup pool. For example `pg18-tiny-create` is matched with
// `pg18-tiny-wakeup`. If the input already has a `-wakeup` suffix it is
// returned unchanged.
func wakeupPoolName(poolName string) string {
	return strings.TrimSuffix(strings.TrimSuffix(poolName, "-create"), "-wakeup") + "-wakeup"
}

var (
	poolClusterWaitTimeout  = 15 * time.Second
	poolClusterPollInterval = 1 * time.Second
)

// findAvailableClusterInPool polls for a healthy cluster in the given pool,
// waiting up to poolClusterWaitTimeout. A short wait is used because the pool
// operator continuously replenishes clusters, so one may become ready shortly.
func findAvailableClusterInPool(ctx context.Context, clusterReader client.Reader, namespace string, pool *cpv1alpha1.ClusterPool) (*apiv1.Cluster, error) {
	deadline := time.Now().Add(poolClusterWaitTimeout)
	for {
		cluster, err := findHealthyClusterInPool(ctx, clusterReader, namespace, pool)
		if err != nil {
			return nil, err
		}
		if cluster != nil {
			return cluster, nil
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poolClusterPollInterval):
		}
	}
}

// findHealthyClusterInPool returns the first cluster owned by the pool that is
// in the Healthy phase and not being deleted.
func findHealthyClusterInPool(ctx context.Context, clusterReader client.Reader, namespace string, pool *cpv1alpha1.ClusterPool) (*apiv1.Cluster, error) {
	var clusters apiv1.ClusterList
	if err := clusterReader.List(ctx, &clusters,
		client.InNamespace(namespace),
		client.MatchingFields{clusterOwnerKey: pool.Name},
	); err != nil {
		return nil, fmt.Errorf("list clusters: %w", err)
	}

	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.DeletionTimestamp != nil {
			continue
		}
		if cluster.Status.ReadyInstances > 0 {
			return cluster, nil
		}
	}

	return nil, nil
}

func extractPostgresMajor(imageName string) string {
	parts := strings.SplitN(imageName, ":", 2)
	if len(parts) < 2 {
		return ""
	}
	tag := parts[1]
	dotIdx := strings.Index(tag, ".")
	if dotIdx < 0 {
		return tag
	}
	return tag[:dotIdx]
}

// orphanCluster removes the cluster from its pool by clearing its
// ownerReferences. This prevents the pool operator from managing or deleting
// the cluster once it has been assigned to a branch.
func orphanCluster(ctx context.Context, kubeClient client.Client, cluster *apiv1.Cluster) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"ownerReferences": nil,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	if err := kubeClient.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("patch cluster %s: %w", cluster.Name, err)
	}

	return nil
}

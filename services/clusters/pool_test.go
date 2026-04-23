package clusters

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
)

func TestExtractPostgresMajor(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		image string
		want  string
	}{
		"standard image": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			want:  "17",
		},
		"major version 18": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.3",
			want:  "18",
		},
		"major version 16": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
			want:  "16",
		},
		"no tag": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus",
			want:  "",
		},
		"tag without dot": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:latest",
			want:  "latest",
		},
		"empty string": {
			image: "",
			want:  "",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := extractPostgresMajor(tt.image)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFindPoolCluster(t *testing.T) {
	t.Parallel()

	origTimeout := poolClusterWaitTimeout
	origInterval := poolClusterPollInterval
	poolClusterWaitTimeout = 100 * time.Millisecond
	poolClusterPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		poolClusterWaitTimeout = origTimeout
		poolClusterPollInterval = origInterval
	})

	const namespace = "xata-clusters"
	poolUID := types.UID("pool-uid-123")

	matchingPool := &cpv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pool",
			Namespace: namespace,
			UID:       poolUID,
		},
		Spec: cpv1alpha1.ClusterPoolSpec{
			Clusters: 2,
			ClusterSpec: apiv1.ClusterSpec{
				ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
				StorageConfiguration: apiv1.StorageConfiguration{
					StorageClass: new("default-storage-class"),
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			},
		},
	}

	availableCluster := &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-cluster-1",
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: cpv1alpha1.GroupVersion.String(),
					Kind:       cpv1alpha1.ClusterPoolKind,
					UID:        poolUID,
					Name:       "test-pool",
					Controller: ptr.To(true),
				},
			},
		},
		Status: apiv1.ClusterStatus{
			Phase:          apiv1.PhaseHealthy,
			ReadyInstances: 1,
		},
	}

	tests := map[string]struct {
		objects      []client.Object
		storageClass string
		image        string
		cpuRequest   string
		memory       string
		wantPoolName string
		wantName     string
	}{
		"matching pool with available cluster": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
			wantPoolName: "test-pool",
			wantName:     "pool-cluster-1",
		},
		"no matching pool - different storage class": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "other-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
		},
		"no matching pool - different postgres major": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
			cpuRequest:   "2",
			memory:       "4Gi",
		},
		"no matching pool - different cpu": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "4",
			memory:       "4Gi",
		},
		"no matching pool - different memory": {
			objects:      []client.Object{matchingPool, availableCluster},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "8Gi",
		},
		"matching pool but no available clusters": {
			objects:      []client.Object{matchingPool},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
		},
		"matching pool with unhealthy cluster": {
			objects: []client.Object{matchingPool, &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pool-cluster-unhealthy",
					Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{
						{APIVersion: cpv1alpha1.GroupVersion.String(), Kind: cpv1alpha1.ClusterPoolKind, UID: poolUID, Name: "test-pool", Controller: ptr.To(true)},
					},
				},
				Status: apiv1.ClusterStatus{
					Phase: apiv1.PhaseWaitingForInstancesToBeActive,
				},
			}},
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
		},
		"no pools at all": {
			objects:      nil,
			storageClass: "default-storage-class",
			image:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.2",
			cpuRequest:   "2",
			memory:       "4Gi",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			k8sClient := newPoolTestClient(t, tt.objects...)

			gotPoolName, got, err := findPoolCluster(ctx, k8sClient, k8sClient, namespace,
				tt.storageClass, tt.image, tt.cpuRequest, tt.memory)
			require.NoError(t, err)

			if tt.wantName == "" {
				require.Nil(t, got)
				require.Empty(t, gotPoolName)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tt.wantName, got.Name)
				require.Equal(t, tt.wantPoolName, gotPoolName)
			}
		})
	}
}

func TestOrphanCluster(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-cluster-1",
			Namespace: "xata-clusters",
			OwnerReferences: []metav1.OwnerReference{
				{
					UID:  types.UID("pool-uid"),
					Name: "test-pool",
				},
			},
		},
	}

	k8sClient := newPoolTestClient(t, cluster)

	err := orphanCluster(ctx, k8sClient, cluster)
	require.NoError(t, err)

	updated := &apiv1.Cluster{}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), updated)
	require.NoError(t, err)
	require.Empty(t, updated.OwnerReferences)
}

func TestWakeupPoolName(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		input string
		want  string
	}{
		"standard create pool": {
			input: "pg18-tiny-create",
			want:  "pg18-tiny-wakeup",
		},
		"already wakeup pool": {
			input: "pg18-tiny-wakeup",
			want:  "pg18-tiny-wakeup",
		},
		"no create suffix": {
			input: "pg18-tiny",
			want:  "pg18-tiny-wakeup",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := wakeupPoolName(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFindHealthyClusterInPool(t *testing.T) {
	t.Parallel()

	const namespace = "xata-clusters"
	poolUID := types.UID("pool-uid-123")

	pool := &cpv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pool",
			Namespace: namespace,
			UID:       poolUID,
		},
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: cpv1alpha1.GroupVersion.String(),
		Kind:       cpv1alpha1.ClusterPoolKind,
		UID:        poolUID,
		Name:       "test-pool",
		Controller: ptr.To(true),
	}

	now := metav1.Now()

	tests := map[string]struct {
		objects  []client.Object
		wantName string
	}{
		"returns healthy cluster": {
			objects: []client.Object{pool, &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "healthy-1", Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{ownerRef},
				},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					ReadyInstances: 1,
				},
			}},
			wantName: "healthy-1",
		},
		"skips cluster being deleted": {
			objects: []client.Object{pool, &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "deleting-1", Namespace: namespace,
					OwnerReferences:   []metav1.OwnerReference{ownerRef},
					DeletionTimestamp: &now,
					Finalizers:        []string{"test-finalizer"},
				},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					ReadyInstances: 1,
				},
			}},
		},
		"skips unhealthy cluster": {
			objects: []client.Object{pool, &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "unhealthy-1", Namespace: namespace,
					OwnerReferences: []metav1.OwnerReference{ownerRef},
				},
				Status: apiv1.ClusterStatus{Phase: apiv1.PhaseWaitingForInstancesToBeActive},
			}},
		},
		"picks first healthy, skips deleting": {
			objects: []client.Object{
				pool,
				&apiv1.Cluster{
					ObjectMeta: metav1.ObjectMeta{
						Name: "deleting-1", Namespace: namespace,
						OwnerReferences:   []metav1.OwnerReference{ownerRef},
						DeletionTimestamp: &now,
						Finalizers:        []string{"test-finalizer"},
					},
					Status: apiv1.ClusterStatus{
						Phase:          apiv1.PhaseHealthy,
						ReadyInstances: 1,
					},
				},
				&apiv1.Cluster{
					ObjectMeta: metav1.ObjectMeta{
						Name: "healthy-2", Namespace: namespace,
						OwnerReferences: []metav1.OwnerReference{ownerRef},
					},
					Status: apiv1.ClusterStatus{
						Phase:          apiv1.PhaseHealthy,
						ReadyInstances: 1,
					},
				},
			},
			wantName: "healthy-2",
		},
		"no clusters at all": {
			objects: []client.Object{pool},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			k8sClient := newPoolTestClient(t, tt.objects...)

			got, err := findHealthyClusterInPool(ctx, k8sClient, namespace, pool)
			require.NoError(t, err)

			if tt.wantName == "" {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tt.wantName, got.Name)
			}
		})
	}
}

func TestWaitForClusterCache(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when cache is ready", func(t *testing.T) {
		ready := make(chan struct{})
		close(ready)
		svc := &ClustersService{clusterCacheOk: ready}
		require.NoError(t, svc.waitForClusterCache(context.Background()))
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		svc := &ClustersService{clusterCacheOk: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, svc.waitForClusterCache(ctx), context.Canceled)
	})
}

func newPoolTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, cpv1alpha1.AddToScheme(scheme))

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&apiv1.Cluster{}, clusterOwnerKey, func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != cpv1alpha1.ClusterPoolKind {
				return nil
			}
			return []string{owner.Name}
		}).
		Build()
}

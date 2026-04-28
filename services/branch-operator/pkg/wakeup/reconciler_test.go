package wakeup_test

import (
	"context"
	"testing"

	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestWakeupReconciler(t *testing.T) {
	t.Parallel()

	t.Run("claims a healthy cluster from the pool and assigns it to the branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a healthy CNPG Cluster with 1 ready instance, owned by the pool
		cluster, err := setupPoolCluster(ctx, pool, clusterName, TestNamespace, 1)
		require.NoError(t, err)

		// Create a Branch with pool annotation and no cluster name
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect the WakeupRequest to complete successfully
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)

		// Expect the Branch to have a cluster name assigned
		requireEventuallyTrue(t, func() bool {
			br := &v1alpha1.Branch{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(branch), br)
			if err != nil {
				return false
			}

			return br.Spec.ClusterSpec.Name != nil
		})

		// Expect the Cluster to no longer have the pool's controller owner
		// reference
		requireEventuallyTrue(t, func() bool {
			c := &apiv1.Cluster{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), c); err != nil {
				return false
			}

			return metav1.GetControllerOf(c) == nil
		})
	})

	t.Run("deletes completed WakeupRequest after TTL expires", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a healthy CNPG Cluster withe 1 ready instance, owned by the pool
		_, err := setupPoolCluster(ctx, pool, clusterName, TestNamespace, 1)
		require.NoError(t, err)

		// Create a Branch with pool annotation and no cluster name
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Wait for the WakeupRequest to complete
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)

		// Expect the WakeupRequest to be deleted after the TTL (1s in tests)
		requireEventuallyTrue(t, func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(wr), &v1alpha1.WakeupRequest{})
			return apierrors.IsNotFound(err)
		})
	})

	t.Run("sets PoolExhausted when no healthy clusters are available", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a CNPG Cluster with 0 ready instances, owned by the pool
		_, err := setupPoolCluster(ctx, pool, clusterName, TestNamespace, 0)
		require.NoError(t, err)

		// Create a Branch with pool annotation and no cluster name
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect the WakeupRequest to have Succeeded=Unknown with PoolExhausted
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionUnknown, v1alpha1.PoolExhaustedReason)
	})

	t.Run("sets BranchNotFound when the branch does not exist", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		wrName := "wur-" + randomString(10)
		branchName := "nonexistent-" + randomString(10)

		// Create a WakeupRequest referencing a non-existent branch
		wr, err := createWakeupRequest(ctx, wrName, branchName)
		require.NoError(t, err)

		// Expect the WakeupRequest to have Succeeded=Unknown with BranchNotFound
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionUnknown, v1alpha1.BranchNotFoundReason)
	})

	t.Run("sets PoolNotFound when the pool does not exist", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)
		poolName := "nonexistent-pool-" + randomString(10)

		// Create a Branch with an annotation referencing a non-existent pool
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect the WakeupRequest to have Succeeded=False with PoolNotFound
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.PoolNotFoundReason)
	})

	t.Run("sets NoPoolAnnotation when the branch has no pool annotation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a Branch without a pool annotation
		branch, err := createBranch(ctx, branchName, nil)
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect the WakeupRequest to have Succeeded=False with NoPoolAnnotation
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.NoPoolAnnotationReason)
	})

	t.Run("sets CSINodePodNotFound when no CSI node pod exists on the primary's node", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a healthy Cluster owned by the pool
		cluster := &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: TestNamespace,
			},
			Spec: apiv1.ClusterSpec{Instances: 1},
		}
		require.NoError(t, controllerutil.SetControllerReference(pool, cluster, testScheme))
		require.NoError(t, k8sClient.Create(ctx, cluster))

		// Create a PV with a CSI volume source
		pvName := clusterName + "-pv"
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       "test.csi.driver",
						VolumeHandle: "slot/" + clusterName,
					},
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pv))

		// Create a PVC bound to the PV
		pvcName := clusterName + "-1"
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: TestNamespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				VolumeName: pvName,
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pvc))

		// Create the primary pod on a node — but do NOT create a CSI node pod
		nodeName := "test-node-" + clusterName
		primaryPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: TestNamespace,
			},
			Spec: corev1.PodSpec{
				NodeName:   nodeName,
				Containers: []corev1.Container{{Name: "postgres", Image: "postgres:17"}},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, primaryPod))

		// Set the Cluster status to healthy with TargetPrimary and HealthyPVC
		cluster.Status.TargetPrimary = pvcName
		cluster.Status.ReadyInstances = 1
		cluster.Status.HealthyPVC = []string{pvcName}
		require.NoError(t, k8sClient.Status().Update(ctx, cluster))

		// Create a Branch with pool annotation
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect CSINodePodNotFound because no CSI node pod exists on the node
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.CSINodePodNotFoundReason)
	})

	t.Run("sets BranchHasNoXVol when the branch has no XVol", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a Branch with a pool annotation but without setting
		// PrimaryXVolName in the status
		branch := &v1alpha1.Branch{
			ObjectMeta: metav1.ObjectMeta{
				Name: branchName,
				Annotations: map[string]string{
					v1alpha1.WakeupPoolAnnotation: "some-pool",
				},
			},
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Instances: 1,
					Storage:   v1alpha1.StorageSpec{Size: "1Gi"},
					Image:     "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7",
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, branch))

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect BranchHasNoXVol because PrimaryXVolName is empty
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.BranchHasNoXVolReason)
	})

	t.Run("sets SlotIDNotAvailable when cluster has no TargetPrimary", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a healthy Cluster owned by the pool but with no TargetPrimary
		cluster := &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: TestNamespace,
			},
			Spec: apiv1.ClusterSpec{Instances: 1},
		}
		require.NoError(t, controllerutil.SetControllerReference(pool, cluster, testScheme))
		require.NoError(t, k8sClient.Create(ctx, cluster))

		// Set the Cluster to have a ready instance but no TargetPrimary
		cluster.Status.ReadyInstances = 1
		require.NoError(t, k8sClient.Status().Update(ctx, cluster))

		// Create a Branch with pool annotation
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect SlotIDNotAvailable because the Cluster has no TargetPrimary
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.SlotIDNotAvailableReason)
	})

	t.Run("sets SlotIDNotAvailable when PV has no CSI source", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		poolName := "pool-" + randomString(10)
		clusterName := "cluster-" + randomString(10)
		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a ClusterPool
		pool := &poolv1alpha1.ClusterPool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolName,
				Namespace: TestNamespace,
			},
			Spec: poolv1alpha1.ClusterPoolSpec{
				Clusters: 1,
				ClusterSpec: apiv1.ClusterSpec{
					Instances: 1,
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pool))

		// Create a healthy Cluster owned by the pool
		cluster := &apiv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: TestNamespace,
			},
			Spec: apiv1.ClusterSpec{Instances: 1},
		}
		require.NoError(t, controllerutil.SetControllerReference(pool, cluster, testScheme))
		require.NoError(t, k8sClient.Create(ctx, cluster))

		// Set TargetPrimary to PVC name
		pvcName := clusterName + "-1"
		pvName := clusterName + "-pv"
		cluster.Status.Phase = apiv1.PhaseHealthy
		cluster.Status.TargetPrimary = pvcName
		cluster.Status.ReadyInstances = 1
		require.NoError(t, k8sClient.Status().Update(ctx, cluster))

		// Create a PV with a HostPath source instead of CSI
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/tmp/test",
					},
				},
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pv))

		// Create a PVC bound to the non-CSI PV
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: TestNamespace,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				VolumeName: pvName,
			},
		}
		require.NoError(t, k8sClient.Create(ctx, pvc))

		// Create a Branch with pool annotation
		branch, err := createBranch(ctx, branchName, map[string]string{
			v1alpha1.WakeupPoolAnnotation: poolName,
		})
		require.NoError(t, err)

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect SlotIDNotAvailable because the PV has no CSI volume source
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionFalse, v1alpha1.SlotIDNotAvailableReason)
	})

	t.Run("sets XVolNotReady when the XVol is in Pending state", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a Branch (createBranch creates an XVol in the Available state)
		branch, err := createBranch(ctx, branchName, nil)
		require.NoError(t, err)

		// Move the XVol into the Pending state
		require.NoError(t, setXVolState(ctx, branch.Status.PrimaryXVolName, "Pending"))

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect XVolNotReady because the XVol is not in a wakeable state
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionUnknown, v1alpha1.XVolNotReadyReason)
	})

	t.Run("sets XVolNotFound when the XVol resource is missing", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branchName := "branch-" + randomString(10)
		wrName := "wur-" + randomString(10)

		// Create a Branch (createBranch creates an XVol in the Available state)
		branch, err := createBranch(ctx, branchName, nil)
		require.NoError(t, err)

		// Delete the XVol so the validation step finds nothing
		require.NoError(t, deleteXVol(ctx, branch.Status.PrimaryXVolName))

		// Create a WakeupRequest
		wr, err := createWakeupRequest(ctx, wrName, branch.Name)
		require.NoError(t, err)

		// Expect XVolNotFound because the XVol resource is missing
		requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionUnknown, v1alpha1.XVolNotFoundReason)
	})
}

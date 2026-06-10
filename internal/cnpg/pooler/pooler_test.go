package pooler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/internal/cnpg/pooler"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		clusterName      string
		instances        int32
		poolMode         apiv1.PgBouncerPoolMode
		maxClientConn    string
		defaultPoolSize  string
		imagePullSecrets []string
		tolerations      []v1.Toleration
		nodeSelector     map[string]string
		expected         *apiv1ac.PoolerSpecApplyConfiguration
	}{
		"active pooler": {
			clusterName:   "cluster-1",
			instances:     1,
			poolMode:      apiv1.PgBouncerPoolModeSession,
			maxClientConn: "100",
			expected:      baseExpectedPoolerSpec("cluster-1", 1, apiv1.PgBouncerPoolModeSession, "100"),
		},
		"zero instances": {
			clusterName:   "cluster-2",
			instances:     0,
			poolMode:      apiv1.PgBouncerPoolModeSession,
			maxClientConn: "100",
			expected:      baseExpectedPoolerSpec("cluster-2", 0, apiv1.PgBouncerPoolModeSession, "100"),
		},
		"with image pull secrets": {
			clusterName:      "cluster-4",
			instances:        1,
			poolMode:         apiv1.PgBouncerPoolModeSession,
			maxClientConn:    "100",
			imagePullSecrets: []string{"ghcr-secret", "ecr-secret"},
			expected: baseExpectedPoolerSpec("cluster-4", 1, apiv1.PgBouncerPoolModeSession, "100").
				WithTemplate(apiv1ac.PodTemplateSpec().WithSpec(func() v1.PodSpec {
					s := basePoolerPodSpec()
					s.ImagePullSecrets = []v1.LocalObjectReference{
						{Name: "ghcr-secret"},
						{Name: "ecr-secret"},
					}
					return s
				}())),
		},
		"with tolerations and node selector": {
			clusterName:   "cluster-6",
			instances:     1,
			poolMode:      apiv1.PgBouncerPoolModeTransaction,
			maxClientConn: "100",
			tolerations: []v1.Toleration{
				{
					Key:      "xata.io/workload",
					Value:    "dataplane",
					Operator: v1.TolerationOpEqual,
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			nodeSelector: map[string]string{
				"xata.io/nodepool": "dataplane",
			},
			expected: baseExpectedPoolerSpec("cluster-6", 1, apiv1.PgBouncerPoolModeTransaction, "100").
				WithTemplate(apiv1ac.PodTemplateSpec().WithSpec(func() v1.PodSpec {
					s := basePoolerPodSpec()
					s.Tolerations = []v1.Toleration{
						{
							Key:      "xata.io/workload",
							Value:    "dataplane",
							Operator: v1.TolerationOpEqual,
							Effect:   v1.TaintEffectNoSchedule,
						},
					}
					s.NodeSelector = map[string]string{
						"xata.io/nodepool": "dataplane",
					}
					return s
				}())),
		},
		"with default_pool_size override": {
			clusterName:     "cluster-5",
			instances:       1,
			poolMode:        apiv1.PgBouncerPoolModeTransaction,
			maxClientConn:   "10000",
			defaultPoolSize: "180",
			expected: baseExpectedPoolerSpec("cluster-5", 1, apiv1.PgBouncerPoolModeTransaction, "10000").
				WithPgBouncer(apiv1ac.PgBouncerSpec().
					WithPoolMode(apiv1.PgBouncerPoolModeTransaction).
					WithParameters(map[string]string{
						"max_client_conn":         "10000",
						"max_prepared_statements": "1000",
						"query_wait_timeout":      "120",
						"default_pool_size":       "180",
						"server_idle_timeout":     "60",
					})),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := pooler.Spec(tc.clusterName, tc.instances, tc.poolMode, tc.maxClientConn, tc.defaultPoolSize, tc.imagePullSecrets, tc.tolerations, tc.nodeSelector)

			require.Equal(t, tc.expected, got)
		})
	}
}

// baseExpectedPoolerSpec builds the apply-config that Spec is expected
// to return for the most common case: standard pgbouncer parameters, no pod
// labels, base pod spec. Test cases override individual fields via .WithX
// chaining.
func baseExpectedPoolerSpec(
	clusterName string,
	instances int32,
	poolMode apiv1.PgBouncerPoolMode,
	maxClientConn string,
) *apiv1ac.PoolerSpecApplyConfiguration {
	return apiv1ac.PoolerSpec().
		WithCluster(corev1ac.LocalObjectReference().WithName(clusterName)).
		WithType(apiv1.PoolerTypeRW).
		WithInstances(instances).
		WithPgBouncer(apiv1ac.PgBouncerSpec().
			WithPoolMode(poolMode).
			WithParameters(map[string]string{
				"max_client_conn":         maxClientConn,
				"max_prepared_statements": "1000",
				"query_wait_timeout":      "120",
				"server_idle_timeout":     "60",
			})).
		WithServiceTemplate(apiv1ac.ServiceTemplateSpec().
			WithObjectMeta(apiv1ac.Metadata().WithAnnotations(pooler.InheritedAnnotations))).
		WithTemplate(apiv1ac.PodTemplateSpec().WithSpec(basePoolerPodSpec()))
}

// basePoolerPodSpec returns the standard pgbouncer pod spec that Spec
// produces when no overrides (image pull secrets, tolerations, node selector)
// are supplied.
func basePoolerPodSpec() v1.PodSpec {
	return v1.PodSpec{
		EnableServiceLinks: new(false),
		Containers: []v1.Container{
			{
				Name: "pgbouncer",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("200m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("500m"),
						v1.ResourceMemory: resource.MustParse("100Mi"),
					},
				},
			},
		},
	}
}

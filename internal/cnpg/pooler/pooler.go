// Package pooler builds the CNPG PgBouncer Pooler spec shared by the
// branch-operator and the clusterpool-operator. Both operators must produce an
// identical Pooler spec for a given cluster so that a pooler pre-warmed by
// the pool is adopted by a branch without rolling the PgBouncer Deployment.
package pooler

import (
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
)

// InheritedAnnotations are set on resources CNPG creates from the Pooler. They
// must match the annotations the Cluster builder sets so a pool-warmed pooler
// is identical to the one a branch applies on adoption.
var InheritedAnnotations = map[string]string{
	// TLS is enabled on the metrics endpoint
	"prometheus.io/scheme": "https",

	// Mark cluster services as Cilium global services
	"service.cilium.io/global": "true",
}

// Spec builds the CNPG PoolerSpec for a PgBouncer connection pooler fronting
// clusterName. It is intentionally branch-agnostic: the pod template carries no
// per-branch labels, so the same inputs always yield the same spec regardless
// of which operator builds it. The caller decides instances (e.g. 0 when the
// branch is hibernated). When defaultPoolSize is non-empty it is set verbatim
// on PgBouncer.
func Spec(clusterName string,
	instances int32,
	poolMode apiv1.PgBouncerPoolMode,
	maxClientConn, defaultPoolSize string,
	imagePullSecrets []string,
	tolerations []v1.Toleration,
	nodeSelector map[string]string,
) *apiv1ac.PoolerSpecApplyConfiguration {
	params := map[string]string{
		"max_client_conn":         maxClientConn,
		"max_prepared_statements": "1000",
		"query_wait_timeout":      "120",
		"server_idle_timeout":     "60",
	}
	if defaultPoolSize != "" {
		params["default_pool_size"] = defaultPoolSize
	}

	var pullSecrets []v1.LocalObjectReference
	for _, name := range imagePullSecrets {
		pullSecrets = append(pullSecrets, v1.LocalObjectReference{Name: name})
	}

	podSpec := v1.PodSpec{
		EnableServiceLinks: new(false),
		ImagePullSecrets:   pullSecrets,
		Tolerations:        tolerations,
		NodeSelector:       nodeSelector,
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

	return apiv1ac.PoolerSpec().
		WithCluster(corev1ac.LocalObjectReference().WithName(clusterName)).
		WithType(apiv1.PoolerTypeRW).
		WithInstances(instances).
		WithPgBouncer(apiv1ac.PgBouncerSpec().
			WithPoolMode(poolMode).
			WithParameters(params)).
		WithServiceTemplate(apiv1ac.ServiceTemplateSpec().
			WithObjectMeta(apiv1ac.Metadata().WithAnnotations(InheritedAnnotations))).
		WithTemplate(apiv1ac.PodTemplateSpec().WithSpec(podSpec))
}

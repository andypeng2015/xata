package reconciler

import (
	"context"
	"strconv"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/internal/cnpg/pooler"
	"xata/services/branch-operator/api/v1alpha1"
)

const PoolerSuffix = "-pooler"

// reconcilePooler ensures that the correct Pooler exists for the given Branch
// when a pooler is configured. If no Pooler is configured for a branch, it
// ensures that no Pooler exists for that branch
func (r *BranchReconciler) reconcilePooler(
	ctx context.Context,
	branch *v1alpha1.Branch,
) error {
	// If there is no Cluster associated with the Branch there is nothing to do.
	// K8s garbage collection will take care of cleaning up any Pooler that may
	// exist from a previously associated Cluster
	if !branch.HasClusterName() {
		return nil
	}

	// The Pooler is named with the convention <cluster-name>-pooler. The
	// Pooler's lifecycle is bound to the Cluster, not the Branch.
	poolerName := branch.ClusterName() + PoolerSuffix

	// Ensure the Pooler does not exist when the branch does not specify a pooler
	// configuration
	if !branch.Spec.Pooler.IsEnabled() {
		pooler := &apiv1.Pooler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      poolerName,
				Namespace: r.ClustersNamespace,
			},
		}
		err := r.Get(ctx, types.NamespacedName{
			Name:      poolerName,
			Namespace: r.ClustersNamespace,
		}, pooler)
		if err != nil {
			return client.IgnoreNotFound(err)
		}

		return r.Delete(ctx, pooler)
	}

	// Get the Cluster associated with the Branch.
	cluster := &apiv1.Cluster{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      branch.ClusterName(),
		Namespace: r.ClustersNamespace,
	}, cluster)
	if err != nil {
		return err
	}

	// The pooler scales to zero instances while the branch is hibernated.
	instances := branch.Spec.Pooler.Instances
	if branch.Spec.ClusterSpec.Hibernation.IsEnabled() {
		instances = 0
	}

	// Org/project labels live on the Pooler resource (not the pod template):
	// the pod template is kept branch-agnostic so a pre-warmed pool pooler can
	// be adopted without rolling its PgBouncer pod.
	ac := apiv1ac.Pooler(poolerName, r.ClustersNamespace).
		WithLabels(clusterLabels(branch.Spec.InheritedMetadata)).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(apiv1.SchemeGroupVersion.String()).
			WithKind(apiv1.ClusterKind).
			WithName(cluster.Name).
			WithUID(cluster.UID).
			WithBlockOwnerDeletion(true).
			WithController(false)).
		WithSpec(pooler.Spec(
			branch.ClusterName(),
			instances,
			apiv1.PgBouncerPoolMode(branch.Spec.Pooler.Mode),
			branch.Spec.Pooler.MaxClientConn,
			defaultPoolSize(branch),
			r.ImagePullSecrets,
			r.Tolerations,
			branch.Spec.ClusterSpec.Affinity.GetNodeSelector(),
		))

	return r.Apply(ctx, ac, client.FieldOwner(OperatorName), client.ForceOwnership)
}

// defaultPoolSize returns the PgBouncer default_pool_size to set for the
// branch. An operator-supplied override on the PoolerSpec wins; otherwise
// the value is derived as floor(0.9 * max_connections) from the branch's
// Postgres parameters. Returns "" when neither is available, which leaves
// the PgBouncer default in place.
func defaultPoolSize(branch *v1alpha1.Branch) string {
	if v := branch.Spec.Pooler.DefaultPoolSize; v != "" {
		// CRD validation enforces ^[1-9][0-9]*$; re-check in case an
		// older CRD let a malformed value through.
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return v
		}
	}
	maxConns := maxConnectionsFromBranch(branch)
	if maxConns <= 0 {
		return ""
	}
	return strconv.Itoa(maxConns * 9 / 10)
}

// maxConnectionsFromBranch extracts the max_connections value from the
// Branch's Postgres parameters. Returns 0 when unset or unparseable.
func maxConnectionsFromBranch(branch *v1alpha1.Branch) int {
	if branch.Spec.ClusterSpec.Postgres == nil {
		return 0
	}
	for _, p := range branch.Spec.ClusterSpec.Postgres.Parameters {
		if p.Name != "max_connections" {
			continue
		}
		v, err := strconv.Atoi(p.Value)
		if err != nil || v <= 0 {
			return 0
		}
		return v
	}
	return 0
}

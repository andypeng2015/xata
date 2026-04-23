package sidecar

import (
	"context"
	"fmt"
	"time"

	cnpgv1 "github.com/xataio/xata-cnpg/api/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	branchv1alpha1 "xata/services/branch-operator/api/v1alpha1"
)

// clusterRetriever is responsible for retrieving the CloudNativePG cluster and
// its credentials. It caches the cluster results to avoid frequent API calls.
// The refresh interval is configurable.
type cnpgClusterClient struct {
	client     client.Client
	clusterKey types.NamespacedName

	cluster                *cnpgv1.Cluster
	lastClusterUpdate      time.Time
	clusterRefreshInterval time.Duration
}

// postgreSQLCredentials holds the connection information for PostgreSQL
type postgreSQLCredentials struct {
	username string
	database string
	host     string
	port     string
}

type scaleToZeroConfig struct {
	enabled           bool
	inactivityMinutes int
}

const (
	defaultRefreshInterval = 30 * time.Second

	forceUpdate      = true
	doNotForceUpdate = false

	branchAnnotation = "xata.io/branch"
)

// newClusterClient creates a new instance of clusterClient with the provided
// cnpg client and cluster key. It initializes the refresh interval to a default
// value if not provided.
func newClusterClient(client client.Client, clusterKey types.NamespacedName, refreshInterval time.Duration) *cnpgClusterClient {
	if refreshInterval == 0 {
		refreshInterval = defaultRefreshInterval
	}

	return &cnpgClusterClient{
		client:                 client,
		clusterKey:             clusterKey,
		clusterRefreshInterval: refreshInterval,
	}
}

func (r *cnpgClusterClient) updateCluster(ctx context.Context, cluster *cnpgv1.Cluster) error {
	return r.client.Update(ctx, cluster)
}

// getCluster retrieves the CloudNativePG cluster object, refreshing it if necessary
func (r *cnpgClusterClient) getCluster(ctx context.Context, forceUpdate bool) (*cnpgv1.Cluster, error) {
	if !forceUpdate && time.Since(r.lastClusterUpdate) < r.clusterRefreshInterval {
		return r.cluster, nil
	}

	cluster := &cnpgv1.Cluster{}
	if err := r.client.Get(ctx, r.clusterKey, cluster); err != nil {
		return nil, err
	}

	r.cluster = cluster
	r.lastClusterUpdate = time.Now()
	return r.cluster, nil
}

// getBranchName resolves the branch name from the cluster's xata.io/branch
// annotation. Falls back to the cluster name if the annotation is missing.
func (r *cnpgClusterClient) getBranchName(ctx context.Context) (string, error) {
	cluster, err := r.getCluster(ctx, doNotForceUpdate)
	if err != nil {
		return "", fmt.Errorf("get cluster for branch name resolution: %w", err)
	}

	if name, ok := cluster.Annotations[branchAnnotation]; ok {
		return name, nil
	}
	return r.clusterKey.Name, nil
}

// socketDir is the Unix socket directory used by CNPG for the PostgreSQL
// server. The sidecar container, running with the same UID as the postgres
// container, authenticates as the "postgres" role via peer auth over this
// socket — no password required.
const socketDir = "/controller/run"

func (r *cnpgClusterClient) getClusterCredentials(_ context.Context) (*postgreSQLCredentials, error) {
	return &postgreSQLCredentials{
		username: "postgres",
		database: "postgres",
		host:     socketDir,
		port:     "5432",
	}, nil
}

func (r *cnpgClusterClient) getClusterScheduledBackup(ctx context.Context) (*cnpgv1.ScheduledBackup, error) {
	branchName, err := r.getBranchName(ctx)
	if err != nil {
		return nil, err
	}
	scheduledBackup := &cnpgv1.ScheduledBackup{}
	key := types.NamespacedName{Namespace: r.clusterKey.Namespace, Name: branchName}
	if err := r.client.Get(ctx, key, scheduledBackup); err != nil {
		return nil, err
	}
	return scheduledBackup, nil
}

func (r *cnpgClusterClient) updateClusterScheduledBackup(ctx context.Context, scheduledBackup *cnpgv1.ScheduledBackup) error {
	return r.client.Update(ctx, scheduledBackup)
}

func (r *cnpgClusterClient) getBranch(ctx context.Context) (*branchv1alpha1.Branch, error) {
	branchName, err := r.getBranchName(ctx)
	if err != nil {
		return nil, err
	}
	branch := &branchv1alpha1.Branch{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: branchName}, branch); err != nil {
		return nil, err
	}
	return branch, nil
}

func (r *cnpgClusterClient) patchBranchHibernation(ctx context.Context, branch *branchv1alpha1.Branch) error {
	patch := fmt.Appendf(nil, `{"spec":{"cluster":{"hibernation":%q}}}`, branchv1alpha1.HibernationModeEnabled)
	return r.client.Patch(ctx, branch, client.RawPatch(types.MergePatchType, patch))
}

func (r *cnpgClusterClient) patchBranchClusterName(ctx context.Context, branch *branchv1alpha1.Branch) error {
	patch := []byte(`{"spec":{"cluster":{"name":null}}}`)
	return r.client.Patch(ctx, branch, client.RawPatch(types.MergePatchType, patch))
}

func (p *postgreSQLCredentials) connString() string {
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable application_name=scale-to-zero",
		p.host, p.port, p.username, p.database)
}

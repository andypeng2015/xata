package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustersv1 "xata/gen/proto/clusters/v1"
	internalgrpc "xata/internal/grpc"
	"xata/internal/o11y"
	"xata/services/gateway/metrics"
)

// ErrBranchHibernated is returned when the branch is manually hibernated
// (scale-to-zero disabled) and cannot be auto-reactivated.
var ErrBranchHibernated = errors.New("branch is hibernated")

// ErrBranchNotFound is returned when the branch has been terminated and its
// clusters-<branchID> Service no longer exists.
var ErrBranchNotFound = errors.New("branch not found")

type clustersServiceClientFn func(ctx context.Context, branchID string) (clustersServiceClient, error)

// ClusterDialer is responsible for dialing into a Postgres cluster, handling
// reactivation when the cluster is hibernated.
type ClusterDialer struct {
	dialer dialerFn

	clustersServiceClient clustersServiceClientFn

	reactivateFn        reactivateClusterFn
	reactivateTimeout   time.Duration
	statusCheckInterval time.Duration
}

type reactivateClusterFn func(ctx context.Context, svc clustersService, clusterID, network, address string) (net.Conn, error)

type dialerFn func(ctx context.Context, network, address string) (net.Conn, error)

type ClusterDialerOption func(*ClusterDialer)

type ClusterDialerConfiguration struct {
	ReactivateTimeout   time.Duration
	StatusCheckInterval time.Duration
}

type clustersService interface {
	DescribePostgresCluster(ctx context.Context, request *clustersv1.DescribePostgresClusterRequest, opts ...grpc.CallOption) (*clustersv1.DescribePostgresClusterResponse, error)
	UpdatePostgresCluster(ctx context.Context, request *clustersv1.UpdatePostgresClusterRequest, opts ...grpc.CallOption) (*clustersv1.UpdatePostgresClusterResponse, error)
}

type clustersServiceClient interface {
	clustersService
	Close() error
}

type clustersServiceClientImpl struct {
	clustersv1.ClustersServiceClient
	conn *grpc.ClientConn
}

func (c *clustersServiceClientImpl) Close() error {
	return c.conn.Close()
}

var netDialer = func(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

// NewClusterDialer creates a dialer for connecting to Postgres clusters.
func NewClusterDialer(cfg ClusterDialerConfiguration, opts ...ClusterDialerOption) *ClusterDialer {
	d := &ClusterDialer{
		dialer:                netDialer,
		clustersServiceClient: defaultClustersService(),
		reactivateTimeout:     cfg.ReactivateTimeout,
		statusCheckInterval:   cfg.StatusCheckInterval,
	}

	d.reactivateFn = d.reactivateCluster

	for _, opt := range opts {
		opt(d)
	}

	return d
}

func defaultClustersService() clustersServiceClientFn {
	return func(ctx context.Context, branchID string) (clustersServiceClient, error) {
		o := o11y.Ctx(ctx)
		conn, err := internalgrpc.NewClient(o, "clusters-"+branchID+":5002")
		if err != nil {
			return nil, err
		}

		return &clustersServiceClientImpl{
			ClustersServiceClient: clustersv1.NewClustersServiceClient(conn),
			conn:                  conn.ClientConn,
		}, nil
	}
}

func WithInstrumentation(gwMetrics *metrics.GatewayMetrics) ClusterDialerOption {
	return func(d *ClusterDialer) {
		reactivate := d.reactivateCluster
		d.reactivateFn = func(ctx context.Context, svc clustersService, clusterID, network, address string) (net.Conn, error) {
			startTime := time.Now()
			defer func() {
				gwMetrics.RecordClusterReactivation(ctx, time.Since(startTime))
			}()
			return reactivate(ctx, svc, clusterID, network, address)
		}
	}
}

func WithDialer(dialer dialerFn) ClusterDialerOption {
	return func(d *ClusterDialer) {
		d.dialer = dialer
	}
}

func WithClustersService(factory clustersServiceClientFn) ClusterDialerOption {
	return func(d *ClusterDialer) {
		d.clustersServiceClient = factory
	}
}

// Dial connects to the specified branch, handling reactivation if the cluster
// is hibernated and the configuration allows it.
func (d *ClusterDialer) Dial(ctx context.Context, network string, branch *Branch) (net.Conn, error) {
	dialLogger := log.Ctx(ctx).With().Str("cluster", branch.ID).Str("address", branch.Address).Logger()
	conn, dialErr := d.dialer(ctx, network, branch.Address)
	if !shouldAttemptReactivation(dialErr) {
		return conn, dialErr
	}

	// if the connection failed (refused or DNS not found), check if the
	// cluster is in hibernation mode and reactivate it if scale to zero is enabled
	svc, err := d.clustersServiceClient(ctx, branch.ID)
	if err != nil {
		dialLogger.Error().Err(err).Msg("failed to create clusters service client")
		return nil, dialErr
	}
	defer svc.Close()

	cluster, err := svc.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
		Id: branch.ID,
	})
	if err != nil {
		if status.Code(err) == codes.Unavailable && strings.Contains(err.Error(), "produced zero addresses") {
			return nil, ErrBranchNotFound
		}
		dialLogger.Error().Err(err).Msg("failed to describe cluster")
		return nil, dialErr
	}

	switch {
	case d.isClusterHibernated(cluster.Status) && d.isScaleToZeroEnabled(cluster.Configuration):
		dialLogger.Info().Msg("cluster is hibernated, reactivating...")

		conn, err := d.reactivateFn(ctx, svc, branch.ID, network, branch.Address)
		if err != nil {
			dialLogger.Error().Err(err).Msg("failed to reactivate cluster")
			return nil, dialErr
		}

		dialLogger.Info().Msg("cluster reactivated successfully")
		return conn, nil

	case d.isClusterHibernated(cluster.Status):
		return nil, ErrBranchHibernated

	case d.isScaleToZeroEnabled(cluster.Configuration) && d.isClusterStartingOrHealthy(cluster.Status):
		// The cluster is not hibernated but the dial still failed. With
		// scale-to-zero enabled this means it is mid-wake: a concurrent request
		// already cleared the hibernation flag, or the Postgres instances are
		// back but the dial target (the pooler Service) isn't routable yet. Hold
		// the connection and wait for it to become reachable instead of
		// returning the dial error.
		dialLogger.Info().Msg("cluster is reactivating, waiting for it to become available...")
		conn, err := d.waitUntilReachable(ctx, svc, branch.ID, network, branch.Address)
		if err != nil {
			dialLogger.Error().Err(err).Msg("failed to wait for cluster to be available")
			return nil, dialErr
		}

		dialLogger.Info().Msg("cluster is now available after waiting for instances to become active")
		return conn, nil
	default:
		dialLogger.Warn().Stringer("status_type", cluster.Status.StatusType).Msg("dial failed but cluster is not hibernated or reactivating")
		return nil, dialErr
	}
}

func (d *ClusterDialer) isScaleToZeroEnabled(cfg *clustersv1.ClusterConfiguration) bool {
	return cfg.ScaleToZero != nil && cfg.ScaleToZero.Enabled
}

func (d *ClusterDialer) isClusterHibernated(status *clustersv1.ClusterStatus) bool {
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED
}

// isClusterStartingOrHealthy reports whether the cluster is healthy or still
// transitioning toward healthy (any transient phase), as opposed to faulted or
// in an unknown state. A failed dial against such a cluster means the target
// endpoint hasn't caught up yet, so the connection should be held rather than
// dropped.
func (d *ClusterDialer) isClusterStartingOrHealthy(status *clustersv1.ClusterStatus) bool {
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY ||
		status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT
}

func (d *ClusterDialer) reactivateCluster(ctx context.Context, svc clustersService, clusterID, network, address string) (net.Conn, error) {
	_, err := svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
		Id: clusterID,
		UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
			Hibernate: new(false),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("reactivating hibernated cluster %s: %w", clusterID, err)
	}

	return d.waitUntilReachable(ctx, svc, clusterID, network, address)
}

// waitUntilReachable waits until the cluster is reported available AND a TCP
// connection to the dial target succeeds. The cluster status only reflects the
// Postgres instances; the dial target may be a separate component (e.g. the
// pooler Service) whose endpoints lag behind the cluster becoming healthy.
// Returns the live connection on success so the caller doesn't have to redial.
func (d *ClusterDialer) waitUntilReachable(ctx context.Context, svc clustersService, clusterID, network, address string) (net.Conn, error) {
	logger := log.Ctx(ctx).With().Str("cluster", clusterID).Str("address", address).Logger()
	reactivateTimeout := time.NewTimer(d.reactivateTimeout)
	defer reactivateTimeout.Stop()
	statusChecker := time.NewTicker(d.statusCheckInterval)
	defer statusChecker.Stop()

	clusterReady := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-reactivateTimeout.C:
			return nil, fmt.Errorf("timed out waiting for cluster %s to be reactivated after %s", clusterID, d.reactivateTimeout)
		case <-statusChecker.C:
			if !clusterReady {
				cluster, err := svc.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: clusterID,
				})
				if err != nil {
					return nil, fmt.Errorf("checking cluster status: %w", err)
				}
				if !d.isClusterAvailable(cluster.Status) {
					logger.Debug().Msgf("waiting for cluster to be available, current status: %s, next check: %s", cluster.Status.StatusType, d.statusCheckInterval)
					continue
				}
				clusterReady = true
			}

			conn, err := d.dialer(ctx, network, address)
			if err == nil {
				return conn, nil
			}
			if !shouldAttemptReactivation(err) {
				return nil, fmt.Errorf("dialing %s: %w", address, err)
			}
			logger.Debug().Err(err).Msgf("cluster ready but target not yet reachable, next check: %s", d.statusCheckInterval)
		}
	}
}

// isClusterAvailable checks if the cluster is available for connections.
// It first checks if the cluster primary instance is healthy, and returns true if so.
// If no healthy primary is found, it then checks if the cluster is healthy and all expected
// instances are ready. Note: The cluster status may briefly appear as healthy after reactivation,
// before starting the instances and switching to transient status. Therefore, this function
// prioritizes primary instance health, then falls back to full cluster health and instance readiness.
func (d *ClusterDialer) isClusterAvailable(status *clustersv1.ClusterStatus) bool {
	if status == nil {
		return false
	}
	for _, instance := range status.Instances {
		if instance.Primary && instance.Status == apiv1.PodHealthy {
			return true
		}
	}
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY &&
		status.InstanceCount == status.InstanceReadyCount
}

// shouldAttemptReactivation returns true for dial errors that indicate the
// cluster may be hibernated: connection refused (pod exists but not listening)
// or DNS not found (K8s service deleted during scale-to-zero).
func shouldAttemptReactivation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}

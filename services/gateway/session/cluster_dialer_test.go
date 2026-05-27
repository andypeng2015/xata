package session

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

func TestClusterDialer_Dial(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	errTest := errors.New("oh noes")
	errDNS := &net.DNSError{Err: "server misbehaving", Name: "branch-rw.svc", IsTemporary: true}

	tests := map[string]struct {
		dialer          *mockDialer
		clustersService clustersServiceClientFn
		setupMocks      func(*protomocks.ClustersServiceClient)

		wantDialCalls    uint // exact expected dial count; ignored if wantMinDialCalls is set
		wantMinDialCalls uint // for timeout-driven tests where the exact count depends on tick timing
		wantErr          error
	}{
		"ok - no dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return &net.TCPConn{}, nil
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {},

			wantDialCalls: 1,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivates cluster": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - DNS not found triggers reactivation for hibernated cluster": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, &net.DNSError{Err: "no such host", Name: "branch-rw.xata-clusters.svc", IsNotFound: true}
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivation ongoing": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						Status:     apiv1.PhaseWaitingForInstancesToBeActive,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      2,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"instance-1": {
								Primary: true,
								Status:  apiv1.PodHealthy,
							},
							"instance-2": {
								Primary: false,
								Status:  apiv1.PodFailed,
							},
						},
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivates cluster with only primary": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      2,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"instance-1": {
								Primary: true,
								Status:  apiv1.PodHealthy,
							},
							"instance-2": {
								Primary: false,
								Status:  apiv1.PodFailed,
							},
						},
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivates cluster waiting for instances to be ready": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 0,
					},
				}, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"error - unable to connect to clusters service, returns dial error": {
			clustersService: clustersServiceClientFn(func(ctx context.Context, branchID string) (clustersServiceClient, error) {
				return nil, errors.New("some error")
			}),
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused with scale to zero disabled, returns error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: false},
					},
				}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - manually hibernated cluster returns ErrBranchHibernated": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: false},
					},
				}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       ErrBranchHibernated,
		},
		// A concurrent request already reactivated the cluster (so it reports
		// HEALTHY, not HIBERNATED) but the dial target — typically the pooler
		// Service — isn't routable yet. The connection must be held and the
		// target re-probed until it succeeds, rather than failing fast.
		"ok - scale to zero enabled, cluster healthy but target briefly unreachable, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return &net.TCPConn{}, nil
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		// Same race as above, but the target never recovers: the dialer keeps
		// re-probing until reactivateTimeout, then surfaces the dial error.
		"error - scale to zero enabled, cluster healthy but target stays unreachable, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)
			},

			wantMinDialCalls: 2,
			wantErr:          syscall.ECONNREFUSED,
		},
		// Simulates a hibernated cluster that reactivates successfully (Postgres
		// instances come back) but the dial target (e.g. the pooler Service)
		// stays unreachable. waitUntilReachable retries the probe-dial until
		// reactivateTimeout, then surfaces the original dial error.
		"error - hibernated cluster reactivates but target stays unreachable, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantMinDialCalls: 2,
			wantErr:          syscall.ECONNREFUSED,
		},
		"error - connection refused, error describing cluster, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused, error updating cluster, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused, error describing cluster after update, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused with hibernated cluster and scale to zero enabled, timeout on reactivation returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - other dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, errTest
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
			},

			wantDialCalls: 1,
			wantErr:       errTest,
		},
		"error - temporary DNS error does not trigger reactivation": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, errDNS
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {},

			wantDialCalls: 1,
			wantErr:       errDNS,
		},
		"error - transient clusters service unavailable returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, status.Error(codes.Unavailable, "connection refused")).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		// Real grpc client so upstream changes to "produced zero addresses" break the match in Dial.
		"error - real grpc client resolves to zero addresses returns branch-not-found": {
			dialer: &mockDialer{
				dialFn: func(_ context.Context, _ uint, _, address string) (net.Conn, error) {
					return nil, &net.DNSError{Err: "no such host", Name: address, IsNotFound: true}
				},
			},
			setupMocks:      func(mockClusters *protomocks.ClustersServiceClient) {},
			clustersService: zeroAddressClustersService(),
			wantDialCalls:   1,
			wantErr:         ErrBranchNotFound,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockClusters := protomocks.NewClustersServiceClient(t)
			tc.setupMocks(mockClusters)

			if tc.clustersService == nil {
				tc.clustersService = clustersServiceClientFn(func(ctx context.Context, branchID string) (clustersServiceClient, error) {
					return &mockClustersServiceClient{mockClusters}, nil
				})
			}

			d := NewClusterDialer(ClusterDialerConfiguration{
				ReactivateTimeout:   time.Second,
				StatusCheckInterval: time.Millisecond * 100,
			}, WithClustersService(tc.clustersService), WithDialer(tc.dialer.Dial))

			_, err := d.Dial(ctx, "tcp", &Branch{
				ID:      "test-branch",
				Address: "test-branch-address",
			})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantMinDialCalls > 0 {
				require.GreaterOrEqual(t, tc.dialer.DialCalls(), tc.wantMinDialCalls, "unexpected number of dial calls")
			} else {
				require.Equal(t, tc.wantDialCalls, tc.dialer.DialCalls(), "unexpected number of dial calls")
			}

			mockClusters.AssertExpectations(t)
		})
	}
}

type mockDialer struct {
	dialCalls uint
	dialFn    func(ctx context.Context, i uint, network, address string) (net.Conn, error)
}

func (m *mockDialer) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	m.dialCalls++
	return m.dialFn(ctx, m.dialCalls, network, address)
}

func (m *mockDialer) DialCalls() uint {
	return m.dialCalls
}

type mockClustersServiceClient struct {
	*protomocks.ClustersServiceClient
}

func (m *mockClustersServiceClient) Close() error {
	return nil
}

func zeroAddressClustersService() clustersServiceClientFn {
	mr := manual.NewBuilderWithScheme("test-zero-addr")
	resolver.Register(mr)
	mr.InitialState(resolver.State{})
	return func(ctx context.Context, branchID string) (clustersServiceClient, error) {
		conn, err := grpc.NewClient(mr.Scheme()+":///"+branchID,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(`{}`),
		)
		if err != nil {
			return nil, err
		}
		return &realClustersClient{ClustersServiceClient: clustersv1.NewClustersServiceClient(conn), conn: conn}, nil
	}
}

type realClustersClient struct {
	clustersv1.ClustersServiceClient
	conn *grpc.ClientConn
}

func (c *realClustersClient) Close() error { return c.conn.Close() }

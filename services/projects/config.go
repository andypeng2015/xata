package projects

import (
	"xata/services/projects/store/sqlstore"
)

type Config struct {
	SQLStore sqlstore.Config

	// AuthGRPCURL is the URL of the auth service gRPC endpoint
	AuthGRPCURL string `env:"AUTH_GRPC_URL" env-default:"auth:5002"`

	// ClustersGRPCURL is the URL of the clusters service gRPC endpoint
	ClustersGRPCURL string `env:"CLUSTERS_GRPC_URL" env-default:"clusters:5002"`

	// GatewayHost is the host of the gateway service, used to build connection strings
	GatewayHostPort string `env:"GATEWAY_HOSTPORT" env-default:"127.0.0.1.nip.io:7654"`

	// BranchTreeChildMaxChildren is the maximum number of children a child branch can have
	BranchTreeChildMaxChildren int32 `env:"BRANCH_TREE_CHILD_MAX_CHILDREN" env-default:"100" env-description:"The maximum number of children a child branch can have"`

	// BranchTreeMaxDepth is the maximum depth of the branch tree
	BranchTreeMaxDepth int32 `env:"BRANCH_TREE_MAX_DEPTH" env-default:"50" env-description:"The maximum depth of the branch tree"`

	// SchedulerConfigPath is the path to the scheduler configuration file
	SchedulerConfigPath string `env:"SCHEDULER_CONFIG_PATH" env-default:"/config/scheduler.yaml" env-description:"Path to the scheduler configuration YAML file"`

	// DefaultRegion is the name of the default region to initialize on startup.
	// If empty, no default region or cell is created.
	DefaultRegion string `env:"DEFAULT_REGION" env-default:"" env-description:"Name of the default region to initialize on startup. If empty, no default region/cell is created"`
}

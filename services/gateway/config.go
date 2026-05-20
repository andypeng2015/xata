package gateway

import (
	"fmt"
	"time"
)

type Config struct {
	ListenAddress   string        `env:"XATA_SQL_GW_LISTEN_ADDRESS" env-description:"SQL Gateway: address and port to listen on" env-default:""`
	SSLCertPath     string        `env:"XATA_SQL_GW_SSL_CERT_PATH" env-description:"SQL Gateway: path to TLS certificate" env-default:""`
	SSLKeyPath      string        `env:"XATA_SQL_GW_SSL_KEY_PATH" env-description:"SQL Gateway: path to TLS private key" env-default:""`
	HealthCheckPort int           `env:"XATA_SQL_GW_HEALTH_CHECK_PORT" env-description:"SQL Gateway: port to listen on for health checks, -1 to disable" env-default:"8089"`
	DrainingTime    time.Duration `env:"XATA_SQL_GW_DRAINING_TIME" env-description:"SQL Gateway: time to wait for connections to drain before shutting down" env-default:"3500s"`
	TargetPort      int           `env:"XATA_SQL_GW_TARGET_PORT" env-description:"SQL Gateway: port to forward requests to" env-default:"5432"`
	CNPGNamespace   string        `env:"XATA_SQL_GW_CNPG_NAMESPACE" env-description:"SQL Gateway: namespace for the CloudNativePG instances" env-default:"xata-clusters"`
	XataNamespace   string        `env:"XATA_NAMESPACE" env-description:"namespace for xata services" env-default:"xata"`
	// BranchesConfigMapName is the name of the ConfigMap containing IP filtering rules
	BranchesConfigMapName string `env:"XATA_SQL_GW_BRANCHES_CONFIGMAP_NAME" env-description:"SQL Gateway: name of the branches ConfigMap" env-default:"ipfiltering"`
	// ClustersGRPCURL is the URL of the clusters service gRPC endpoint
	ClustersGRPCURL string `env:"CLUSTERS_GRPC_URL" env-default:"clusters:5002"`
	// ClusterReactivateTimeout is the timeout for reactivating a hibernated cluster on the first connection
	ClusterReactivateTimeout time.Duration `env:"XATA_SQL_GW_CLUSTER_REACTIVATE_TIMEOUT" env-description:"SQL Gateway: timeout for reactivating a hibernated cluster on the first connection" env-default:"50s"`
	// ClusterStatusCheckInterval is the interval for checking the status of a cluster during reactivation
	ClusterStatusCheckInterval time.Duration `env:"XATA_SQL_GW_CLUSTER_STATUS_CHECK_INTERVAL" env-description:"SQL Gateway: interval for checking the status of a cluster during reactivation" env-default:"100ms"`
	// HTTPEnabled enables the HTTP and WebSocket endpoints for the SQL Gateway.
	HTTPEnabled bool `env:"XATA_SQL_GW_HTTP_ENABLED" env-description:"SQL Gateway: enable HTTP/WebSocket endpoints" env-default:"true"`
	// HTTPListenAddress is the address and port to listen on for HTTP and WebSocket connections.
	HTTPListenAddress string `env:"XATA_SQL_GW_HTTP_LISTEN_ADDRESS" env-description:"SQL Gateway: address and port to listen on for HTTP/WebSocket" env-default:":8443"`
	// EnablePooler allows connecting to PostgreSQL using PgBouncer
	EnablePooler bool `env:"XATA_ENABLE_POOLER" env-default:"true" env-description:"enable PgBouncer connection pooler for new branches"`
}

func (c *Config) Validate() error {
	if c.ListenAddress == "" {
		return fmt.Errorf("listen address is required")
	}
	return nil
}

type ServerConfig struct {
	// Listen is the address to listen on for incoming wire protocol connections.
	// The format of the address is "host:port".
	Listen string
	// DrainingTime is the time to wait for connections to drain before shutting down. After receiving a SIGTERM, the server will stop accepting new connections and wait for existing connections to drain.
	DrainingTime time.Duration
}

type CLIConfig struct {
	DevPostgresURL string // development postgres connection string. If set the gateway will always route to this postgres instance.
}

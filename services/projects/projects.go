package projects

import (
	"context"
	"errors"
	"fmt"
	"os"

	"xata/internal/postgresversions"

	"xata/services/projects/metrics"
	"xata/services/projects/scheduler"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/internal/analytics"
	capi "xata/internal/api"
	"xata/internal/envcfg"
	internalgrpc "xata/internal/grpc"
	"xata/internal/o11y"
	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/service"
	"xata/services/projects/api"
	"xata/services/projects/api/spec"
	"xata/services/projects/cells"
	"xata/services/projects/rpc"
	"xata/services/projects/store"
	"xata/services/projects/store/sqlstore"

	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
)

// ensure ProjectService implements HTTPService interface
var _ service.HTTPService = (*ProjectsService)(nil)

// ensure ProjectService implements GRPCService interface
var _ service.GRPCService = (*ProjectsService)(nil)

type ProjectsService struct {
	config    Config
	store     store.ProjectsStore
	feat      openfeature.Client
	analytics analytics.Client
	scheduler *scheduler.Scheduler
	authConn  *internalgrpc.ClientConnection // connection to the auth service
}

func NewProjectsService() *ProjectsService {
	return &ProjectsService{}
}

// SetOpenFeatureClient overrides the feature flag client. This is used by the
// SaaS wrapper to inject a PostHog-backed client instead of the default noop.
func (s *ProjectsService) SetOpenFeatureClient(c openfeature.Client) {
	s.feat = c
}

func (s *ProjectsService) Name() string {
	return "projects"
}

func (s *ProjectsService) ReadConfig(ctx context.Context) error {
	return envcfg.Read(&s.config)
}

func (s *ProjectsService) Setup(ctx context.Context) error {
	// setup the store
	err := s.store.Setup(ctx)
	if err != nil {
		return err
	}

	// initialize main region and cell
	if s.config.DefaultRegion != "" {
		// create the default region (if it doesn't exist)
		_, err = s.store.CreateRegion(ctx, s.config.DefaultRegion, store.RegionFlags{PublicAccess: true, BackupsEnabled: true}, s.config.GatewayHostPort)
		if err != nil && !errors.As(err, &store.ErrRegionAlreadyExists{}) {
			return err
		}

		// create the default cell (if it doesn't exist)
		isPrimaryCell := true
		_, err = s.store.CreateCell(ctx, s.config.DefaultRegion, "cell-1", s.config.ClustersGRPCURL, isPrimaryCell)
		if err != nil && !errors.As(err, &store.ErrCellAlreadyExists{}) {
			return err
		}
	}

	return nil
}

func (s *ProjectsService) Close(ctx context.Context) error {
	// Close the store
	if err := s.store.Close(ctx); err != nil {
		return err
	}

	// Close the analytics client
	if err := s.analytics.Close(ctx); err != nil {
		return err
	}

	// Close the gRPC connection to the auth service
	if err := s.authConn.Close(); err != nil {
		return err
	}

	return nil
}

func (s *ProjectsService) Init(ctx context.Context) error {
	var err error
	s.store, err = sqlstore.NewSQLProjectStore(ctx, s.config.SQLStore, s.config.BranchTreeMaxDepth, s.config.BranchTreeChildMaxChildren)
	if err != nil {
		return err
	}

	// Initialize the OpenFeature client (if not already set)
	if s.feat == nil {
		s.feat, err = openfeature.NewClient(ctx, "projects-service")
		if err != nil {
			return err
		}
	}

	// Initialize the analytics client (if not already set)
	if s.analytics == nil {
		s.analytics, err = analytics.NewClient(ctx)
		if err != nil {
			return err
		}
	}

	// Initialize the gRPC connection to the auth service
	o := o11y.Ctx(ctx)
	s.authConn, err = internalgrpc.NewClient(o, s.config.AuthGRPCURL)
	if err != nil {
		return err
	}

	// Initialize the scheduler
	cfgFile, err := os.Open(s.config.SchedulerConfigPath)
	if err != nil {
		return fmt.Errorf("failed to open scheduler config file: %w", err)
	}
	defer cfgFile.Close()

	s.scheduler, err = scheduler.NewScheduler(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to create scheduler: %w", err)
	}

	return nil
}

func (s *ProjectsService) SetFeat(feat openfeature.Client) {
	s.feat = feat
}

func (s *ProjectsService) SetAnalytics(analytics analytics.Client) {
	s.analytics = analytics
}

func (s *ProjectsService) RegisterHTTPHandlers(o *o11y.O, router *echo.Group) error {
	// require auth for all routes
	group := router.Group("", capi.AuthMiddleware(s.authConn), openfeature.Middleware())

	cellsConn := cells.New(s.store)

	// Metrics client routes branch metric/log queries to the
	// VictoriaMetrics/VictoriaLogs backend via clusters gRPC.
	metricsClient := metrics.NewCellsClient(cellsConn)

	spec.RegisterHandlers(group,
		api.NewAPIHandler(
			s.feat,
			s.store,
			cellsConn,
			s.config.GatewayHostPort,
			metricsClient,
			s.scheduler,
			s.analytics,
			&postgrescfg.DefaultPostgresConfigProvider{},
			&postgresversions.DefaultImageProvider{}),
	)

	return nil
}

// Store returns the underlying ProjectsStore so that SaaS wrappers can register
// additional handlers without re-initializing the store.
func (s *ProjectsService) Store() store.ProjectsStore {
	return s.store
}

// RegisterGRPCHandlers implements service.GRPCService.
func (s *ProjectsService) RegisterGRPCHandlers(o *o11y.O, server *grpc.Server) {
	projectsv1.RegisterProjectsServiceServer(server, rpc.NewProjectsService(s.store, cells.New(s.store)))
}

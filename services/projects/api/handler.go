package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"xata/internal/analytics"
	"xata/internal/analytics/events"
	"xata/internal/api"
	"xata/internal/extensions"
	"xata/internal/flags"
	"xata/internal/o11y"
	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/postgresversions"
	"xata/services/clusters"
	"xata/services/projects/api/spec"
	"xata/services/projects/cells"
	"xata/services/projects/metrics"
	"xata/services/projects/scheduler"
	"xata/services/projects/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	clustersv1 "xata/gen/proto/clusters/v1"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	FallbackInstanceType         = "custom"
	DefaultBackupRetentionPeriod = 2 // days
	MinRetentionPeriod           = 2
	MaxRetentionPeriod           = 35
	DefaultBackupFrequency       = "weekly"

	backupTimestampLayout = "2006-01-02 15:04:05 -0700 MST"
)

type Permission int

const (
	All Permission = iota
	OnlyEnabled
)

var (
	DefaultMaxInstances = 5
	DefaultMinInstances = 1

	// maxDateRange is the maximum date range for metrics queries
	maxDateRange = 6 * 30 * 24 * time.Hour // 6 months
	// we are still receiving this from the FE when loading our custom image
	// TODO remove once UI no longer uses it
	validImage = "postgresql:17"

	// internalExtensions are extensions that should not be exposed in the API
	internalExtensions = []string{"xatautils"}
)

type handler struct {
	store     store.ProjectsStore
	cells     cells.Cells
	feat      openfeature.Client
	sched     *scheduler.Scheduler
	analytics analytics.Client

	// defaultGatewayHostPort is the host:port of the gateway service, used to build connection strings
	defaultGatewayHostPort string

	// metricsClient is the legacy SigNoz-backed metrics client.
	metricsClient metrics.Client

	// cellsMetricsClient routes branch metric/log queries to the per-cell
	// observability backend via the clusters gRPC service. Selected per
	// request by start time, or by the observabilityBackendHeader override.
	cellsMetricsClient metrics.Client

	// postgresConfigProvider is the provider for PostgreSQL configuration operations
	postgresConfigProvider postgrescfg.PostgresConfigProvider

	// imageProvider is the provider for the PostgreSQL images
	imageProvider postgresversions.ImageProvider
}

func NewAPIHandler(feat openfeature.Client, store store.ProjectsStore, cells cells.Cells, gatewayHostPort string, metricsClient metrics.Client, cellsMetricsClient metrics.Client, scheduler *scheduler.Scheduler, analytics analytics.Client, postgresConfigProvider postgrescfg.PostgresConfigProvider, imageProvider postgresversions.ImageProvider) spec.ServerInterface {
	return &handler{
		feat:                   feat,
		store:                  store,
		cells:                  cells,
		defaultGatewayHostPort: gatewayHostPort,
		metricsClient:          metricsClient,
		cellsMetricsClient:     cellsMetricsClient,
		sched:                  scheduler,
		analytics:              analytics,
		postgresConfigProvider: postgresConfigProvider,
		imageProvider:          imageProvider,
	}
}

// observabilityBackendHeader is an internal, undocumented HTTP header the
// console can set to override per-request backend routing. It is
// intentionally absent from the OpenAPI spec. Recognised values: "signoz",
// "victoria". Honoured only when the BranchObservabilityPerCell feature flag
// is enabled for the org; otherwise the header is ignored and the backend is
// chosen automatically by request time range.
const observabilityBackendHeader = "X-Xata-Observability-Backend"

const (
	observabilityBackendSigNoz   = "signoz"
	observabilityBackendVictoria = "victoria"
)

// vmDataAvailableSince is the earliest timestamp for which the per-cell
// VictoriaMetrics/VictoriaLogs backend has data. Queries whose start time
// pre-dates this cutoff fall back to SigNoz, which retains the full history.
// Temporary — remove once the VM retention window covers the SigNoz one.
var vmDataAvailableSince = time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)

// maxMetricsPerRequest bounds the per-request fan-out to the backend (one
// HTTP/PromQL call per metric). It matches the `metrics` maxItems in the
// OpenAPI spec, but is enforced here because requests are not validated
// against the spec at runtime.
const maxMetricsPerRequest = 15

// selectMetricsClient returns the metrics client for the current request.
// By default it picks automatically by time range: the per-cell VM backend
// for ranges within its retention window, SigNoz otherwise (it keeps the full
// history). The BranchObservabilityPerCell flag enables the
// observabilityBackendHeader override, letting the console force a specific
// backend for debugging and side-by-side comparison.
func (s *handler) selectMetricsClient(c echo.Context, start time.Time) metrics.Client {
	if s.feat.BoolValue(c.Request().Context(), flags.BranchObservabilityPerCell) {
		switch strings.ToLower(c.Request().Header.Get(observabilityBackendHeader)) {
		case observabilityBackendSigNoz:
			return s.metricsClient
		case observabilityBackendVictoria:
			return s.cellsMetricsClient
		}
	}
	if start.Before(vmDataAvailableSince) {
		return s.metricsClient
	}
	return s.cellsMetricsClient
}

// Get list of regions available for the organization
// (GET /organizations/{organizationID}/regions)
func (s *handler) ListRegions(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		regions, err := s.store.ListRegions(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Regions []store.Region `json:"regions"`
		}{regions})
	})
}

// Get list of images available for the organization
// (GET /organizations/{organizationID}/images)
func (s *handler) ListImages(c echo.Context, organizationID spec.OrganizationID, params spec.ListImagesParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), organizationID, *params.Region)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
				}
				return err
			}
		}

		images := s.imageProvider.GetAllImageNames()

		// Filter out experimental images if the feature flag is not enabled
		experimentalEnabled := s.feat.BoolValue(c.Request().Context(), flags.ExperimentalImages)
		if !experimentalEnabled {
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				if !strings.HasPrefix(img, "experimental:") {
					filtered = append(filtered, img)
				}
			}
			images = filtered
		}

		// Filter out analytics images if the feature flag is not enabled
		analyticsEnabled := s.feat.BoolValue(c.Request().Context(), flags.AnalyticsImages)
		if !analyticsEnabled {
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				if !strings.HasPrefix(img, "analytics:") {
					filtered = append(filtered, img)
				}
			}
			images = filtered
		}

		imagesResp := make([]spec.Image, len(images))
		for i, it := range images {
			version := s.imageProvider.ExtractVersionFromImageName(it)
			imagesResp[i] = spec.Image{
				MajorVersion: s.imageProvider.GetMajorForVersion(version),
				FullVersion:  version,
				Name:         it,
			}
		}
		return c.JSON(http.StatusOK, struct {
			Images []spec.Image `json:"images"`
		}{imagesResp})
	})
}

// Get list of extensions available for the image
// (GET /organizations/{organizationID}/extensions)
func (s *handler) ListExtensions(c echo.Context, organizationID spec.OrganizationID, params spec.ListExtensionsParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), organizationID, *params.Region)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
				}
				return err
			}
		}

		err := s.imageProvider.ValidateImage(params.Image)
		if err != nil {
			return ErrorInvalidParam{Param: "image", Message: err.Error()}
		}

		exts := extensions.GetExtensions(params.Image)
		if exts == nil {
			return ErrorInvalidParam{Param: "image", Message: "no extensions found for image"}
		}

		extensionsResp := make([]spec.Extension, 0, len(exts))
		for _, ext := range exts {
			if slices.Contains(internalExtensions, ext.Name) {
				continue
			}
			extensionsResp = append(extensionsResp, spec.Extension{
				Name:            ext.Name,
				Version:         ext.Version,
				Description:     ext.Description,
				Docs:            ext.DocsURL,
				PreloadRequired: ext.PreloadRequired,
				Type:            spec.ExtensionType(ext.Type),
			})
		}

		return c.JSON(http.StatusOK, struct {
			Extensions []spec.Extension `json:"extensions"`
		}{extensionsResp})
	})
}

// Get list of instance types available for the organization
// (GET /organizations/{organizationID}/instanceTypes)
func (s *handler) ListInstanceTypes(c echo.Context, organizationID spec.OrganizationID, params spec.ListInstanceTypesParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.validateRegion(c.Request().Context(), organizationID, params.Region)
		if err != nil {
			return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
		}

		instanceTypes, err := s.store.ListInstanceTypes(c.Request().Context(), organizationID, params.Region)
		if err != nil {
			return err
		}

		type instanceType struct {
			Name               string  `json:"name"`
			VCPUs              int     `json:"vcpus"` // requested. This is an integer in milli-CPUs / millicores. E.g "500" -> 0.5 vCPU
			RAM                int     `json:"ram"`   // in GB
			HourlyRate         float64 `json:"hourlyRate"`
			StorageMonthlyRate float64 `json:"storageMonthlyRate"`
			// for now we have the same instance types in all regions, but this may not always be the case
			Region string `json:"region"`
		}

		instanceTypesResp := make([]instanceType, len(instanceTypes))
		for i, it := range instanceTypes {
			instanceTypesResp[i] = instanceType{
				Name:               it.Name,
				VCPUs:              it.VCPUsRequest,
				RAM:                it.RAM,
				HourlyRate:         it.HourlyRate,
				StorageMonthlyRate: it.StorageMonthlyRate,
				Region:             it.Region,
			}
		}
		return c.JSON(http.StatusOK, struct {
			InstanceTypes []instanceType `json:"instanceTypes"`
		}{instanceTypesResp})
	})
}

// List all projects
// (GET /organizations/{organizationID}/projects)
func (s *handler) ListProjects(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		projects, err := s.store.ListProjects(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		claims := api.GetUserClaims(c)
		filtered := make([]store.Project, 0, len(projects))
		for _, p := range projects {
			if claims.HasAccessToProject(p.ID) {
				filtered = append(filtered, p)
			}
		}

		return c.JSON(http.StatusOK, struct {
			Projects []spec.Project `json:"projects"`
		}{storeToAPIProjectList(filtered)})
	})
}

// Create a new project
// (POST /organizations/{organizationID}/projects)
func (s *handler) CreateProject(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var req spec.CreateProjectJSONBody
		err := c.Bind(&req)
		if err != nil {
			return err
		}

		createdProject, err := s.store.CreateProject(c.Request().Context(), organizationID, apiToStoreCreateProjectConfig(req))
		if err != nil {
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewProjectCreatedEvent(string(organizationID), createdProject.ID))

		return c.JSON(http.StatusCreated, storeToAPIProject(createdProject))
	})
}

// Delete a project by ID
// (DELETE /organizations/{organizationID}/projects/{projectID})
func (s *handler) DeleteProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.store.DeleteProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewProjectDeletedEvent(string(organizationID), projectID))

		return c.NoContent(http.StatusNoContent)
	})
}

// Get a project by ID
// (GET /organizations/{organizationID}/projects/{projectID})
func (s *handler) GetProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		project, err := s.store.GetProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIProject(project))
	})
}

// List project backups
// (GET /organizations/{organizationID}/projects/{projectID}/backups)
func (s *handler) ListBackups(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return echo.NewHTTPError(http.StatusNotImplemented, "listing backups is not implemented")
}

// Get a backup by ID
// (GET /organizations/{organizationID}/projects/{projectID}/backups/{backupID})
func (s *handler) GetBackup(c echo.Context, organizationID spec.OrganizationID, projectID, backupID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		// backupID is the same as branchID, so we fetch the branch to get cell info
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, backupID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBackupNotFound{ID: backupID}
			}
			return err
		}
		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		status, err := client.GetObjectStore(c.Request().Context(), &clustersv1.GetObjectStoreRequest{
			Id: branch.ID,
		})
		if err != nil {
			return err
		}

		earliestRestore, latestRestore, err := parseRestoreTimes(status, backupID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, spec.BackupMetadata{
			Id:              backupID,
			BranchID:        branch.ID,
			EarliestRestore: earliestRestore,
			LatestRestore:   latestRestore,
			Description:     "Continuous backup for branch " + branch.ID,
		})
	})
}

// Update a project by ID
// (PATCH /organizations/{organizationID}/projects/{projectID})
func (s *handler) UpdateProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateProjectJSONBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if body.Name == nil && body.Configuration == nil {
			return ErrorInvalidParam{ProjectID: projectID, Param: "all", Message: "at least one of the request fields needs to be set"}
		}

		ctx := c.Request().Context()
		updateConfig := apiToStoreUpdateProjectConfig(body)

		// If IP filtering is being updated, acquire project lock to prevent race conditions
		// with branch creation, then apply changes to primary cells before saving to DB
		if updateConfig.IPFiltering != nil {
			releaseLock, err := s.store.AcquireProjectLock(ctx, projectID)
			if err != nil {
				return fmt.Errorf("failed to acquire project lock: %w", err)
			}
			defer releaseLock()

			if err := s.applyIPFilteringToPrimaryCells(ctx, organizationID, projectID, updateConfig.IPFiltering); err != nil {
				return err
			}
		}

		project, err := s.store.UpdateProject(ctx, organizationID, projectID, updateConfig)
		if err != nil {
			return err
		}

		var changedFields []string
		newValues := map[string]any{}
		if body.Name != nil {
			changedFields = append(changedFields, "name")
			newValues["name"] = *body.Name
		}
		if updateConfig.IPFiltering != nil {
			changedFields = append(changedFields, "ip_filtering")
			newValues["ip_filtering_enabled"] = updateConfig.IPFiltering.Enabled
		}
		s.analytics.Track(ctx, events.NewProjectUpdatedEvent(string(organizationID), projectID, changedFields, newValues))

		return c.JSON(http.StatusOK, storeToAPIProject(project))
	})
}

// applyIPFilteringToPrimaryCells applies IP filtering settings to the primary cell of each branch's region
// before saving to the DB. Returns an error if any call fails.
func (s *handler) applyIPFilteringToPrimaryCells(ctx context.Context, organizationID string, projectID string, ipFiltering *store.IPFiltering) error {
	// Get all branches for the project
	branches, err := s.store.ListBranches(ctx, organizationID, projectID)
	if err != nil {
		return fmt.Errorf("listing branches: %w", err)
	}

	if len(branches) == 0 {
		// No branches to update, nothing to do
		return nil
	}

	// Group branches by region
	regionToBranches := make(map[string][]string)
	for _, branch := range branches {
		regionID := branch.Region
		if regionID == "" {
			return fmt.Errorf("branch %s has no region", branch.ID)
		}
		regionToBranches[regionID] = append(regionToBranches[regionID], branch.ID)
	}

	if len(regionToBranches) == 0 {
		return fmt.Errorf("no valid regions found for branches")
	}

	regionToCellClient := make(map[string]cells.CellClient)

	// Clean up cell connections when done
	defer func() {
		for _, client := range regionToCellClient {
			if client != nil {
				client.Close()
			}
		}
	}()

	// Get primary cell and connection for each unique region
	for regionID := range regionToBranches {
		primaryCell, err := s.store.GetPrimaryCell(ctx, organizationID, regionID)
		if err != nil {
			return fmt.Errorf("getting primary cell for region %s: %w", regionID, err)
		}

		cellClient, err := s.cells.GetCellConnection(ctx, organizationID, primaryCell.ID)
		if err != nil {
			return fmt.Errorf("connecting to primary cell %s for region %s: %w", primaryCell.ID, regionID, err)
		}

		regionToCellClient[regionID] = cellClient
	}

	ipFilteringConfig := &clustersv1.IPFilteringConfig{
		Enabled: ipFiltering.Enabled,
		Allowed: ipFiltering.CIDRStrings(),
	}

	// Apply IP filtering to all branches in each region with a single call per region
	for regionID, branchIDs := range regionToBranches {
		cellClient, exists := regionToCellClient[regionID]
		if !exists {
			return fmt.Errorf("no cell client found for region %s", regionID)
		}

		_, err := cellClient.SetBranchesIPFiltering(ctx, &clustersv1.SetBranchesIPFilteringRequest{
			BranchIds:   branchIDs,
			IpFiltering: ipFilteringConfig,
		})
		if err != nil {
			return fmt.Errorf("setting IP filtering for branches in region %s: %w", regionID, err)
		}
	}

	return nil
}

// List all branches of a project
// (GET /organizations/{organizationID}/projects/{projectID}/branches)
func (s *handler) ListBranches(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branches, err := s.store.ListBranches(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		claims := api.GetUserClaims(c)
		filtered := make([]store.Branch, 0, len(branches))
		for _, b := range branches {
			if claims.HasAccessToBranch(b.ID) {
				filtered = append(filtered, b)
			}
		}

		return c.JSON(http.StatusOK, struct {
			Branches []spec.BranchListMetadata `json:"branches"`
		}{storeToAPIListBranchListMetadata(filtered)})
	})
}

// Validate backup time format
func isValidBackupTimeFormat(s string) bool {
	// Regex pattern: ^(\*|[0-6]):(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$
	// 0-6-days of the week, * = daily
	s = strings.TrimSpace(s)
	pattern := regexp.MustCompile(`^(\*|[0-6]):(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$`)
	return pattern.MatchString(s)
}

type ValidatableCreateRequest interface {
	spec.CreateBranchJSONRequestBody | spec.RestoreFromBackupJSONRequestBody
}

// enforceProjectBranchLimit checks that the project hasn't reached its branch
// cap. Projects with the UseXatastor flag get a higher cap.
func (s *handler) enforceProjectBranchLimit(ctx context.Context, projectID string, useXatastor bool) error {
	limit := int64(store.MaxBranchesPerProject)
	if useXatastor {
		limit = int64(store.MaxBranchesPerProjectWithXatastor)
	}
	count, err := s.store.CountActiveProjectBranches(ctx, projectID)
	if err != nil {
		return fmt.Errorf("count active project branches: %w", err)
	}
	if count >= limit {
		return store.ErrTooManyBranches{ID: projectID}
	}
	return nil
}

func validateBranchRequestCommons[T ValidatableCreateRequest](body T) error {
	v := any(body)
	var desc *string
	var name string
	var backupConfig *spec.BackupConfiguration

	switch req := v.(type) {
	case spec.CreateBranchJSONRequestBody:
		desc = req.Description
		name = req.Name
		backupConfig = req.BackupConfiguration
	case spec.RestoreFromBackupJSONRequestBody:
		desc = req.Description
		name = req.Name
		backupConfig = req.BackupConfiguration
	}
	if desc != nil {
		err := IsBranchDescriptionValid(*desc)
		if err != nil {
			return err
		}
	}

	if name == "" {
		return ErrorInvalidParam{BranchName: name, Param: "name", Message: "branch name is required"}
	}

	return validateBackupConfiguration(name, backupConfig)
}

func validateBackupConfiguration(branchName string, c *spec.BackupConfiguration) error {
	if c == nil {
		return nil
	}
	if c.RetentionPeriod != nil && (*c.RetentionPeriod < MinRetentionPeriod || *c.RetentionPeriod > MaxRetentionPeriod) {
		return ErrorInvalidParam{BranchName: branchName, Param: "backup retentionPeriod", Message: fmt.Sprintf("must be at least %d days and maximum %d days", MinRetentionPeriod, MaxRetentionPeriod)}
	}
	if c.BackupTime != nil && !isValidBackupTimeFormat(*c.BackupTime) {
		return ErrorInvalidParam{BranchName: branchName, Param: "backup time", Message: fmt.Sprintf("invalid backup time format '%s', must match format 'D:HH:MM' where D= * or 0-6, HH=00-23, MM=00-59", *c.BackupTime)}
	}
	return nil
}

type ClusterServicePayload struct {
	ParentID       *string
	Configuration  clustersv1.ClusterConfiguration
	CellID         string
	Region         string
	BackupsEnabled bool
}

// Create a new branch
// (POST /organizations/{organizationID}/projects/{projectID}/branches)
func (s *handler) CreateBranch(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		// Check if branch creation is disabled (any type of branch)
		if s.feat.BoolValue(c.Request().Context(), flags.BranchCreationDisabled) {
			return ErrorBranchCreationDisabled{}
		}

		ctx := c.Request().Context()
		useXatastor := s.feat.BoolValue(ctx, flags.UseXatastor)

		claims := api.GetUserClaims(c)
		if claims != nil && !useXatastor {
			if org, ok := claims.Organizations[string(organizationID)]; ok && org.IsNewOrganization() {
				count, err := s.store.CountOrganizationBranches(ctx, string(organizationID))
				if err != nil {
					return fmt.Errorf("count organization branches: %w", err)
				}
				if count >= store.MaxNewOrgBranches {
					return ErrorNewOrgBranchLimitExceeded{OrganizationID: string(organizationID)}
				}
			}
		}

		var body spec.CreateBranchJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if err := validateBranchRequestCommons(body); err != nil {
			return err
		}

		value, err := body.ValueByDiscriminator()
		if err != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "body", Message: fmt.Sprintf("failed to parse branch creation details - %s", err.Error())}
		}

		var createClusterPayload ClusterServicePayload

		switch payload := value.(type) {
		// mode: inherit - we create a child branch
		case spec.BranchFromParent:
			// keeping feature flag separate from other checks for visibility
			if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
				return ErrorChildBranchCreationDisabled{}
			}
			createClusterPayload, err = s.handleBranchFromParent(ctx, organizationID, projectID, body.Name, payload)
			if err != nil {
				return err
			}
		// mode custom - we create a main branch
		case spec.BranchFromConfiguration:
			createClusterPayload, err = s.handleBranchFromConfiguration(ctx, organizationID, projectID, body.Name, payload)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported branch creation mode: %T", payload)

		}

		if !createClusterPayload.BackupsEnabled && body.BackupConfiguration != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
		}

		// we are now in possession of the following:
		// cellID
		// parentID (valid for child branch, nil for main branch
		// AND
		// (vcpu request and limit, memory, default postgres params, and image) for main branch
		// for child branch - we get them from the parent in the clusters service

		// Acquire project lock BEFORE reading project to prevent race conditions with IP filtering updates
		// This ensures we read the current IP filtering settings even if an update is in progress
		releaseLock, err := s.store.AcquireProjectLock(ctx, projectID)
		if err != nil {
			return fmt.Errorf("failed to acquire project lock: %w", err)
		}
		defer releaseLock()

		if err := s.enforceProjectBranchLimit(ctx, projectID, useXatastor); err != nil {
			return err
		}

		return s.withProject(c, organizationID, projectID, func(project *store.Project) error {
			branch, err := s.store.CreateBranch(ctx, organizationID, projectID, createClusterPayload.CellID, &store.CreateBranchConfiguration{
				Name:                  body.Name,
				ParentID:              createClusterPayload.ParentID,
				Description:           body.Description,
				BackupRetentionPeriod: apiToStoreBackupConfig(body.BackupConfiguration),
				BackupsEnabled:        createClusterPayload.BackupsEnabled,
			}, func(branch *store.Branch) error {
				scaleToZero := apiToClustersScaleToZero(body.ScaleToZero, createClusterPayload.ParentID, project)
				createClusterPayload.Configuration.ScaleToZero = scaleToZero
				request := clustersv1.CreatePostgresClusterRequest{
					Id:                  branch.ID,
					ParentId:            branch.ParentID,
					OrganizationId:      organizationID,
					ProjectId:           projectID,
					Configuration:       &createClusterPayload.Configuration,
					BackupConfiguration: apiToClustersBackupConfig(body.BackupConfiguration, createClusterPayload.BackupsEnabled),
				}
				if branch.ParentID != nil {
					request.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
						ClusterSnapshot: &clustersv1.ClusterSnapshot{
							ClusterId: *branch.ParentID,
						},
					}
				}
				usePool := s.feat.BoolValue(ctx, flags.UseClusterPool)
				log.Ctx(ctx).Info().Bool("usePool", usePool).Msg("cluster pool feature flag")
				if usePool {
					request.UsePool = proto.Bool(true)
				}

				if branch.ParentID == nil {
					useXatastor := s.feat.BoolValue(ctx, flags.UseXatastor)
					log.Ctx(ctx).Info().Bool("useXatastor", useXatastor).Msg("xatastor feature flag")
					if useXatastor {
						request.UseXatastor = proto.Bool(true)
					}
				}

				client, err := s.cells.GetCellConnection(ctx, organizationID, createClusterPayload.CellID)
				if err != nil {
					return err
				}
				defer client.Close()

				_, err = client.CreatePostgresCluster(ctx, &request)
				if err != nil {
					return err
				}

				return s.setupBranchOnPrimaryCell(ctx, organizationID, createClusterPayload.Region, createClusterPayload.CellID, branch.ID, project)
			})
			if err != nil {
				st, _ := status.FromError(err)
				if st.Code() == codes.NotFound && createClusterPayload.ParentID != nil {
					return ErrorBranchNotFound{BranchID: *createClusterPayload.ParentID}
				}
				if st.Code() == codes.InvalidArgument {
					return ErrorInvalidParam{BranchName: body.Name, Param: "configuration", Message: st.Message()}
				}
				if st.Code() == codes.FailedPrecondition && createClusterPayload.ParentID != nil {
					return ErrorParentBranchUnhealthy{ParentID: *createClusterPayload.ParentID}
				}
				return err
			}

			var analyticsEvent events.Event
			switch payload := value.(type) {
			case spec.BranchFromConfiguration:
				analyticsEvent = events.NewBranchFromConfigurationEvent(
					string(organizationID),
					projectID,
					branch.ID,
					branch.Region,
					string(payload.Configuration.Image),
					payload.Configuration.InstanceType,
					int(payload.Configuration.Replicas),
					payload.Configuration.Storage,
				)
			case spec.BranchFromParent:
				analyticsEvent = events.NewBranchFromParentEvent(string(organizationID), projectID, payload.ParentID, branch.ID, branch.Region)
			}
			s.analytics.Track(c.Request().Context(), analyticsEvent)

			// get the connection string
			// swallow the error, the resource got created and the connection string will be eventually available
			connString, _ := s.getConnectionString(c, organizationID, branch)
			return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(branch, connString))
		})
	})
}

func (s *handler) handleBranchFromParent(c context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromParent) (ClusterServicePayload, error) {
	if err := validateBranchFromParent(branchName, payload); err != nil {
		return ClusterServicePayload{}, err
	}
	return s.prepareCreateClusterFromParent(c, organizationID, projectID, payload)
}

func validateBranchFromParent(branchName string, payload spec.BranchFromParent) error {
	if payload.ParentID == "" {
		return ErrorInvalidParam{BranchName: branchName, Param: "parentID", Message: "parentId is required for 'inherit' mode"}
	}
	return nil
}

func (s *handler) prepareCreateClusterFromParent(ctx context.Context, organizationID spec.OrganizationID, projectID string, payload spec.BranchFromParent) (ClusterServicePayload, error) {
	// get the cell ID from the parent branch
	parentBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, payload.ParentID)
	if err != nil {
		if errors.As(err, &store.ErrBranchNotFound{}) {
			return ClusterServicePayload{}, ErrorBranchNotFound{BranchID: payload.ParentID}
		}
		return ClusterServicePayload{}, err
	}

	return ClusterServicePayload{
		ParentID: new(payload.ParentID),
		// If the parent branch ID is present, all settings (including vcpu, memory, and Postgres parameters) get copied from the
		// parent branch. This means we don't need to send them over, they just get copied locally in the cell.
		Configuration:  clustersv1.ClusterConfiguration{},
		CellID:         parentBranch.CellID,
		Region:         parentBranch.Region,
		BackupsEnabled: parentBranch.BackupsEnabled,
	}, nil
}

func (s *handler) handleBranchFromConfiguration(c context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromConfiguration) (ClusterServicePayload, error) {
	if err := s.validateBranchFromConfiguration(c, organizationID, branchName, payload); err != nil {
		return ClusterServicePayload{}, err
	}
	return s.prepareCreateClusterFromConfiguration(c, organizationID, projectID, branchName, payload)
}

func (s *handler) validateBranchFromConfiguration(ctx context.Context, organizationID spec.OrganizationID, name string, payload spec.BranchFromConfiguration) error {
	// validate payload
	if payload.Configuration == (spec.ClusterConfiguration{}) {
		return ErrorInvalidParam{BranchName: name, Param: "configuration", Message: "configuration is required for 'custom' mode"}
	}

	// validate replicas
	if payload.Configuration.Replicas < 0 {
		return ErrorInvalidParam{BranchName: name, Param: "configuration", Message: "number of replicas must be at least zero"}
	}

	return nil
}

func (s *handler) prepareCreateClusterFromConfiguration(ctx context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromConfiguration) (ClusterServicePayload, error) {
	// validate image - from this moment on, the image is in the correct format, no need for prefix, suffix, extra validation
	validImageFormat, err := s.validateImage(ctx, organizationID, payload.Configuration.Image)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid image: " + err.Error()}
	}

	region, err := s.store.GetRegion(ctx, organizationID, payload.Configuration.Region)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid region: " + err.Error()}
	}

	// allocate to a cell in the region
	cellID, err := s.allocateCell(ctx, organizationID, branchName, payload.Configuration.Region)
	if err != nil {
		return ClusterServicePayload{}, err
	}

	// extract the vcpu and memory from the instance type
	vcpuRequest, vcpuLimit, memory, err := s.getResourcesByInstanceType(ctx, organizationID, payload.Configuration.Region, payload.Configuration.InstanceType)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "instanceType", Message: err.Error()}
	}
	// Extract major version from image name
	majorVersion := postgresversions.ExtractMajorVersionFromImage(validImageFormat)

	// use configured preload libraries if provided, otherwise use defaults
	var preloadLibraries []string
	if payload.Configuration.PreloadLibraries != nil && len(*payload.Configuration.PreloadLibraries) > 0 {
		if err := s.postgresConfigProvider.ValidatePreloadLibraries(validImageFormat, *payload.Configuration.PreloadLibraries); err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "preloadLibraries", Message: err.Error()}
		}
		preloadLibraries = *payload.Configuration.PreloadLibraries
	} else {
		preloadLibraries, err = s.postgresConfigProvider.GetDefaultPreloadLibraries(validImageFormat)
		if err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "image", Message: fmt.Sprintf("failed to get default preload libraries: %v", err)}
		}
	}

	// validate configured postgres parameters if provided
	if payload.Configuration.PostgresConfigurationParameters != nil {
		errs, err := s.postgresConfigProvider.ValidateSettings(payload.Configuration.InstanceType, *payload.Configuration.PostgresConfigurationParameters, majorVersion, validImageFormat, preloadLibraries)
		if err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: fmt.Sprintf("validation failed: %v", err)}
		}
		if errs != nil {
			paramNames := slices.Sorted(maps.Keys(errs))
			var errorMessages []string
			for _, paramName := range paramNames {
				errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", paramName, errs[paramName].Error()))
			}
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: strings.Join(errorMessages, "; ")}
		}
	}

	// compute default Postgres parameters based on instance type, image, and preloaded extensions
	postgresParameters, err := s.postgresConfigProvider.GetDefaultPostgresParameters(payload.Configuration.InstanceType, majorVersion, validImageFormat, preloadLibraries)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "instanceType", Message: fmt.Sprintf("failed to compute Postgres parameters: %v", err)}
	}

	// merge configured postgres parameters if provided (they override defaults)
	if payload.Configuration.PostgresConfigurationParameters != nil {
		maps.Copy(postgresParameters, *payload.Configuration.PostgresConfigurationParameters)
	}

	numInstances := payload.Configuration.Replicas + 1 // the primary is always created

	// TODO storage size: we are currently not using the storage size sent from the API.
	// when/if we change that, we need to add it to this payload and to the created event below
	return ClusterServicePayload{
		ParentID: nil,
		Configuration: clustersv1.ClusterConfiguration{
			NumInstances:                    numInstances,
			ImageName:                       validImageFormat,
			VcpuRequest:                     vcpuRequest,
			VcpuLimit:                       vcpuLimit,
			Memory:                          memory,
			PostgresConfigurationParameters: postgresParameters,
			PreloadLibraries:                preloadLibraries,
		},
		CellID:         cellID,
		Region:         payload.Configuration.Region,
		BackupsEnabled: region.BackupsEnabled,
	}, nil
}

// formatCPUresource formats milliCPUs (millicores) into K8s resource spec
func formatCPUResource(milliCPUs int) string {
	if milliCPUs < 1000 {
		return fmt.Sprintf("%dm", milliCPUs)
	}
	return fmt.Sprintf("%d", milliCPUs/1000)
}

// parseCPUResource parses k8s cpu spec into milliCPUs
func parseCPUResource(cpuSpec string) (int, error) {
	quantity, err := resource.ParseQuantity(cpuSpec)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cpu resource: %w", err)
	}
	return int(quantity.MilliValue()), nil
}

// returns the vcpu and memory for the given instance type
func (s *handler) getResourcesByInstanceType(ctx context.Context, organizationID spec.OrganizationID, region string, name string) (cpuRequest string, cpuLimit string, memory string, err error) {
	instanceTypes, err := s.store.ListInstanceTypes(ctx, organizationID, region)
	if err != nil {
		return "", "", "", err
	}
	for _, instance := range instanceTypes {
		if instance.Name == name {
			return formatCPUResource(instance.VCPUsRequest), formatCPUResource(instance.VCPUsLimit), fmt.Sprintf("%dGi", instance.RAM), nil
		}
	}
	return "", "", "", fmt.Errorf("instance type %s is not found", name)
}

// returns the instance type for the pair vcpu, memory
func (s *handler) getInstanceTypeByResources(ctx context.Context, organizationID spec.OrganizationID, region string, cpuRequest string, cpuLimit string, memory string) (name string, err error) {
	instanceTypes, err := s.store.ListInstanceTypes(ctx, organizationID, region)
	if err != nil {
		return "", err
	}

	vcpusRequest, err := parseCPUResource(cpuRequest)
	if err != nil {
		return "", err
	}

	vcpusLimit, err := parseCPUResource(cpuLimit)
	if err != nil {
		return "", err
	}

	ram, err := strconv.Atoi(memory)
	if err != nil {
		return "", err
	}

	for _, instance := range instanceTypes {
		if instance.VCPUsRequest == vcpusRequest && instance.VCPUsLimit == vcpusLimit && instance.RAM == ram {
			return instance.Name, nil
		}
	}

	// for invalid combinations we will return the FallbackInstanceType defined as "custom" string as not to break the UI in the case we needed to amend the configs manually
	return FallbackInstanceType, nil
}

func (s *handler) validateRegion(ctx context.Context, organizationID spec.OrganizationID, region string) error {
	regions, err := s.store.ListRegions(ctx, organizationID)
	if err != nil {
		return err
	}

	for _, r := range regions {
		if region == r.ID {
			return nil
		}
	}
	return fmt.Errorf("region %s is not found", region)
}

func (s *handler) validateImage(ctx context.Context, organizationID spec.OrganizationID, image string) (string, error) {
	// Reject experimental images if the feature flag is not enabled
	if strings.HasPrefix(image, "experimental:") && !s.feat.BoolValue(ctx, flags.ExperimentalImages) {
		return "", fmt.Errorf("image %s is not available", image)
	}

	// Reject analytics images if the feature flag is not enabled
	if strings.HasPrefix(image, "analytics:") && !s.feat.BoolValue(ctx, flags.AnalyticsImages) {
		return "", fmt.Errorf("image %s is not available", image)
	}

	allValidImages := s.imageProvider.GetAllImageNames()
	// TODO once the UI starts sending valid responses, remove the validImages var
	// this is only for backward compat
	if image == validImage {
		return "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", nil
	}
	if slices.Contains(allValidImages, image) {
		imageURL := s.imageProvider.BuildImageURL(image)
		return imageURL, nil
	}
	return "", fmt.Errorf("image %s is not valid", image)
}

func (s *handler) validateImageUpgrade(ctx context.Context, organizationID spec.OrganizationID, newImage, currentImage string) (string, error) {
	// if the new image a valid one?
	newImageURL, err := s.validateImage(ctx, organizationID, newImage)
	if err != nil {
		return "", err
	}

	// make sure the offering is the same and that the minor is bigger than the current one
	newImageInfo, err := s.imageProvider.ParseImageVersion(newImageURL)
	if err != nil {
		return "", err
	}
	currentImageInfo, err := s.imageProvider.ParseImageVersion(currentImage)
	if err != nil {
		return "", err
	}

	if newImageInfo.Offering != currentImageInfo.Offering {
		return "", fmt.Errorf("incompatible offering: %s is not compatible with %s", newImageInfo.Offering, currentImageInfo.Offering)
	}
	if newImageInfo.Major != currentImageInfo.Major {
		return "", fmt.Errorf("no major version upgrades supported: %d is different than current %d", newImageInfo.Major, currentImageInfo.Major)
	}

	if newImageInfo.Minor < currentImageInfo.Minor {
		return "", fmt.Errorf("new minor: %d is older than current  %d", newImageInfo.Minor, currentImageInfo.Minor)
	}

	return newImageURL, nil
}

// allocateCell allocates a cell in the region
func (s *handler) allocateCell(ctx context.Context, organizationID spec.OrganizationID, branchName, regionID string) (string, error) {
	cells, err := s.store.ListCells(ctx, organizationID, regionID)
	if err != nil {
		return "", err
	}

	if len(cells) == 0 {
		return "", ErrorInvalidParam{BranchName: branchName, Param: "region", Message: "cannot allocate to given region"}
	}

	strategy := s.sched.StrategyForRegion(regionID)
	cell, err := strategy.Schedule(ctx, cells)
	if err != nil {
		return "", fmt.Errorf("failed to schedule branch %q: %w", branchName, err)
	}

	return cell.ID, nil
}

// Describe a new branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) DescribeBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		cluster, err := client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{
					BranchID: branchID,
				}
			}
			return err
		}

		// Extract major version from image name
		majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

		// filter out parameters that are not configurable, as they might contain internal information
		cluster.Configuration.PostgresConfigurationParameters = s.postgresConfigProvider.FilterConfigurableParameters(cluster.Configuration.PostgresConfigurationParameters, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		// filter out internal preload libraries
		cluster.Configuration.PreloadLibraries = postgrescfg.FilterOutInternalPreloadLibraries(cluster.Configuration.PreloadLibraries)

		// get the connection string, ignore errors as we may not have a connection string yet
		connString, err := s.getConnectionString(c, organizationID, branch)
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			log.Ctx(c.Request().Context()).
				Err(err).
				Str("branchID", branch.ID).
				Str("grpc.message", st.Message()).
				Msg("connection string not found")
		}

		// get instance type from resources
		instanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
		if err != nil {
			return fmt.Errorf("converting resources to instance type: %w", err)
		}

		return c.JSON(http.StatusOK, storeToAPIBranchMetadata(branch, connString, instanceType, cluster))
	})
}

func (s *handler) getConnectionString(c echo.Context, organizationID string, branch *store.Branch) (string, error) {
	// TODO I believe eventually this must be its own API call (ie, we may support several managed users in the future)
	client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
	if err != nil {
		return "", err
	}
	defer client.Close()

	// get gateway host:port from the region
	region, err := s.store.GetRegion(c.Request().Context(), organizationID, branch.Region)
	if err != nil {
		return "", err
	}

	hostPort := s.defaultGatewayHostPort
	if region.GatewayHostPort != "" {
		hostPort = region.GatewayHostPort
	}

	username := "app"
	// Admin kill-switch: disabling the flag falls back to the superuser DSN.
	if !s.feat.BoolValue(c.Request().Context(), flags.XataUser) {
		username = "superuser"
	}

	creds, err := client.GetPostgresClusterCredentials(c.Request().Context(), &clustersv1.GetPostgresClusterCredentialsRequest{
		Id:       branch.ID,
		Username: username,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("postgresql://%s:%s@%s.%s/xata?sslmode=require",
		creds.GetUsername(),
		creds.GetPassword(),
		branch.ID,
		hostPort), nil
}

// Get branch credentials
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/credentials)
func (s *handler) GetBranchCredentials(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string, params spec.GetBranchCredentialsParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Username == nil || *params.Username != "xata" {
			return ErrorInvalidParam{BranchName: branchID, Param: "username", Message: "only the xata user credentials can be retrieved"}
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		creds, err := client.GetPostgresClusterCredentials(c.Request().Context(), &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "app"})
		if err != nil {
			if errors.Is(err, clusters.SecretNotFoundForIDError(branch.ID)) {
				return ErrorCredentialsForBranchNotFound{BranchID: branch.ID, Username: "xata"}
			}
			return err
		}

		return c.JSON(http.StatusOK, spec.BranchCredentials{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		})
	})
}

// Rotate branch credentials
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/credentials/rotate)
func (s *handler) RotateBranchCredentials(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.RotateBranchCredentialsJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if body.Username != "xata" {
			return ErrorInvalidParam{BranchName: branchID, Param: "username", Message: "only the xata user credentials can be rotated"}
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		_, err = client.RotatePostgresClusterCredentials(c.Request().Context(), &clustersv1.RotatePostgresClusterCredentialsRequest{
			Id:   branch.ID,
			User: body.Username,
		})
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusNoContent)
	})
}

// Update a branch
// (PATCH /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) UpdateBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateBranchJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		// we need at least one parameter to perform the update
		if body.Description == nil && body.Name == nil && !hasClusterConfigChanged(&body) {
			return ErrorInvalidParam{BranchName: branchID, Param: "all", Message: fmt.Sprintf("branch [%s]: at least one of the request fields needs to be set", branchID)}
		}

		if body.Replicas != nil && *body.Replicas < 0 {
			return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: fmt.Sprintf("branch [%s]: cannot set number of replicas to less than 0", branchID)}
		}

		// CNPG rejects simultaneous image and configuration changes when
		// primaryUpdateMethod is set to "switchover". Instance type changes are
		// included because they auto-adjust postgres parameters. Preload libraries
		// are included because they are passed as postgres configuration parameters.
		if body.Image != nil && (body.PostgresConfigurationParameters != nil || body.InstanceType != nil || body.PreloadLibraries != nil) {
			return ErrorInvalidParam{BranchName: branchID, Param: "image", Message: "image cannot be updated together with postgres configuration parameters, instance type, or preload libraries"}
		}

		if err := validateBackupConfiguration(branchID, body.BackupConfiguration); err != nil {
			return err
		}

		branch, err := s.store.UpdateBranch(c.Request().Context(), organizationID, projectID, branchID, apiToStoreUpdateBranchConfig(body), func(branch *store.Branch) error {
			if !hasClusterConfigChanged(&body) {
				return nil
			}
			var config clustersv1.UpdateClusterConfiguration

			if body.Replicas != nil {
				config.NumInstances = new(*body.Replicas + 1)
			}
			if body.Storage != nil {
				config.StorageSize = body.Storage
			}
			if body.Hibernate != nil {
				config.Hibernate = body.Hibernate
			}
			if body.ScaleToZero != nil {
				config.ScaleToZero = apiToClustersScaleToZero(body.ScaleToZero, nil, nil)
			}
			// if the UI sends custom back we don't try to decode vcpu and memory
			if body.InstanceType != nil && *body.InstanceType != FallbackInstanceType {
				vcpuRequest, vcpuLimit, memory, err := s.getResourcesByInstanceType(c.Request().Context(), organizationID, branch.Region, *body.InstanceType)
				if err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: fmt.Sprintf("branch [%s]: unknown instance type %s", branchID, *body.InstanceType)}
				}
				config.VcpuRequest = &vcpuRequest
				config.VcpuLimit = &vcpuLimit
				config.Memory = &memory
			}

			client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
			if err != nil {
				return err
			}
			defer client.Close()

			// Fetch cluster info once if needed for any of the operations
			var cluster *clustersv1.DescribePostgresClusterResponse
			needsClusterInfo := (body.InstanceType != nil && *body.InstanceType != FallbackInstanceType) ||
				body.PostgresConfigurationParameters != nil ||
				body.PreloadLibraries != nil ||
				body.Image != nil
			if needsClusterInfo {
				cluster, err = client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
				if err != nil {
					st, _ := status.FromError(err)
					if st.Code() == codes.NotFound {
						return ErrorBranchNotFound{BranchID: branchID}
					}
					return err
				}
			}

			// Validate preload libraries against extensions available for this image
			if body.PreloadLibraries != nil {
				if err := s.postgresConfigProvider.ValidatePreloadLibraries(cluster.Configuration.ImageName, *body.PreloadLibraries); err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "preloadLibraries", Message: fmt.Sprintf("branch [%s]: %v", branchID, err)}
				}
			}

			// If instance type is changing, we need to update default settings
			if body.InstanceType != nil && *body.InstanceType != FallbackInstanceType {
				oldInstanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
				if err != nil {
					return fmt.Errorf("converting current resources (%s, %s, %s) to instance type: %w", cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory, err)
				}

				if oldInstanceType != *body.InstanceType {
					currentParams := cluster.Configuration.PostgresConfigurationParameters
					if currentParams == nil {
						currentParams = make(map[string]string)
					}

					// Extract major version from image name
					majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

					// Get parameter specifications for both old and new instance types
					oldSpecs, err := s.postgresConfigProvider.GetParametersSpec(oldInstanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
					if err != nil {
						return fmt.Errorf("failed to get parameter specifications for old instance type %s: %w", oldInstanceType, err)
					}

					newSpecs, err := s.postgresConfigProvider.GetParametersSpec(*body.InstanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
					if err != nil {
						return fmt.Errorf("failed to get parameter specifications for new instance type %s: %w", *body.InstanceType, err)
					}

					// Identify settings that are currently at their default values for the old instance type
					// and update them to the new instance type's defaults
					// For custom values, check if they're within the new instance type's min/max bounds
					updatedParams := make(map[string]string)
					for paramName, currentValue := range currentParams {
						oldSpec, oldExists := oldSpecs[paramName]
						newSpec, newExists := newSpecs[paramName]

						if !oldExists || !newExists {
							// Parameter doesn't exist in one of the specs, keep current value
							updatedParams[paramName] = currentValue
							continue
						}

						// Check if this parameter is at its default value for the old instance type
						if currentValue == oldSpec.DefaultValue {
							// This parameter is at its default value, update it to the new instance type's default
							updatedParams[paramName] = newSpec.DefaultValue
						} else {
							// This is a custom value, check if it's within the new instance type's bounds
							adjustedValue := currentValue

							// For numeric values, check min/max bounds
							if newSpec.MinValue != "" || newSpec.MaxValue != "" {
								adjustedValue = postgrescfg.AdjustValueToBounds(currentValue, newSpec)
							}

							updatedParams[paramName] = adjustedValue
						}
					}

					// Add any new default parameters that weren't in the current configuration
					for paramName, newSpec := range newSpecs {
						if _, exists := currentParams[paramName]; !exists {
							updatedParams[paramName] = newSpec.DefaultValue
						}
					}

					// Update the configuration with the new parameters
					config.PostgresConfigurationParameters = updatedParams
				}
			}

			if body.PostgresConfigurationParameters != nil {
				// find the instance type, because valid configuration depends on that
				instanceType := FallbackInstanceType
				if body.InstanceType != nil {
					body.InstanceType = &instanceType
				} else {
					// otherwise, find the instance type from the cluster (already fetched above)
					var err error
					instanceType, err = s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
					if err != nil {
						return fmt.Errorf("converting resources to instance type: %w", err)
					}
				}
				params := *body.PostgresConfigurationParameters

				// Extract major version from image name (cluster is already fetched above)
				majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

				// Use the new preload libraries if provided, otherwise use current
				preloadLibraries := cluster.Configuration.PreloadLibraries
				if body.PreloadLibraries != nil {
					preloadLibraries = *body.PreloadLibraries
				}

				errs, err := s.postgresConfigProvider.ValidateSettings(instanceType, params, majorVersion, cluster.Configuration.ImageName, preloadLibraries)
				if err != nil {
					return fmt.Errorf("invalid instance type %s: %w", instanceType, err)
				}
				if errs != nil {
					// Format validation errors into a user-friendly message
					var errorMessages []string
					for paramName, paramErr := range errs {
						errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", paramName, paramErr.Error()))
					}
					errorMessage := fmt.Sprintf("PostgreSQL configuration validation failed: %s", strings.Join(errorMessages, "; "))
					return ErrorInvalidParam{
						BranchName: branchID,
						Param:      "postgres_configuration_parameters",
						Message:    errorMessage,
					}
				}

				config.PostgresConfigurationParameters = params
			}

			// Handle preload libraries update
			if body.PreloadLibraries != nil {
				config.PreloadLibraries = append(postgrescfg.GetInternalPreloadLibraries(), *body.PreloadLibraries...) // add internal preload libraries to the list

				majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

				// Get current parameters to work with
				currentParams := config.PostgresConfigurationParameters
				if currentParams == nil {
					currentParams = cluster.Configuration.PostgresConfigurationParameters
				}
				if currentParams == nil {
					currentParams = make(map[string]string)
				}

				// Filter out parameters for extensions being removed from preload
				filteredParams := s.postgresConfigProvider.FilterConfigurableParameters(
					currentParams, majorVersion, cluster.Configuration.ImageName, config.PreloadLibraries)

				// Add default parameters for newly added extensions
				configurableParams := s.postgresConfigProvider.GetConfigurableParameters(majorVersion, cluster.Configuration.ImageName, config.PreloadLibraries)
				for paramName, spec := range configurableParams {
					// Only add extension parameters with a default value that don't already exist
					if spec.Extension != "" && spec.DefaultValue != "" {
						if _, exists := filteredParams[paramName]; !exists {
							filteredParams[paramName] = spec.DefaultValue
						}
					}
				}

				config.PostgresConfigurationParameters = filteredParams
			}

			// Handle backup configuration update
			if body.BackupConfiguration != nil {
				if !branch.BackupsEnabled {
					return ErrorInvalidParam{BranchName: branch.ID, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
				}
				backupConfig := &clustersv1.BackupConfiguration{
					BackupsEnabled: branch.BackupsEnabled,
				}
				if body.BackupConfiguration.RetentionPeriod != nil && *body.BackupConfiguration.RetentionPeriod != 0 {
					backupConfig.BackupRetention = fmt.Sprintf("%dd", *body.BackupConfiguration.RetentionPeriod)
				}
				if body.BackupConfiguration.BackupTime != nil && *body.BackupConfiguration.BackupTime != "" {
					backupConfig.BackupSchedule = generateCron(*body.BackupConfiguration.BackupTime)
				}
				config.BackupConfiguration = backupConfig
			}

			// Handle image minor version upgrades
			if body.Image != nil {
				// It can be argued that this should be decided by the operator. However - more than what
				// the operator supports - there should be a way for us to allow/disallow certain
				// upgrades - there can be business reasons for this and so it needs to happen here.
				imageURL, err := s.validateImageUpgrade(c.Request().Context(), organizationID, *body.Image, cluster.Configuration.ImageName)
				if err != nil {
					return ErrorInvalidParam{BranchName: branch.ID, Param: "image", Message: err.Error()}
				}
				config.ImageName = &imageURL
			}

			_, err = client.UpdatePostgresCluster(c.Request().Context(), &clustersv1.UpdatePostgresClusterRequest{
				Id:                  branch.ID,
				UpdateConfiguration: &config,
			})
			return err
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			if st.Code() == codes.InvalidArgument {
				return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: st.Message()}
			}
			if st.Code() == codes.PermissionDenied {
				return ErrorBranchUpdateForbidden{BranchID: branchID}
			}
			if st.Code() == codes.Aborted {
				return ErrorBranchConflict{BranchID: branchID}
			}
			return err
		}

		var changedFields []string
		newValues := map[string]any{}
		if body.Name != nil {
			changedFields = append(changedFields, "name")
			newValues["name"] = *body.Name
		}
		if body.Description != nil {
			changedFields = append(changedFields, "description")
			newValues["description"] = *body.Description
		}
		if body.InstanceType != nil {
			changedFields = append(changedFields, "instance_type")
			newValues["instance_type"] = *body.InstanceType
		}
		if body.Replicas != nil {
			changedFields = append(changedFields, "replicas")
			newValues["replicas"] = *body.Replicas
		}
		if body.Storage != nil {
			changedFields = append(changedFields, "storage")
			newValues["storage_gi"] = *body.Storage
		}
		if body.Hibernate != nil {
			changedFields = append(changedFields, "hibernate")
			newValues["hibernate"] = *body.Hibernate
		}
		if body.ScaleToZero != nil {
			changedFields = append(changedFields, "scale_to_zero")
			newValues["scale_to_zero"] = body.ScaleToZero
		}
		if body.PostgresConfigurationParameters != nil {
			changedFields = append(changedFields, "postgres_config")
			newValues["postgres_config"] = *body.PostgresConfigurationParameters
		}
		if body.PreloadLibraries != nil {
			changedFields = append(changedFields, "preload_libraries")
			newValues["preload_libraries"] = *body.PreloadLibraries
		}
		if body.BackupConfiguration != nil {
			changedFields = append(changedFields, "backup_config")
			newValues["backup_config"] = body.BackupConfiguration
		}
		s.analytics.Track(c.Request().Context(), events.NewBranchUpdatedEvent(string(organizationID), projectID, branchID, changedFields, newValues))

		// get the connection string
		// swallow the error, the resource got created and the connection string will be eventually available
		connString, err := s.getConnectionString(c, organizationID, branch)
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			log.Ctx(c.Request().Context()).
				Err(err).
				Str("branchID", branch.ID).
				Str("grpc.message", st.Message()).
				Msg("connection string not found")
		}
		return c.JSON(http.StatusOK, storeToAPIBranchShortMetadata(branch, connString))
	})
}

func hasClusterConfigChanged(body *spec.UpdateBranchJSONRequestBody) bool {
	return body.Storage != nil ||
		body.InstanceType != nil ||
		body.Replicas != nil ||
		body.Hibernate != nil ||
		body.ScaleToZero != nil ||
		body.PostgresConfigurationParameters != nil ||
		body.PreloadLibraries != nil ||
		body.BackupConfiguration != nil ||
		body.Image != nil
}

// Delete a branch
// (DELETE /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) DeleteBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		log.Ctx(c.Request().Context()).Log().Msgf("Deleting branch [%s]", branchID)
		err := s.store.DeleteBranch(c.Request().Context(), organizationID, projectID, branchID, func(branch *store.Branch) error {
			return cells.DeprovisionBranch(c.Request().Context(), organizationID, s.store, s.cells, branch)
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewBranchDeletedEvent(string(organizationID), projectID, branchID))

		return c.NoContent(http.StatusNoContent)
	})
}

// GetProjectLimits returns the effective resource limits for a project
// (GET /organizations/{organizationID}/projects/{projectID}/limits)
func (s *handler) GetProjectLimits(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		return s.withProject(c, organizationID, projectID, func(_ *store.Project) error {
			return echo.ErrNotImplemented
		})
	})
}

// GetDefaultProjectLimits returns the default project limits in the organization
// (GET /organizations/{organizationID}/projects/limits)
func (s *handler) GetDefaultProjectLimits(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		return c.JSON(http.StatusOK, spec.ProjectLimits{
			MaxDescriptionLength: MaxBranchDescriptionLength,
			MaxInstances:         DefaultMaxInstances,
			MinInstances:         DefaultMinInstances,
			MaxBranches:          store.MaxBranchesPerProject,
		})
	})
}

// GetOrganizationLimits returns the effective limits for an organization, applying
// tier defaults and any per-organization overrides stored in the DB.
// T1 organizations always receive tier defaults with no DB lookup.
// (GET /organizations/{organizationID}/limits)
func (s *handler) GetOrganizationLimits(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		claims := api.GetUserClaims(c)
		tier := orgTier(c.Request().Context(), claims.Organizations[organizationID].UsageTier)

		def := func(key store.LimitKey) int {
			return store.TierDefaultInt(tier, key, 0)
		}

		if tier == store.TierT1 {
			return c.JSON(http.StatusOK, spec.OrganizationLimits{
				MaxProjects:            def(store.LimitMaxProjects),
				MaxProjectsPerHour:     def(store.LimitMaxProjectsPerHour),
				MaxBranchesPerProject:  def(store.LimitMaxBranchesPerProject),
				MaxBranchesPerOrg:      def(store.LimitMaxBranchesPerOrg),
				MaxInstancesPerBranch:  def(store.LimitMaxInstancesPerBranch),
				MinInstancesPerBranch:  def(store.LimitMinInstancesPerBranch),
				MaxBranchesPerHour:     def(store.LimitMaxBranchesPerHour),
				MaxDescriptionLength:   def(store.LimitMaxDescriptionLength),
				MaxAllowedInstanceType: def(store.LimitMaxAllowedInstanceType),
			})
		}

		overrides, err := s.store.GetOrgLimits(c.Request().Context(), organizationID, "")
		if err != nil {
			return fmt.Errorf("get org limits: %w", err)
		}

		ctx := c.Request().Context()
		return c.JSON(http.StatusOK, spec.OrganizationLimits{
			MaxProjects:            resolveIntLimit(ctx, overrides, store.LimitMaxProjects, def(store.LimitMaxProjects)),
			MaxProjectsPerHour:     resolveIntLimit(ctx, overrides, store.LimitMaxProjectsPerHour, def(store.LimitMaxProjectsPerHour)),
			MaxBranchesPerProject:  resolveIntLimit(ctx, overrides, store.LimitMaxBranchesPerProject, def(store.LimitMaxBranchesPerProject)),
			MaxBranchesPerOrg:      resolveIntLimit(ctx, overrides, store.LimitMaxBranchesPerOrg, def(store.LimitMaxBranchesPerOrg)),
			MaxInstancesPerBranch:  resolveIntLimit(ctx, overrides, store.LimitMaxInstancesPerBranch, def(store.LimitMaxInstancesPerBranch)),
			MinInstancesPerBranch:  resolveIntLimit(ctx, overrides, store.LimitMinInstancesPerBranch, def(store.LimitMinInstancesPerBranch)),
			MaxBranchesPerHour:     resolveIntLimit(ctx, overrides, store.LimitMaxBranchesPerHour, def(store.LimitMaxBranchesPerHour)),
			MaxDescriptionLength:   resolveIntLimit(ctx, overrides, store.LimitMaxDescriptionLength, def(store.LimitMaxDescriptionLength)),
			MaxAllowedInstanceType: resolveIntLimit(ctx, overrides, store.LimitMaxAllowedInstanceType, def(store.LimitMaxAllowedInstanceType)),
		})
	})
}

// orgTier resolves the usage tier from a raw usage tier string, defaulting to
// TierT1 (more restrictive) for any unrecognized value.
func orgTier(ctx context.Context, usageTier string) store.UsageTier {
	tier, ok := store.ParseUsageTier(usageTier)
	if !ok {
		log.Ctx(ctx).Warn().Str("usage_tier", usageTier).Msg("unknown usage tier, defaulting to t1")
	}
	return tier
}

// resolveIntLimit returns the override value for key if present, otherwise def.
func resolveIntLimit(ctx context.Context, overrides map[store.LimitKey]any, key store.LimitKey, def int) int {
	v, ok := overrides[key]
	if !ok {
		return def
	}
	n, ok := v.(json.Number)
	if !ok {
		log.Ctx(ctx).Warn().Str("key", string(key)).Msgf("unexpected type %T for limit override, using default", v)
		return def
	}
	i, err := n.Int64()
	if err != nil {
		log.Ctx(ctx).Warn().Str("key", string(key)).Str("value", n.String()).Msg("limit override is not a valid int64, using default")
		return def
	}
	return int(i)
}

// BranchMetrics retrieves the branch metrics
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/metrics)
func (s *handler) BranchMetrics(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		var req spec.BranchMetricsRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		if len(req.Metrics) == 0 {
			return ErrorInvalidParam{BranchName: branchID, Param: "metrics", Message: "`metrics` must not be empty"}
		}
		metricNames := stringArrayValue(req.Metrics)

		if len(metricNames) > maxMetricsPerRequest {
			return ErrorInvalidParam{BranchName: branchID, Param: "metrics", Message: fmt.Sprintf("at most %d metrics may be requested", maxMetricsPerRequest)}
		}

		if err := validateTimeRange(branchID, req.Start, req.End); err != nil {
			return err
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		instances := ptr.Deref(req.Instances, nil)
		if err := s.validateBranchInstances(c.Request().Context(), organizationID, branch, instances); err != nil {
			return err
		}

		results, err := s.selectMetricsClient(c, req.Start).GetMetrics(c.Request().Context(), organizationID, branch.CellID, req.Start, req.End, branchID, metricNames, instances, stringArrayValue(req.Aggregations))
		if err != nil {
			return fmt.Errorf("get metrics for branch [%s]: %w", branchID, err)
		}
		if len(results) == 0 {
			return fmt.Errorf("get metrics for branch [%s]: backend returned no results", branchID)
		}

		specResults := make([]spec.BranchMetricResult, len(results))
		for i, r := range results {
			specResults[i] = spec.BranchMetricResult{
				Metric: r.Metric,
				Unit:   r.Unit,
				Series: toSpecSeries(r.Series),
			}
		}

		return c.JSON(http.StatusOK, spec.BranchMetrics{
			Start:   req.Start,
			End:     req.End,
			Results: specResults,
		})
	})
}

// BranchLogs retrieves the branch logs
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/logs)
func (s *handler) BranchLogs(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if !s.feat.BoolValue(c.Request().Context(), flags.BranchLogs) {
			return echo.NewHTTPError(http.StatusNotFound)
		}

		var req spec.BranchLogsRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		if err := validateTimeRange(branchID, req.Start, req.End); err != nil {
			return err
		}
		if err := validateLogsLimit(branchID, req.Limit); err != nil {
			return err
		}

		userFilters, err := validateLogFilters(branchID, ptr.Deref(req.Filters, nil))
		if err != nil {
			return err
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		logs, err := s.selectMetricsClient(c, req.Start).GetLogs(
			c.Request().Context(),
			organizationID,
			branch.CellID,
			req.Start,
			req.End,
			branchID,
			userFilters,
			ptr.Deref(req.Limit, DefaultLogLimit),
			ptr.Deref(req.Cursor, ""),
		)
		if err != nil {
			return fmt.Errorf("getting logs for branch [%s]: %w", branchID, err)
		}

		return c.JSON(http.StatusOK, logs)
	})
}

// Restore from backup
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/restore)
func (s *handler) RestoreFromBackup(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		// Check if branch creation is disabled (any type of branch)
		if s.feat.BoolValue(c.Request().Context(), flags.BranchCreationDisabled) {
			return ErrorBranchCreationDisabled{}
		}

		var body spec.RestoreFromBackupJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if err := validateBranchRequestCommons(body); err != nil {
			return err
		}

		var createClusterPayload ClusterServicePayload
		var err error
		ctx := c.Request().Context()

		useXatastor := s.feat.BoolValue(ctx, flags.UseXatastor)

		// restore must happen in the same cell where the backup exists
		// otherwise we don't have access to the source object store
		sourceBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		if body.Configuration == nil {
			// Inherit configuration from source branch
			if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
				return ErrorChildBranchCreationDisabled{}
			}
			createClusterPayload, err = s.handleBranchFromParent(ctx, organizationID, projectID, body.Name, spec.BranchFromParent{
				Mode:     spec.Inherit,
				ParentID: branchID,
			})
			if err != nil {
				return err
			}
		} else {
			// Use source branch's region if not specified, otherwise validate it matches
			if body.Configuration.Region == "" {
				body.Configuration.Region = sourceBranch.Region
			} else if body.Configuration.Region != sourceBranch.Region {
				return ErrorInvalidParam{BranchName: body.Name, Param: "region", Message: "restore must be in the same region as the source branch"}
			}
			// Use provided configuration
			createClusterPayload, err = s.handleBranchFromConfiguration(ctx, organizationID, projectID, body.Name, spec.BranchFromConfiguration{
				Mode:          spec.BranchFromConfigurationModeCustom,
				Configuration: *body.Configuration,
			})
			if err != nil {
				return err
			}
			// Use source branch's cell - the backup only exists there
			createClusterPayload.CellID = sourceBranch.CellID
			// Mark source branch as parent - this will always be the case
			createClusterPayload.ParentID = &branchID
		}

		if !createClusterPayload.BackupsEnabled && body.BackupConfiguration != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
		}

		releaseLock, err := s.store.AcquireProjectLock(ctx, projectID)
		if err != nil {
			return fmt.Errorf("failed to acquire project lock: %w", err)
		}
		defer releaseLock()

		if err := s.enforceProjectBranchLimit(ctx, projectID, useXatastor); err != nil {
			return err
		}

		return s.withProject(c, organizationID, projectID, func(project *store.Project) error {
			branch, err := s.store.CreateBranch(ctx, organizationID, projectID, createClusterPayload.CellID, &store.CreateBranchConfiguration{
				Name:                  body.Name,
				ParentID:              createClusterPayload.ParentID,
				Description:           body.Description,
				BackupRetentionPeriod: apiToStoreBackupConfig(body.BackupConfiguration),
				BackupsEnabled:        createClusterPayload.BackupsEnabled,
			}, func(branch *store.Branch) error {
				scaleToZero := apiToClustersScaleToZero(body.ScaleToZero, createClusterPayload.ParentID, project)
				createClusterPayload.Configuration.ScaleToZero = scaleToZero
				request := clustersv1.CreatePostgresClusterRequest{
					Id:             branch.ID,
					OrganizationId: organizationID,
					ProjectId:      projectID,
					ParentId:       branch.ParentID,
					Configuration:  &createClusterPayload.Configuration,
					DataSource: &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
						ContinuousBackup: &clustersv1.ContinuousBackup{
							ClusterId: branchID, // the source branch ID
						},
					},
					BackupConfiguration: apiToClustersBackupConfig(body.BackupConfiguration, createClusterPayload.BackupsEnabled),
				}

				client, err := s.cells.GetCellConnection(ctx, organizationID, createClusterPayload.CellID)
				if err != nil {
					return err
				}
				defer client.Close()

				_, err = client.CreatePostgresCluster(ctx, &request)
				if err != nil {
					return err
				}

				return s.setupBranchOnPrimaryCell(ctx, organizationID, createClusterPayload.Region, createClusterPayload.CellID, branch.ID, project)
			})
			if err != nil {
				st, _ := status.FromError(err)
				if st.Code() == codes.NotFound {
					return ErrorBranchNotFound{BranchID: branchID}
				}
				if st.Code() == codes.InvalidArgument {
					return ErrorInvalidParam{BranchName: body.Name, Param: "configuration", Message: st.Message()}
				}
				return err
			}

			s.analytics.Track(ctx, events.NewBranchRestoredFromBackupEvent(string(organizationID), projectID, branchID, branch.ID))

			// swallow the error, the resource got created and the connection string will be eventually available
			connString, _ := s.getConnectionString(c, organizationID, branch)
			return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(branch, connString))
		})
	})
}

func stringArrayValue[T ~string](v []T) []string {
	if len(v) == 0 {
		return nil
	}
	strs := make([]string, len(v))
	for i, s := range v {
		strs[i] = string(s)
	}
	return strs
}

// toSpecSeries: spec.MetricSeries.Values is an inline anonymous struct, so this can't be a cast.
func toSpecSeries(in []metrics.MetricSeries) []spec.MetricSeries {
	out := make([]spec.MetricSeries, len(in))
	for i, s := range in {
		values := make([]struct {
			Timestamp time.Time `json:"timestamp"`
			Value     float32   `json:"value"`
		}, len(s.Values))
		for j, v := range s.Values {
			values[j].Timestamp = v.Timestamp
			values[j].Value = v.Value
		}
		out[i] = spec.MetricSeries{
			Aggregation: spec.MetricSeriesAggregation(s.Aggregation),
			InstanceID:  s.InstanceID,
			Values:      values,
		}
	}
	return out
}

func validateTimeRange(branchID string, start, end time.Time) error {
	if end.Before(start) {
		return ErrorInvalidParam{BranchName: branchID, Param: "start", Message: "start time must come before end time"}
	}

	if end.Sub(start) > maxDateRange {
		return ErrorInvalidParam{BranchName: branchID, Param: "end", Message: "maximum date range is " + maxDateRange.String()}
	}

	return nil
}

// validateBranchInstances rejects instance names that don't belong to the
// branch's cluster. Pool clusters carry pod names that don't share the
// branchID prefix, so the check is against the actual cluster status keys
// rather than a string prefix.
func (s *handler) validateBranchInstances(ctx context.Context, organizationID spec.OrganizationID, branch *store.Branch, instances []string) error {
	if len(instances) == 0 {
		return nil
	}

	client, err := s.cells.GetCellConnection(ctx, organizationID, branch.CellID)
	if err != nil {
		return err
	}
	defer client.Close()

	cluster, err := client.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			return ErrorBranchNotFound{BranchID: branch.ID}
		}
		return err
	}

	valid := cluster.GetStatus().GetInstances()
	for _, inst := range instances {
		if _, ok := valid[inst]; !ok {
			return ErrorInvalidParam{BranchName: branch.ID, Param: "instances", Message: fmt.Sprintf("unknown instance [%s]", inst)}
		}
	}
	return nil
}

// setupBranchOnPrimaryCell registers a cluster with the primary cell if it was
// created on a secondary cell, and applies IP filtering settings from the project.
func (s *handler) setupBranchOnPrimaryCell(ctx context.Context, organizationID spec.OrganizationID, region, cellID, branchID string, project *store.Project) error {
	primaryCell, err := s.store.GetPrimaryCell(ctx, organizationID, region)
	if err != nil {
		return err
	}

	hasIPFiltering := project.IPFiltering.Enabled || len(project.IPFiltering.CIDRs) > 0
	needsRegistration := primaryCell.ID != cellID

	if !hasIPFiltering && !needsRegistration {
		return nil
	}

	client, err := s.cells.GetCellConnection(ctx, organizationID, primaryCell.ID)
	if err != nil {
		return err
	}
	defer client.Close()

	if hasIPFiltering {
		_, err = client.SetBranchIPFiltering(ctx, &clustersv1.SetBranchIPFilteringRequest{
			BranchId: branchID,
			IpFiltering: &clustersv1.IPFilteringConfig{
				Enabled: project.IPFiltering.Enabled,
				Allowed: project.IPFiltering.CIDRStrings(),
			},
		})
		if err != nil {
			return fmt.Errorf("setting IP filtering for branch: %w", err)
		}
	}

	if needsRegistration {
		_, err = client.RegisterPostgresCluster(ctx, &clustersv1.RegisterPostgresClusterRequest{Id: branchID})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *handler) withOrganizationAccess(c echo.Context, organizationID spec.OrganizationID, p Permission, fn func() error) error {
	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	if !claims.HasAccessToOrganization(organizationID) {
		return api.ErrorAuthorizationFailed{Reason: fmt.Sprintf("no access to organization [%s]", organizationID)}
	}
	if p == OnlyEnabled && !claims.IsEnabledOrganization(organizationID) {
		return ErrorOrganizationDisabled{organizationID}
	}

	o11y.SetReqAttribute(c, api.OrganizationO11yK, organizationID)
	o11y.SetReqAttribute(c, api.UserIDO11yK, claims.UserID())

	return fn()
}

func (s *handler) withProject(c echo.Context, organizationID spec.OrganizationID, projectID string, fn func(project *store.Project) error) error {
	project, err := s.store.GetProject(c.Request().Context(), organizationID, projectID)
	if err != nil {
		return err
	}

	return fn(project)
}

// GetBranchPostgresConfig retrieves detailed information about PostgreSQL configuration parameters for a branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/postgres-config)
func (s *handler) GetBranchPostgresConfig(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		cluster, err := client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{
					BranchID: branchID,
				}
			}
			return err
		}

		instanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
		if err != nil {
			return fmt.Errorf("converting resources to instance type: %w", err)
		}

		majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

		configurableParams := s.postgresConfigProvider.GetConfigurableParameters(majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		instanceDefaults, err := s.postgresConfigProvider.GetParametersSpec(instanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
		if err != nil {
			return fmt.Errorf("getting instance defaults: %w", err)
		}

		// Merge configurable parameters with instance-specific defaults
		mergedParams := postgrescfg.MergeParametersMaps(configurableParams, instanceDefaults)

		// Convert to API format using helper function
		parameters := postgresConfigToAPIParameters(mergedParams, instanceType, cluster.Configuration.PostgresConfigurationParameters, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		return c.JSON(http.StatusOK, spec.PostgresConfigDetails{
			Parameters: parameters,
		})
	})
}

// (POST /organizations/{organizationID}/githubapp/installations)
func (s *handler) CreateGithubAppInstallation(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.CreateGithubAppInstallationJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		inst, err := s.store.CreateGithubInstallation(c.Request().Context(), organizationID, body.InstallationId)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, storeToAPIGithubInstallation(inst))
	})
}

// (GET /organizations/{organizationID}/githubapp/installations)
func (s *handler) ListGithubAppInstallations(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		installations, err := s.store.ListGithubInstallations(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Installations []spec.GithubInstallation `json:"installations"`
		}{storeToAPIGithubInstallationList(installations)})
	})
}

// (PUT /organizations/{organizationID}/githubapp/installations/{githubInstallationID})
func (s *handler) UpdateGithubAppInstallation(c echo.Context, organizationID spec.OrganizationID, githubInstallationID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateGithubAppInstallationJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		inst, err := s.store.UpdateGithubInstallation(c.Request().Context(), organizationID, githubInstallationID, body.InstallationId)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIGithubInstallation(inst))
	})
}

// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) GetGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		mapping, err := s.store.GetGithubRepoMappingByProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]any{"mapping": storeToAPIGithubRepository(mapping)})
	})
}

// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) CreateGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.CreateGithubRepositoryJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}
		if body.GithubRepositoryID <= 0 {
			return ErrorInvalidParam{Param: "githubRepositoryID", Message: "must be greater than 0"}
		}
		if strings.TrimSpace(branchID) == "" {
			return ErrorInvalidParam{Param: "branchID", Message: "must not be empty"}
		}

		mapping, err := s.store.CreateGithubRepoMapping(c.Request().Context(), organizationID, projectID, body.GithubRepositoryID, branchID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, storeToAPIGithubRepository(mapping))
	})
}

// (PUT /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) UpdateGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateGithubRepositoryJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}
		if body.GithubRepositoryID <= 0 {
			return ErrorInvalidParam{Param: "githubRepositoryID", Message: "must be greater than 0"}
		}
		if strings.TrimSpace(branchID) == "" {
			return ErrorInvalidParam{Param: "branchID", Message: "must not be empty"}
		}

		mapping, err := s.store.UpdateGithubRepoMapping(c.Request().Context(), organizationID, projectID, body.GithubRepositoryID, branchID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIGithubRepository(mapping))
	})
}

// (DELETE /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) DeleteGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.store.DeleteGithubRepoMapping(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusNoContent)
	})
}

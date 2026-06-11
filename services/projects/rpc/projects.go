package rpc

import (
	"context"
	"fmt"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/projects/cells"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/services/projects/store"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ensure clusters implements GRPCService interface.
var _ projectsv1.ProjectsServiceServer = (*ProjectsService)(nil)

// ProjectsService is a GRPC service for interacting with projects service.
type ProjectsService struct {
	// fail to compile if the service does not implement all the methods
	projectsv1.UnsafeProjectsServiceServer

	store store.ProjectsStore
	cells cells.Cells
}

// NewProjectsService creates a new ProjectsService.
func NewProjectsService(store store.ProjectsStore, cells cells.Cells) *ProjectsService {
	return &ProjectsService{
		store: store,
		cells: cells,
	}
}

// CreateCell implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) CreateCell(ctx context.Context, input *projectsv1.CreateCellRequest) (*projectsv1.CreateCellResponse, error) {
	_, err := p.store.CreateCell(ctx, input.GetRegionId(), input.GetId(), input.GetClustersGrpcUrl(), input.GetIsPrimary())
	return &projectsv1.CreateCellResponse{}, err
}

// CreateRegion implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) CreateRegion(ctx context.Context, input *projectsv1.CreateRegionRequest) (*projectsv1.CreateRegionResponse, error) {
	provider, err := store.ParseProvider(input.GetProvider())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	flags := store.RegionFlags{
		PublicAccess:   input.GetPublicAccess(),
		BackupsEnabled: input.GetBackupsEnabled(),
		Provider:       provider,
	}
	if input.OrganizationId != nil {
		_, err = p.store.CreateOrganizationRegion(ctx, input.GetOrganizationId(), input.GetId(), flags, input.GetHostport())
	} else {
		_, err = p.store.CreateRegion(ctx, input.GetId(), flags, input.GetHostport())
	}

	return &projectsv1.CreateRegionResponse{}, err
}

// ListCells implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) ListCells(ctx context.Context, _ *projectsv1.ListCellsRequest) (*projectsv1.ListCellsResponse, error) {
	cells, err := p.store.ListAllCells(ctx)
	if err != nil {
		return nil, err
	}

	response := &projectsv1.ListCellsResponse{}
	for _, cell := range cells {
		response.Cells = append(response.Cells, &projectsv1.Cell{
			Id:              cell.ID,
			RegionId:        cell.RegionID,
			ClustersGrpcUrl: cell.ClustersGRPCURL,
			IsPrimary:       cell.Primary,
		})
	}
	return response, nil
}

// ListRegions implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) ListRegions(ctx context.Context, _ *projectsv1.ListRegionsRequest) (*projectsv1.ListRegionsResponse, error) {
	regions, err := p.store.ListAllRegions(ctx)
	if err != nil {
		return nil, err
	}

	response := &projectsv1.ListRegionsResponse{}
	for _, region := range regions {
		response.Regions = append(response.Regions, &projectsv1.Region{
			Id:             region.ID,
			PublicAccess:   region.PublicAccess,
			OrganizationId: region.OrganizationID,
			BackupsEnabled: region.BackupsEnabled,
			Provider:       string(region.Provider),
		})
	}
	return response, nil
}

// ValidateHierarchy implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) ValidateHierarchy(ctx context.Context, req *projectsv1.ValidateHierarchyRequest) (*projectsv1.ValidateHierarchyResponse, error) {
	err := p.store.ValidateHierarchy(ctx, req.GetOrganizationIds(), req.GetProjectIds(), req.GetBranchIds())
	if err != nil {
		return nil, err
	}

	return &projectsv1.ValidateHierarchyResponse{}, nil
}

// HasActiveProjects implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) HasActiveProjects(ctx context.Context, req *projectsv1.HasActiveProjectsRequest) (*projectsv1.HasActiveProjectsResponse, error) {
	projects, err := p.store.ListProjects(ctx, req.GetOrganizationId())
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	return &projectsv1.HasActiveProjectsResponse{HasActiveProjects: len(projects) > 0}, nil
}

// UpdateOrganizationStatus implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) UpdateOrganizationStatus(ctx context.Context, req *projectsv1.UpdateOrganizationStatusRequest) (*projectsv1.UpdateOrganizationStatusResponse, error) {
	projects, err := p.store.ListProjects(ctx, req.OrganizationId)
	if err != nil {
		return nil, err
	}

	connMap := make(map[string]cells.CellClient)
	defer func() {
		for k, conn := range connMap {
			if err := conn.Close(); err != nil {
				log.Err(err).Msgf("Failed to close cell [%s] connection", k)
			}
		}
	}()
	for i := range projects {
		branches, err := p.store.ListBranches(ctx, req.OrganizationId, projects[i].ID)
		if err != nil {
			return nil, err
		}
		for i2 := range branches {
			branch := branches[i2]

			if _, ok := connMap[branch.CellID]; !ok {
				conn, err := p.cells.GetCellConnection(ctx, req.OrganizationId, branch.CellID)
				if err != nil {
					log.Err(err).Msgf("Failed to get cell [%s] connection for branch [%s]", branch.CellID, branch.ID)
					continue
				}
				connMap[branch.CellID] = conn
			}

			client := connMap[branch.CellID]
			cluster, err := client.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
				Id: branch.ID,
			})
			if err != nil {
				log.Err(err).Msgf("Failed to describe cluster [%s]", branch.ID)
				continue
			}
			shouldUpdate := false
			hibernate := req.Disabled
			// Only update if there is a change
			configuration := clustersv1.UpdateClusterConfiguration{}
			if hibernate != (cluster.Status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED) {
				shouldUpdate = true
				configuration.Hibernate = new(hibernate)
			}

			// Toggle S2Z only for branches that already had it configured.
			// DescribePostgresCluster always returns a non-nil ScaleToZero proto
			// (defaulting to {Enabled:false, InactivityPeriodMinutes:0} for
			// branches without CRD config), so the nil check is purely defensive.
			// InactivityPeriodMinutes > 0 is the real guard: the CRD enforces
			// minimum=1, so 0 means the branch was never configured for S2Z.
			if cluster.Configuration.ScaleToZero != nil {
				desiredEnabled := !hibernate && cluster.Configuration.ScaleToZero.InactivityPeriodMinutes > 0
				if cluster.Configuration.ScaleToZero.Enabled != desiredEnabled {
					shouldUpdate = true
					configuration.ScaleToZero = &clustersv1.ScaleToZero{
						Enabled:                 desiredEnabled,
						InactivityPeriodMinutes: cluster.Configuration.ScaleToZero.InactivityPeriodMinutes,
					}
				}
			}

			if shouldUpdate {
				log.Info().Msgf("Updating cluster [%s] configuration", branch.ID)
				_, err = client.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id:                  branch.ID,
					UpdateConfiguration: &configuration,
				})
				if err != nil {
					log.Err(err).Msgf("Failed to update Postgres cluster for branch [%s]", branch.ID)
					continue
				}
			}
		}
	}

	return &projectsv1.UpdateOrganizationStatusResponse{OrganizationId: req.OrganizationId}, nil
}

// DeleteProjectsInOrg implements projectsv1.ProjectsServiceServer.
func (p *ProjectsService) DeleteProjectsInOrg(ctx context.Context, req *projectsv1.DeleteProjectsInOrgRequest) (*projectsv1.DeleteProjectsInOrgResponse, error) {
	projects, err := p.store.ListProjects(ctx, req.OrganizationId)
	if err != nil {
		return nil, err
	}

	response := &projectsv1.DeleteProjectsInOrgResponse{
		OrganizationId: req.OrganizationId,
	}

	for _, project := range projects {
		var projectErrors []string
		branches, err := p.store.ListBranches(ctx, req.OrganizationId, project.ID)
		if err != nil {
			response.Errors = append(response.Errors, fmt.Sprintf("list branches for project %s: %v", project.ID, err))
			continue
		}

		for _, branch := range branches {
			err := p.store.DeleteBranch(ctx, req.OrganizationId, project.ID, branch.ID, func(b *store.Branch) error {
				return cells.DeprovisionBranch(ctx, req.OrganizationId, p.store, p.cells, b)
			})
			if err != nil {
				projectErrors = append(projectErrors, fmt.Sprintf("delete branch %s: %v", branch.ID, err))
				continue
			}
			response.BranchesDeleted++
			log.Info().Msgf("Deleted branch [%s] in project [%s] for org [%s]", branch.ID, project.ID, req.OrganizationId)
		}

		if projectErrors == nil {
			if err := p.store.DeleteProject(ctx, req.OrganizationId, project.ID); err != nil {
				projectErrors = append(projectErrors, fmt.Sprintf("delete project %s: %v", project.ID, err))
			} else {
				response.ProjectsDeleted++
				log.Info().Msgf("Deleted project [%s] for org [%s]", project.ID, req.OrganizationId)
			}
		}
		response.Errors = append(response.Errors, projectErrors...)
	}

	return response, nil
}

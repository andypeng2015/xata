package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

const (
	MaxBranchesPerProject             = 100
	MaxBranchesPerProjectWithXatastor = 1000
	MaxNewOrgBranches                 = 10
	DefaultRegion                     = "us-east-1"
	BackupTypeContinuous              = "continuous"
)

// LimitKey identifies a project-level limit stored in the projects service.
type LimitKey string

const (
	LimitMaxDescriptionLength   LimitKey = "max_description_length"
	LimitMaxBranchesPerProject  LimitKey = "max_branches_per_project"
	LimitMaxInstancesPerBranch  LimitKey = "max_instances_per_branch"
	LimitMinInstancesPerBranch  LimitKey = "min_instances_per_branch"
	LimitMaxStorageGBPerBranch  LimitKey = "max_storage_gb_per_branch"
	LimitMaxAllowedInstanceType LimitKey = "max_allowed_instance_type"
	LimitMaxBranchesPerHour     LimitKey = "max_branches_per_hour"
	LimitMaxProjects            LimitKey = "max_projects"
	LimitMaxProjectsPerHour     LimitKey = "max_projects_per_hour"
)

func (k LimitKey) IsValid() bool {
	switch k {
	case LimitMaxDescriptionLength, LimitMaxBranchesPerProject, LimitMaxInstancesPerBranch,
		LimitMinInstancesPerBranch, LimitMaxStorageGBPerBranch, LimitMaxAllowedInstanceType,
		LimitMaxBranchesPerHour, LimitMaxProjects, LimitMaxProjectsPerHour:
		return true
	}
	return false
}

type Project struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	CreatedAt   time.Time          `json:"createdAt"`
	UpdatedAt   time.Time          `json:"updatedAt"`
	ScaleToZero ProjectScaleToZero `json:"scaleToZero"`
	IPFiltering IPFiltering        `json:"ipFiltering"`
}

type UpdateProjectConfiguration struct {
	Name *string `json:"name,omitempty"`
	// ScaleToZero indicates if the project should scale to zero when not in use
	ScaleToZero *ProjectScaleToZero `json:"scaleToZero,omitempty"`
	// IPFiltering indicates IP filtering configuration for the project
	IPFiltering *IPFiltering `json:"ipFiltering,omitempty"`
}

type CreateProjectConfiguration struct {
	Name        string             `json:"name"`
	ScaleToZero ProjectScaleToZero `json:"scaleToZero"`
	IPFiltering IPFiltering        `json:"ipFiltering"`
}

type ProjectScaleToZero struct {
	BaseBranches  ScaleToZero `json:"baseBranches"`
	ChildBranches ScaleToZero `json:"childBranches"`
}

type ScaleToZero struct {
	// Enabled indicates if the scale to zero feature is enabled for the branch
	Enabled bool `json:"enabled"`
	// Duration after which the branch should be scaled to zero if not in use
	InactivityPeriod InactivityPeriod `json:"inactivityPeriod"`
}

type InactivityPeriod time.Duration

type CIDREntry struct {
	CIDR        string `json:"cidr"`
	Description string `json:"description,omitempty"`
}

type IPFiltering struct {
	// Enabled indicates if IP filtering is enabled
	Enabled bool `json:"enabled"`
	// CIDRs is a list of CIDR entries allowed to access the project branches
	CIDRs []CIDREntry `json:"cidrs"`
}

// RegionFlags contains configuration flags for a region
type RegionFlags struct {
	// PublicAccess indicates if the region has SQL access from outside the data plane (e.g. from the frontend app)
	PublicAccess bool `json:"publicAccess"`
	// BackupsEnabled indicates if backups are enabled for branches created in this region
	BackupsEnabled bool `json:"backupsEnabled"`
}

type Region struct {
	ID string `json:"id"`
	// PublicAccess indicates if the region has SQL access from outside the data plane (e.g. from the frontend app)
	PublicAccess bool `json:"publicAccess"`
	// BackupsEnabled indicates if backups are enabled for branches created in this region
	BackupsEnabled bool `json:"backupsEnabled"`

	// GatewayHostPort is the host of the gateway service in the region, used to build connection strings
	GatewayHostPort string `json:"-"`

	CreatedAt time.Time `json:"-"`

	// If set, the region is only available to the organization
	OrganizationID *string `json:"organizationId"`
}

type Cell struct {
	ID        string
	RegionID  string
	CreatedAt time.Time
	Primary   bool

	ClustersGRPCURL string
}

type Branch struct {
	ID          string    `json:"id"`
	ParentID    *string   `json:"parentID"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	Region      string    `json:"region"`

	// PublicAccess indicates if the branch has SQL access from outside the data plane (e.g. from the frontend app)
	PublicAccess bool `json:"publicAccess"`

	// BackupsEnabled indicates if backups are enabled for the region
	BackupsEnabled bool `json:"backupsEnabled"`

	// Depth is the depth of the branch in the tree (nil branch has depth=0, first branch depth=1). No json mapping since we don't want to disclose it so far
	Depth int32 `json:"-"`

	CellID string `json:"_"`
}

type Backup struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	BranchID    string `json:"branchID"`
	ProjectID   string `json:"projectID"`

	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	BranchCreatedAt time.Time `json:"branchCreatedAt"`

	CellID string `json:"cellID"`

	RetentionPeriod int32 `json:"retentionPeriod"`
	Orphan          bool  `json:"orphan"`
}

type UpdateBranchConfiguration struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type CreateBranchConfiguration struct {
	Name string `json:"branchName"`
	// ParentID is the ID of the parent branch, if any. If nil, the branch is a base branch.
	ParentID *string `json:"parentID,omitempty"`
	// Description is an optional description of the branch
	Description *string `json:"description,omitempty"`
	// BackupRetentionPeriod this needs to be kept on a per branch basis
	BackupRetentionPeriod int `json:"backupRetentionPeriod"`
	// BackupsEnabled indicates whether backups should be created for this branch
	BackupsEnabled bool `json:"backupsEnabled"`
}

type InstanceType struct {
	Name               string  `json:"name"`
	VCPUsRequest       int     `json:"vcpus_request"` // requested. This is an integer in milli-CPUs / millicores. E.g "500" -> 0.5 vCPU
	VCPUsLimit         int     `json:"vcpus_limit"`   // limit. This is an integer in milli-CPUs / millicores. E.g "500" -> 0.5 vCPU
	RAM                int     `json:"ram"`           // in GB
	HourlyRate         float64 `json:"hourlyRate"`
	StorageMonthlyRate float64 `json:"storageMonthlyRate"`

	// for now we have the same instance types in all regions, but this may not always be the case
	Region string `json:"region"`
}

// InstanceTypes hardcoded for now, they will live in our metadata CP DB
// Source: https://www.notion.so/xata/Instance-sizes-23fe9e30407180258c5fcaa349deb2e0
var InstanceTypes = []InstanceType{
	{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1, HourlyRate: 0.012, StorageMonthlyRate: 0.3},
	{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2, HourlyRate: 0.024, StorageMonthlyRate: 0.3},
	{Name: "xata.medium", VCPUsRequest: 1000, VCPUsLimit: 2000, RAM: 4, HourlyRate: 0.048, StorageMonthlyRate: 0.3},
	{Name: "xata.large", VCPUsRequest: 2000, VCPUsLimit: 4000, RAM: 8, HourlyRate: 0.096, StorageMonthlyRate: 0.3},
	{Name: "xata.xlarge", VCPUsRequest: 4000, VCPUsLimit: 8000, RAM: 16, HourlyRate: 0.192, StorageMonthlyRate: 0.3},
	{Name: "xata.2xlarge", VCPUsRequest: 8000, VCPUsLimit: 12000, RAM: 32, HourlyRate: 0.384, StorageMonthlyRate: 0.3},
	{Name: "xata.4xlarge", VCPUsRequest: 16000, VCPUsLimit: 24000, RAM: 64, HourlyRate: 0.768, StorageMonthlyRate: 0.3},
	{Name: "xata.8xlarge", VCPUsRequest: 32000, VCPUsLimit: 48000, RAM: 128, HourlyRate: 1.536, StorageMonthlyRate: 0.3},
}

var RegionMultiplier = map[string]float64{
	"us-east-1":    1.0,
	"eu-central-1": 1.15,
}

type GithubInstallation struct {
	ID             string `json:"id"`
	InstallationID int64  `json:"installationId"`
	// Note: this is a Xata organization and not a GitHub organization
	Organization string    `json:"organization"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type GithubRepoMapping struct {
	ID                 string    `json:"id"`
	GithubRepositoryID int64     `json:"repositoryId"`
	Project            string    `json:"projectId"`
	RootBranchID       string    `json:"rootBranchId"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type GithubRepoMappingWithOrg struct {
	GithubRepoMapping
	OrganizationID string `json:"organizationId"`
}

//go:generate go run github.com/vektra/mockery/v3 --with-expecter --name ProjectsStore

// ProjectsStore stores information about projects
type ProjectsStore interface {
	// Setup runs DB migrations for the store
	Setup(ctx context.Context) error

	// Close closes the store
	Close(ctx context.Context) error

	// ListRegions returns a list of regions in the organization
	ListRegions(ctx context.Context, organizationID string) ([]Region, error)

	// ListAllRegions returns a list of all existing regions
	ListAllRegions(ctx context.Context) ([]Region, error)

	// CreateRegion creates a new region
	CreateRegion(ctx context.Context, regionID string, flags RegionFlags, hostport string) (*Region, error)

	// GetRegion returns a region by id
	// organizationID is used to filter the cells by organization
	GetRegion(ctx context.Context, organizationID, regionID string) (*Region, error)

	// CreateOrganizationRegion creates a new region only available to the organization
	CreateOrganizationRegion(ctx context.Context, organizationID, regionID string, flags RegionFlags, hostport string) (*Region, error)

	// DeleteRegion deletes a region
	// It is not possible to delete a region if there are cells in the region
	DeleteRegion(ctx context.Context, regionID string) error

	// ListCells returns a list of cells in the region
	// organizationID is used to filter the cells by organization
	ListCells(ctx context.Context, organizationID, regionID string) ([]Cell, error)

	// ListAllCells returns a list of all existing cells (across all regions)
	ListAllCells(ctx context.Context) ([]Cell, error)

	// CreateCell creates a new cell in the region
	CreateCell(ctx context.Context, regionID, cellID, grpcURL string, isPrimary bool) (*Cell, error)

	// DeleteCell deletes a cell in the region
	// It is not possible to delete a cell if there are branches in the cell
	DeleteCell(ctx context.Context, cellID string) error

	// GetCell returns a cell by id
	// organizationID is used to filter the cells by organization
	GetCell(ctx context.Context, organizationID, cellID string) (*Cell, error)

	// GetPrimaryCell returns the primary cell in the region
	// organizationID is used to filter the cells by organization
	GetPrimaryCell(ctx context.Context, organizationID, regionID string) (*Cell, error)

	// ListProjects returns a list of projects in the organization
	ListProjects(ctx context.Context, organizationID string) ([]Project, error)

	// CreateProject creates a new project in the organization
	CreateProject(ctx context.Context, organizationID string, config *CreateProjectConfiguration) (*Project, error)

	// DeleteProject soft-deletes a project from the organization
	DeleteProject(ctx context.Context, organizationID, projectID string) error

	// GetProject returns a project from the organization
	GetProject(ctx context.Context, organizationID, projectID string) (*Project, error)

	// UpdateProject updates the project configuration in the sql store
	UpdateProject(ctx context.Context, organizationID, projectID string, config *UpdateProjectConfiguration) (*Project, error)

	// CreateBranch creates a new branch in the project. provisionFn is called during the create transaction
	// and should be used to provision the branch in the underlying system
	CreateBranch(ctx context.Context, organizationID, projectID, cellID string, config *CreateBranchConfiguration, provisionFn func(b *Branch) error) (*Branch, error)

	// DeleteBranch soft-deletes a branch in the project. deprovisionFn is called during the delete transaction
	// and should be used to deprovision the branch in the underlying system
	DeleteBranch(ctx context.Context, organizationID, projectID, branchID string, deprovisionFn func(b *Branch) error) error

	// DescribeBranch returns the branch information stored in the sql layer
	DescribeBranch(ctx context.Context, organizationID, projectID, branchID string) (*Branch, error)

	// GetBranchByName returns the active branch with the given name in the project.
	// Returns ErrBranchNotFound if no active branch with that name exists.
	GetBranchByName(ctx context.Context, organizationID, projectID, name string) (*Branch, error)

	// UpdateBranch can only update the branch name or description in the sql store.
	// It can update cluster parameters in the cnpg connector via the updateFn
	UpdateBranch(ctx context.Context, organizationID, projectID, branchID string, config *UpdateBranchConfiguration, updateFn func(b *Branch) error) (*Branch, error)

	// ListBranches returns a list of branches in the project
	ListBranches(ctx context.Context, organizationID string, projectID string) ([]Branch, error)

	// ListInstanceTypes returns a list of instance types in the organization and region
	ListInstanceTypes(ctx context.Context, organizationID string, region string) ([]InstanceType, error)

	// ValidateHierarchy validates that the provided organization IDs, project IDs, and branch IDs form a valid hierarchy
	ValidateHierarchy(ctx context.Context, organizationIds []string, projectIds []string, branchIds []string) error

	// CountOrganizationBranches counts all branches ever created for an organization (including soft-deleted ones)
	CountOrganizationBranches(ctx context.Context, organizationID string) (int64, error)

	// CountActiveProjectBranches counts active (non-deleted) branches in a project.
	CountActiveProjectBranches(ctx context.Context, projectID string) (int64, error)

	// AcquireProjectLock acquires a PostgreSQL advisory lock for the given projectID.
	// Returns a release function that must be called to release the lock. The release function
	// should be called in a defer to ensure the lock is released even on errors.
	// Returns an error if the lock cannot be acquired (e.g., timeout).
	// The lock is held on a dedicated connection that is closed when the release function is called.
	AcquireProjectLock(ctx context.Context, projectID string) (release func() error, err error)
	// CleanupTerminatedBranches hard-deletes terminated branches in a cell that have been terminated for at least the given duration
	CleanupTerminatedBranches(ctx context.Context, cellID string, terminatedFor time.Duration) (int64, error)

	// CleanupTerminatedProjects hard-deletes terminated projects that have been terminated for at least the given duration
	CleanupTerminatedProjects(ctx context.Context, terminatedFor time.Duration) (int64, error)

	CreateGithubInstallation(ctx context.Context, organization string, installationID int64) (*GithubInstallation, error)

	ListGithubInstallations(ctx context.Context, organization string) ([]GithubInstallation, error)

	UpdateGithubInstallation(ctx context.Context, organization, id string, installationID int64) (*GithubInstallation, error)

	CreateGithubRepoMapping(ctx context.Context, organization, project string, repoID int64, rootBranchID string) (*GithubRepoMapping, error)

	GetGithubRepoMappingByProject(ctx context.Context, organization, project string) (*GithubRepoMapping, error)

	GetGithubRepoMappingByRepoID(ctx context.Context, repoID int64) (*GithubRepoMappingWithOrg, error)

	UpdateGithubRepoMapping(ctx context.Context, organization, project string, repoID int64, rootBranchID string) (*GithubRepoMapping, error)

	DeleteGithubRepoMapping(ctx context.Context, organization, project string) error

	DeleteGithubInstallation(ctx context.Context, installationID int64) error

	// Org/project limit operations
	GetOrgLimits(ctx context.Context, orgID, projectID string) (map[LimitKey]any, error)
	SetOrgLimit(ctx context.Context, orgID, projectID string, key LimitKey, value any) error
	DeleteOrgLimit(ctx context.Context, orgID, projectID string, key LimitKey) error
}

// CanAddChild returns the child depth in the branch tree if another child branch can be added
func (b *Branch) CanAddChild(currentChildren, childBranchMaxChildren, maxDepth int32) (int32, error) {
	if b == nil {
		return 1, nil
	}
	if b.Depth == 1 {
		// First generation branches have no child limit
		return 2, nil
	}
	if b.Depth >= maxDepth {
		return 0, ErrMaxDepthExceeded{BranchID: b.ID, MaxDepth: maxDepth}
	}
	if currentChildren >= childBranchMaxChildren {
		return 0, ErrMaxChildrenExceeded{BranchID: b.ID, MaxChildren: childBranchMaxChildren}
	}

	return b.Depth + 1, nil
}

func (s *ProjectScaleToZero) GetBaseEnabled() *bool {
	if s == nil {
		return nil
	}
	return new(s.BaseBranches.Enabled)
}

func (s *ProjectScaleToZero) GetChildEnabled() *bool {
	if s == nil {
		return nil
	}
	return new(s.ChildBranches.Enabled)
}

func (s *ProjectScaleToZero) GetBaseInactivityPeriod() *InactivityPeriod {
	if s == nil || s.BaseBranches.InactivityPeriod == 0 {
		return nil
	}
	return new(s.BaseBranches.InactivityPeriod)
}

func (s *ProjectScaleToZero) GetChildInactivityPeriod() *InactivityPeriod {
	if s == nil || s.ChildBranches.InactivityPeriod == 0 {
		return nil
	}
	return new(s.ChildBranches.InactivityPeriod)
}

func (s *ScaleToZero) GetEnabled() *bool {
	if s == nil {
		return nil
	}
	return new(s.Enabled)
}

func (s *ScaleToZero) GetInactivityPeriod() *InactivityPeriod {
	if s == nil || s.InactivityPeriod == 0 {
		return nil
	}
	return new(s.InactivityPeriod)
}

func (p *InactivityPeriod) Duration() time.Duration {
	if p == nil {
		return 0
	}
	return time.Duration(*p)
}

// Make the InactivityPeriod type implement the driver.Valuer interface. It
// stores the inactivity period as minutes in the database to improve
// readability vs nanoseconds which is the default for time.Duration.
func (p InactivityPeriod) Value() (driver.Value, error) {
	return int64(time.Duration(p).Minutes()), nil
}

// Make the InactivityPeriod type implement the driver.Scanner interface. It
// reads the inactivity period as minutes from the database and converts it
// to a time.Duration.
func (p *InactivityPeriod) Scan(value any) error {
	if value == nil {
		return nil // nil value is valid, no error
	}
	minutes, ok := value.(int64)
	if !ok {
		return errors.New("type assertion to int64 failed")
	}
	// Convert minutes to time.Duration
	*p = InactivityPeriod(time.Duration(minutes) * time.Minute)
	return nil
}

func (i *IPFiltering) GetEnabled() *bool {
	if i == nil {
		return nil
	}
	return new(i.Enabled)
}

func (i *IPFiltering) GetCIDRs() *[]CIDREntry {
	if i == nil || i.CIDRs == nil {
		return nil
	}
	return &i.CIDRs
}

func (i *IPFiltering) CIDRStrings() []string {
	if i == nil {
		return nil
	}
	strs := make([]string, len(i.CIDRs))
	for idx, entry := range i.CIDRs {
		strs[idx] = entry.CIDR
	}
	return strs
}

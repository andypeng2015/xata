package sqlstore

import (
	"context"
	"testing"
	"time"

	"xata/services/projects/store"

	"github.com/stretchr/testify/require"
)

func TestSQLStoreRegions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)

	testRegionID := "test-region"
	testOrganizationRegionID := "test-organization-region"
	testCellID := "test-cell"
	testOrganizationCellID := "test-organization-cell"
	testOrganizationID := "test-organization"
	testGRPCURL := "grpc://localhost:50051"
	isPrimaryCell := true

	// no regions
	regions, err := sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.Empty(t, regions)

	// region available to everyone
	region, err := sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, "custom.domain.tld:1234")
	require.NoError(t, err)
	require.Equal(t, testRegionID, region.ID)
	require.Equal(t, (*string)(nil), region.OrganizationID)
	require.Equal(t, true, region.PublicAccess)
	require.Equal(t, true, region.BackupsEnabled)
	require.Equal(t, store.ProviderAWS, region.Provider)
	require.Equal(t, "custom.domain.tld:1234", region.GatewayHostPort)

	gotRegion, err := sqlStore.GetRegion(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.Equal(t, region, gotRegion)

	regions, err = sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region})

	// region available to organization only
	orgRegion, err := sqlStore.CreateOrganizationRegion(ctx, testOrganizationID, testOrganizationRegionID, store.RegionFlags{PublicAccess: false, BackupsEnabled: true, Provider: store.ProviderCustom}, "")
	require.NoError(t, err)
	require.Equal(t, testOrganizationRegionID, orgRegion.ID)
	require.Equal(t, &testOrganizationID, orgRegion.OrganizationID)
	require.Equal(t, false, orgRegion.PublicAccess)
	require.Equal(t, true, orgRegion.BackupsEnabled)
	require.Equal(t, store.ProviderCustom, orgRegion.Provider)

	regions, err = sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region, *orgRegion})

	regions, err = sqlStore.ListRegions(ctx, "other")
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region})

	// Test creating region with backups disabled
	regionNoBackups, err := sqlStore.CreateRegion(ctx, "no-backups-region", store.RegionFlags{PublicAccess: false, BackupsEnabled: false, Provider: store.ProviderAWS}, "")
	require.NoError(t, err)
	require.Equal(t, "no-backups-region", regionNoBackups.ID)
	require.Equal(t, false, regionNoBackups.PublicAccess)
	require.Equal(t, false, regionNoBackups.BackupsEnabled)

	// Test creating region with a different provider
	gcpRegion, err := sqlStore.CreateRegion(ctx, "gcp-region", store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderGCP}, "")
	require.NoError(t, err)
	require.Equal(t, store.ProviderGCP, gcpRegion.Provider)

	// invalid provider is rejected
	_, err = sqlStore.CreateRegion(ctx, "invalid-provider-region", store.RegionFlags{Provider: "azure"}, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown provider")

	// provider is required
	_, err = sqlStore.CreateRegion(ctx, "missing-provider-region", store.RegionFlags{}, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown provider")

	// duplicate region cannot be created
	_, err = sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, "")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionAlreadyExists{})

	_, err = sqlStore.CreateOrganizationRegion(ctx, "other", testOrganizationRegionID, store.RegionFlags{PublicAccess: false, BackupsEnabled: true, Provider: store.ProviderAWS}, "")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionAlreadyExists{})

	// list cells
	cells, err := sqlStore.ListCells(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.Empty(t, cells)

	// create cell in public region
	cell, err := sqlStore.CreateCell(ctx, testRegionID, testCellID, testGRPCURL, isPrimaryCell)
	require.NoError(t, err)
	require.Equal(t, testCellID, cell.ID)
	require.Equal(t, testRegionID, cell.RegionID)
	require.Equal(t, testGRPCURL, cell.ClustersGRPCURL)
	require.Equal(t, isPrimaryCell, cell.Primary)

	cells, err = sqlStore.ListCells(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.ElementsMatch(t, cells, []store.Cell{*cell})

	// cell names must be unique
	_, err = sqlStore.CreateCell(ctx, testRegionID, testCellID, testGRPCURL, isPrimaryCell)
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrCellAlreadyExists{})

	// create cell in organization region
	orgCell, err := sqlStore.CreateCell(ctx, testOrganizationRegionID, testOrganizationCellID, testGRPCURL, isPrimaryCell)
	require.NoError(t, err)
	require.Equal(t, testOrganizationCellID, orgCell.ID)
	require.Equal(t, testOrganizationRegionID, orgCell.RegionID)
	require.Equal(t, testGRPCURL, orgCell.ClustersGRPCURL)
	require.Equal(t, isPrimaryCell, orgCell.Primary)

	cells, err = sqlStore.ListCells(ctx, testOrganizationID, testOrganizationRegionID)
	require.NoError(t, err)
	require.ElementsMatch(t, cells, []store.Cell{*orgCell})

	cells, err = sqlStore.ListCells(ctx, "other", testOrganizationRegionID)
	require.NoError(t, err)
	require.Empty(t, cells)

	// create a project & branches in both regions
	project, err := sqlStore.CreateProject(ctx, testOrganizationID, createProjectConfig("test", nil))
	require.NoError(t, err)

	// create a branch in the public region
	branch, err := sqlStore.CreateBranch(ctx, testOrganizationID, project.ID, testCellID, createBranchConfig("test-branch", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, testCellID, branch.CellID)
	require.Equal(t, testRegionID, branch.Region)
	require.Equal(t, true, branch.PublicAccess)

	orgRegionBranch, err := sqlStore.CreateBranch(ctx, testOrganizationID, project.ID, testOrganizationCellID, createBranchConfig("test-branch-org", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, testOrganizationCellID, orgRegionBranch.CellID)
	require.Equal(t, testOrganizationRegionID, orgRegionBranch.Region)
	require.Equal(t, false, orgRegionBranch.PublicAccess)

	// list branches
	branches, err := sqlStore.ListBranches(ctx, testOrganizationID, project.ID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, testRegionID, branches[0].Region)
	require.Equal(t, true, branches[0].PublicAccess)
	require.Equal(t, testOrganizationRegionID, branches[1].Region)
	require.Equal(t, false, branches[1].PublicAccess)

	// describe branches
	gotBranch, err := sqlStore.DescribeBranch(ctx, testOrganizationID, project.ID, branch.ID)
	require.NoError(t, err)
	require.Equal(t, branch.ID, gotBranch.ID)
	require.Equal(t, testRegionID, gotBranch.Region)
	require.Equal(t, true, gotBranch.PublicAccess)

	gotOrgRegionBranch, err := sqlStore.DescribeBranch(ctx, testOrganizationID, project.ID, orgRegionBranch.ID)
	require.NoError(t, err)
	require.Equal(t, orgRegionBranch.ID, gotOrgRegionBranch.ID)
	require.Equal(t, testOrganizationRegionID, gotOrgRegionBranch.Region)
	require.Equal(t, false, gotOrgRegionBranch.PublicAccess)

	// delete branches
	err = sqlStore.DeleteBranch(ctx, testOrganizationID, project.ID, branch.ID, func(*store.Branch) error { return nil })
	require.NoError(t, err)

	err = sqlStore.DeleteBranch(ctx, testOrganizationID, project.ID, orgRegionBranch.ID, func(*store.Branch) error { return nil })
	require.NoError(t, err)

	// get cell
	gotCell, err := sqlStore.GetCell(ctx, testOrganizationID, testCellID)
	require.NoError(t, err)
	require.Equal(t, cell, gotCell)

	gotOrgCell, err := sqlStore.GetCell(ctx, testOrganizationID, testOrganizationCellID)
	require.NoError(t, err)
	require.Equal(t, orgCell, gotOrgCell)

	// get cell not found
	gotCell, err = sqlStore.GetCell(ctx, testOrganizationID, "not-found")
	require.Error(t, err)
	require.Nil(t, gotCell)
	require.ErrorAs(t, err, &store.ErrCellNotFound{})

	// cannot delete regions with cells
	err = sqlStore.DeleteRegion(ctx, testRegionID)
	require.Error(t, err)

	err = sqlStore.DeleteRegion(ctx, testOrganizationRegionID)
	require.Error(t, err)

	err = sqlStore.DeleteRegion(ctx, "not-found")
	require.Error(t, err)

	// Cannot delete cell with soft-deleted (terminated) branches
	err = sqlStore.DeleteCell(ctx, testCellID)
	require.Error(t, err)

	// Clean up terminated branches
	_, err = sqlStore.CleanupTerminatedBranches(ctx, testCellID, time.Duration(0))
	require.NoError(t, err)
	_, err = sqlStore.CleanupTerminatedBranches(ctx, testOrganizationCellID, time.Duration(0))
	require.NoError(t, err)

	// delete cells
	err = sqlStore.DeleteCell(ctx, testCellID)
	require.NoError(t, err)
	err = sqlStore.DeleteCell(ctx, testOrganizationCellID)
	require.NoError(t, err)

	// delete cells not found
	err = sqlStore.DeleteCell(ctx, "not-found")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrCellNotFound{})

	// delete regions
	err = sqlStore.DeleteRegion(ctx, testRegionID)
	require.NoError(t, err)
	err = sqlStore.DeleteRegion(ctx, testOrganizationRegionID)
	require.NoError(t, err)

	// delete regions not found
	err = sqlStore.DeleteRegion(ctx, "not-found")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionNotFound{})
}

func TestPrimaryCellUniqueness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)

	testRegionID := "test-region"
	testCellID := "test-cell"
	testGRPCURL := "grpc://localhost:50051"

	// Create a public region
	region, err := sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, testGRPCURL)
	require.NoError(t, err)
	require.Equal(t, testRegionID, region.ID)

	// Create a primary cell in the public region
	cell, err := sqlStore.CreateCell(ctx, testRegionID, testCellID, testGRPCURL, true)
	require.NoError(t, err)
	require.Equal(t, testCellID, cell.ID)
	require.Equal(t, true, cell.Primary)

	// Get the cell and ensure it is primary
	gotCell, err := sqlStore.GetCell(ctx, "", testCellID)
	require.NoError(t, err)
	require.Equal(t, cell.ID, gotCell.ID)
	require.Equal(t, true, gotCell.Primary)

	// Create another cell in the same region, but not primary
	nonPrimaryCellID := "test-non-primary-cell"
	nonPrimaryCell, err := sqlStore.CreateCell(ctx, testRegionID, nonPrimaryCellID, testGRPCURL, false)
	require.NoError(t, err)
	require.Equal(t, nonPrimaryCellID, nonPrimaryCell.ID)
	require.Equal(t, false, nonPrimaryCell.Primary)

	// Get the non-primary cell and ensure it is not primary
	gotNonPrimaryCell, err := sqlStore.GetCell(ctx, "", nonPrimaryCellID)
	require.NoError(t, err)
	require.Equal(t, nonPrimaryCell.ID, gotNonPrimaryCell.ID)
	require.Equal(t, false, gotNonPrimaryCell.Primary)

	// Attempt to create another primary cell in the same region
	anotherPrimaryCellID := "test-another-primary-cell"
	_, err = sqlStore.CreateCell(ctx, testRegionID, anotherPrimaryCellID, testGRPCURL, true)
	// Expect an error because only one primary cell is allowed per region
	require.Error(t, err)
}

func TestGetPrimaryCell(t *testing.T) {
	t.Parallel()

	type testRegion struct {
		ID             string
		PublicAccess   bool
		OrganizationID string
	}

	type testCell struct {
		ID       string
		RegionID string
		Primary  bool
	}

	ctx := context.Background()
	testOrganizationID := "test-organization"
	testOtherOrganizationID := "other-organization"
	testGRPCURL := "grpc://localhost:50051"

	testCases := []struct {
		name           string
		setupRegions   []testRegion
		setupCells     []testCell
		orgID          string
		regionID       string
		expectedCellID string
		expectedError  any
	}{
		{
			name: "basic primary cell retrieval",
			setupRegions: []testRegion{
				{ID: "test-region-basic", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "primary-cell-basic", RegionID: "test-region-basic", Primary: true},
			},
			orgID:          testOrganizationID,
			regionID:       "test-region-basic",
			expectedCellID: "primary-cell-basic",
		},
		{
			name: "primary cell with multiple cells",
			setupRegions: []testRegion{
				{ID: "test-region-multiple", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "non-primary-cell-1", RegionID: "test-region-multiple", Primary: false},
				{ID: "non-primary-cell-2", RegionID: "test-region-multiple", Primary: false},
				{ID: "primary-cell-multiple", RegionID: "test-region-multiple", Primary: true},
			},
			orgID:          testOrganizationID,
			regionID:       "test-region-multiple",
			expectedCellID: "primary-cell-multiple",
		},
		{
			name: "organization can access public region primary cell",
			setupRegions: []testRegion{
				{ID: "public-region", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "public-primary-cell", RegionID: "public-region", Primary: true},
			},
			orgID:          testOrganizationID,
			regionID:       "public-region",
			expectedCellID: "public-primary-cell",
		},
		{
			name: "organization can access own region primary cell",
			setupRegions: []testRegion{
				{ID: "org-specific-region", PublicAccess: false, OrganizationID: testOrganizationID},
			},
			setupCells: []testCell{
				{ID: "org-primary-cell", RegionID: "org-specific-region", Primary: true},
			},
			orgID:          testOrganizationID,
			regionID:       "org-specific-region",
			expectedCellID: "org-primary-cell",
		},
		{
			name: "other organization cannot access org-specific region",
			setupRegions: []testRegion{
				{ID: "org-specific-region-2", PublicAccess: false, OrganizationID: testOrganizationID},
			},
			setupCells: []testCell{
				{ID: "org-primary-cell-2", RegionID: "org-specific-region-2", Primary: true},
			},
			orgID:         testOtherOrganizationID,
			regionID:      "org-specific-region-2",
			expectedError: &store.ErrCellNotFound{},
		},
		{
			name: "other organization can access public region primary cell",
			setupRegions: []testRegion{
				{ID: "public-region-2", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "public-primary-cell-2", RegionID: "public-region-2", Primary: true},
			},
			orgID:          testOtherOrganizationID,
			regionID:       "public-region-2",
			expectedCellID: "public-primary-cell-2",
		},
		{
			name:          "non-existent region",
			orgID:         testOrganizationID,
			regionID:      "non-existent-region",
			expectedError: &store.ErrCellNotFound{},
		},
		{
			name: "region with no primary cell",
			setupRegions: []testRegion{
				{ID: "no-primary-region", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "non-primary-1", RegionID: "no-primary-region", Primary: false},
				{ID: "non-primary-2", RegionID: "no-primary-region", Primary: false},
			},
			orgID:         testOrganizationID,
			regionID:      "no-primary-region",
			expectedError: &store.ErrCellNotFound{},
		},
		{
			name: "empty region",
			setupRegions: []testRegion{
				{ID: "empty-region", PublicAccess: true},
			},
			orgID:         testOrganizationID,
			regionID:      "empty-region",
			expectedError: &store.ErrCellNotFound{},
		},
		{
			name: "empty organization ID",
			setupRegions: []testRegion{
				{ID: "test-region-empty-org", PublicAccess: true},
			},
			setupCells: []testCell{
				{ID: "primary-cell-empty-org", RegionID: "test-region-empty-org", Primary: true},
			},
			orgID:          "",
			regionID:       "test-region-empty-org",
			expectedCellID: "primary-cell-empty-org",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)

			// Setup regions
			for _, region := range tc.setupRegions {
				if region.OrganizationID != "" {
					_, err := sqlStore.CreateOrganizationRegion(ctx, region.OrganizationID, region.ID, store.RegionFlags{PublicAccess: region.PublicAccess, BackupsEnabled: true, Provider: store.ProviderAWS}, testGRPCURL)
					require.NoError(t, err)
				} else {
					_, err := sqlStore.CreateRegion(ctx, region.ID, store.RegionFlags{PublicAccess: region.PublicAccess, BackupsEnabled: true, Provider: store.ProviderAWS}, testGRPCURL)
					require.NoError(t, err)
				}
			}

			// Setup cells
			for _, cell := range tc.setupCells {
				_, err := sqlStore.CreateCell(ctx, cell.RegionID, cell.ID, testGRPCURL, cell.Primary)
				require.NoError(t, err)
			}

			// Execute test
			gotCell, err := sqlStore.GetPrimaryCell(ctx, tc.orgID, tc.regionID)

			if tc.expectedError != nil {
				require.Error(t, err)
				require.Nil(t, gotCell)
				require.ErrorAs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, gotCell)
				require.Equal(t, tc.expectedCellID, gotCell.ID)
				require.Equal(t, tc.regionID, gotCell.RegionID)
				require.True(t, gotCell.Primary)
			}
		})
	}
}

package sqlstore

import (
	"context"
	"fmt"
	"log"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"xata/services/projects/store"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// Low limits on purpose so testing is faster
	maxDepth    = 3
	maxChildren = 2
)

func TestSQLStore(t *testing.T) {
	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)

	t.Run("projects", func(t *testing.T) {
		// create project with empty name fails
		_, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("", nil))
		require.Error(t, err)
		require.Equal(t, err, store.ErrInvalidProjectName{Name: ""})

		// test projects CRUD
		project, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("projectName", nil))
		require.NoError(t, err)
		require.Equal(t, "projectName", project.Name)
		require.True(t, len(project.ID) > 10 && strings.HasPrefix(project.ID, "prj_"))

		// create project with non default scale to zero configuration
		anotherProject, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("anotherProject", &store.ProjectScaleToZero{
			BaseBranches: store.ScaleToZero{
				Enabled:          false,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 40),
			},
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 15),
			},
		}))
		require.NoError(t, err)
		require.Equal(t, "anotherProject", anotherProject.Name)
		require.Equal(t, store.ProjectScaleToZero{
			BaseBranches: store.ScaleToZero{
				Enabled:          false,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 40),
			},
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 15),
			},
		}, anotherProject.ScaleToZero)
		require.True(t, len(anotherProject.ID) > 10 && strings.HasPrefix(anotherProject.ID, "prj_"))

		// create project with same name fails
		_, err = sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("projectName", nil))
		require.Error(t, err)
		require.Equal(t, err, store.ErrProjectAlreadyExists{
			Name: "projectName",
		})

		// get project
		newP, err := sqlStore.GetProject(ctx, "organizationID", project.ID)
		require.NoError(t, err)
		require.Equal(t, *project, *newP)

		// list project
		projects, err := sqlStore.ListProjects(ctx, "organizationID")
		require.NoError(t, err)
		require.Len(t, projects, 2)
		require.ElementsMatch(t, []store.Project{*project, *anotherProject}, projects)

		// unknown organization has no projects
		projects, err = sqlStore.ListProjects(ctx, "unknownOrganizationID")
		require.NoError(t, err)
		require.Len(t, projects, 0)

		// unknown project returns an error
		_, err = sqlStore.GetProject(ctx, "organizationID", "unknownProjectID")
		require.Equal(t, err, store.ErrProjectNotFound{ID: "unknownProjectID"})

		// update project no changes
		newP, err = sqlStore.UpdateProject(ctx, "organizationID", project.ID, updateProjectConfig(nil, nil))
		require.NoError(t, err)
		require.Equal(t, "projectName", newP.Name)
		require.True(t, project.UpdatedAt.Equal(newP.UpdatedAt))

		// update project name
		newP, err = sqlStore.UpdateProject(ctx, "organizationID", project.ID, updateProjectConfig(new("newProjectName"), nil))
		require.NoError(t, err)
		require.Equal(t, "newProjectName", newP.Name)
		require.True(t, project.UpdatedAt.Before(newP.UpdatedAt))

		// update project scale to zero configuration
		newP, err = sqlStore.UpdateProject(ctx, "organizationID", project.ID, updateProjectConfig(new(""), &store.ProjectScaleToZero{
			BaseBranches: defaultScaleToZeroConfig(),
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 15),
			},
		}))
		require.NoError(t, err)
		require.Equal(t, store.ProjectScaleToZero{
			BaseBranches: defaultScaleToZeroConfig(),
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(time.Minute * 15),
			},
		}, newP.ScaleToZero)
		require.True(t, project.UpdatedAt.Before(newP.UpdatedAt))

		// update project to already existing name
		sameNamePrj, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("updatedName", nil))
		require.NoError(t, err)
		_, err = sqlStore.UpdateProject(ctx, "organizationID", project.ID, updateProjectConfig(new("updatedName"), nil))
		require.Error(t, err)
		require.Equal(t, err, store.ErrProjectAlreadyExists{Name: "updatedName"})

		// delete projects
		err = sqlStore.DeleteProject(ctx, "organizationID", sameNamePrj.ID)
		require.NoError(t, err)
		// delete project
		err = sqlStore.DeleteProject(ctx, "organizationID", project.ID)
		require.NoError(t, err)
		err = sqlStore.DeleteProject(ctx, "organizationID", anotherProject.ID)
		require.NoError(t, err)

		// project is deleted
		projects, err = sqlStore.ListProjects(ctx, "organizationID")
		require.NoError(t, err)
		require.Len(t, projects, 0)

		newP, err = sqlStore.GetProject(ctx, "organizationID", project.ID)
		require.Nil(t, newP)
		require.Equal(t, err, store.ErrProjectNotFound{ID: project.ID})

		// unknown project returns an error
		err = sqlStore.DeleteProject(ctx, "organizationID", "unknownProjectID")
		require.Equal(t, err, store.ErrProjectNotFound{ID: "unknownProjectID"})
	})

	// project deletion tests
	deletePrj, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("testDeletePrj", nil))
	require.NoError(t, err)

	otherPrj, err := sqlStore.CreateProject(ctx, "otherWS", createProjectConfig("other", nil))
	require.NoError(t, err)

	fullProject, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("fullPrj", nil))
	require.NoError(t, err)

	// branch-related tests
	createRegionAndCell(t, sqlStore, "region", "cell")

	_, err = sqlStore.CreateBranch(ctx, "organizationID", fullProject.ID, "cell", createBranchConfig("filling", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)

	projectNotFoundErr := store.ErrProjectNotFound{ID: otherPrj.ID}
	projectNotEmptyErr := store.ErrProjectNotEmpty{ID: fullProject.ID}

	projectDeleteTests := []struct {
		name         string
		projectID    string
		wantError    bool
		errorMessage string
	}{
		{
			name:      "empty project gets deleted",
			projectID: deletePrj.ID,
			wantError: false,
		},
		{
			name:         "project with branches cannot be deleted",
			projectID:    fullProject.ID,
			wantError:    true,
			errorMessage: projectNotEmptyErr.Error(),
		},
		{
			name:         "non-existing project cannot be deleted",
			projectID:    otherPrj.ID,
			wantError:    true,
			errorMessage: projectNotFoundErr.Error(),
		},
	}

	for _, tt := range projectDeleteTests {
		t.Run(tt.name, func(t *testing.T) {
			err = sqlStore.DeleteProject(ctx, "organizationID", tt.projectID)
			if tt.wantError {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("delete project cleans up orphaned backup entries", func(t *testing.T) {
		prj, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("projectWithOrphanedBackup", nil))
		require.NoError(t, err)

		// Manually insert an orphaned backup entry (simulating legacy data)
		_, err = sqlStore.sql.ExecContext(ctx, `
			INSERT INTO backups (id, project_id, branch_id, name, type, description, retention_period, orphan)
			VALUES ($1, $2, NULL, $3, $4, $5, $6, $7)`,
			"orphaned-backup-id", prj.ID, "orphaned-backup", "continuous", "Orphaned backup", 7, true)
		require.NoError(t, err)

		// Verify the backup exists
		var count int
		err = sqlStore.sql.QueryRow("SELECT COUNT(*) FROM backups WHERE project_id = $1", prj.ID).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		// Delete the project - should succeed and clean up the orphaned backup
		err = sqlStore.DeleteProject(ctx, "organizationID", prj.ID)
		require.NoError(t, err)

		// Verify the backup was cleaned up
		err = sqlStore.sql.QueryRow("SELECT COUNT(*) FROM backups WHERE project_id = $1", prj.ID).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	project, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("projectName", nil))
	require.NoError(t, err)

	altProject, err := sqlStore.CreateProject(ctx, "otherWS", createProjectConfig("projectName", nil))
	require.NoError(t, err)

	parent, err := sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("parent", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)

	fakeParentID := "fakeID"

	_, cleanupBranchLimit := createProjectAndBranchesForLimitTest(t, ctx, sqlStore, "organizationID", "branchLimitProject")
	defer cleanupBranchLimit()

	depthProject, depthBranch, childrenBranch, cleanupDepthLimit := createProjectAndBranchesForDepthTest(t, ctx, sqlStore, "organizationID", "depthProject")
	defer cleanupDepthLimit()

	parentNotFoundErr := store.ErrBranchNotFound{ID: fakeParentID}
	branchAlreadyExistsErr := store.ErrBranchAlreadyExists{Name: "branch4"}
	projectNotFoundErr = store.ErrProjectNotFound{ID: altProject.ID}

	var gotBranch *store.Branch
	branchCreateTests := []struct {
		name           string
		branchName     string
		organizationID string
		projectID      string
		parentID       *string
		description    *string
		callbackFunc   func(b *store.Branch) error
		wantError      bool
		errorMessage   string
	}{
		{
			name:           "create branch succeeds",
			branchName:     "branch1",
			organizationID: "organizationID",
			projectID:      project.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				gotBranch = b
				return nil
			},
			wantError: false,
		},
		{
			name:           "create branch with parent branch succeeds",
			branchName:     "branchChild",
			organizationID: "organizationID",
			projectID:      project.ID,
			parentID:       &parent.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				gotBranch = b
				return nil
			},
			wantError: false,
		},
		{
			name:           "create branch with non-existing parent branch fails",
			branchName:     "branchChild2",
			organizationID: "organizationID",
			projectID:      project.ID,
			parentID:       &fakeParentID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				gotBranch = b
				return nil
			},
			wantError:    true,
			errorMessage: parentNotFoundErr.Error(),
		},
		{
			name:           "create branch fails in wrong organization",
			branchName:     "branch2",
			organizationID: "organizationID",
			projectID:      altProject.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				gotBranch = b
				return nil
			},
			wantError:    true,
			errorMessage: projectNotFoundErr.Error(),
		},
		{
			name:           "create branch fails for failed provision",
			branchName:     "branch3",
			organizationID: "organizationID",
			projectID:      project.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				return fmt.Errorf("some error from infra")
			},
			wantError:    true,
			errorMessage: "some error from infra",
		},
		{
			name:           "create branch fails for existing branch name",
			branchName:     "branch4",
			organizationID: "organizationID",
			projectID:      project.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				return nil
			},
			wantError:    true,
			errorMessage: branchAlreadyExistsErr.Error(),
		},
		{
			name:           "create branch fails due to max depth limit",
			branchName:     "b1111",
			organizationID: "organizationID",
			projectID:      depthProject.ID,
			parentID:       &depthBranch.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				return nil
			},
			wantError:    true,
			errorMessage: store.ErrMaxDepthExceeded{BranchID: depthBranch.ID, MaxDepth: maxDepth}.Error(),
		},
		{
			name:           "create branch fails due to max children limit",
			branchName:     "b113",
			organizationID: "organizationID",
			projectID:      depthProject.ID,
			parentID:       &childrenBranch.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				return nil
			},
			wantError:    true,
			errorMessage: store.ErrMaxChildrenExceeded{BranchID: childrenBranch.ID, MaxChildren: maxChildren}.Error(),
		},
		{
			name:           "create branch with custom backup retention period succeeds",
			branchName:     "branchWithCustomBackup",
			organizationID: "organizationID",
			projectID:      project.ID,
			description:    nil,
			callbackFunc: func(b *store.Branch) error {
				gotBranch = b
				return nil
			},
			wantError: false,
		},
	}
	for _, tt := range branchCreateTests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "create branch fails for existing branch name" {
				branch, err := sqlStore.CreateBranch(ctx, tt.organizationID, tt.projectID, "cell", createBranchConfig(tt.branchName, nil, nil), tt.callbackFunc)
				require.NoError(t, err)
				require.Equal(t, tt.branchName, branch.Name)
			}
			branch, err := sqlStore.CreateBranch(ctx, tt.organizationID, tt.projectID, "cell", createBranchConfig(tt.branchName, tt.parentID, tt.description), tt.callbackFunc)
			if !tt.wantError {
				require.NoError(t, err)
				require.Equal(t, tt.branchName, branch.Name)
				require.True(t, len(branch.ID) > 10)
				require.Equal(t, branch, gotBranch)
				if tt.parentID != nil {
					require.Equal(t, int32(2), branch.Depth)
					require.Equal(t, *tt.parentID, *branch.ParentID)
				} else {
					require.Equal(t, int32(1), branch.Depth)
					require.Nil(t, branch.ParentID)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, branch)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	// list branches project
	listProject, err := sqlStore.CreateProject(ctx, "organizationID", createProjectConfig("listProject", nil))
	require.NoError(t, err)
	var listBranches []store.Branch
	for i := range 5 {
		branch, err := sqlStore.CreateBranch(ctx, "organizationID", listProject.ID, "cell", createBranchConfig("listBranch"+strconv.Itoa(i), nil, nil), func(b *store.Branch) error {
			return nil
		})
		require.NoError(t, err)
		listBranches = append(listBranches, *branch)
	}

	branchListTests := []struct {
		name           string
		organizationID string
		projectID      string
		branches       []store.Branch
		wantError      bool
		errorMessage   string
	}{
		{
			name:           "empty project",
			organizationID: "otherWS",
			projectID:      altProject.ID,
			branches:       []store.Branch{},
			wantError:      false,
		},
		{
			name:           "list branches on a project",
			organizationID: "organizationID",
			projectID:      listProject.ID,
			branches:       listBranches,
			wantError:      false,
		},
	}
	for _, tt := range branchListTests {
		t.Run(tt.name, func(t *testing.T) {
			branches, err := sqlStore.ListBranches(ctx, tt.organizationID, tt.projectID)
			if !tt.wantError {
				require.NoError(t, err)
				require.Equal(t, len(tt.branches), len(branches))
				for i := range tt.branches {
					require.True(t, slices.ContainsFunc(branches, func(branch store.Branch) bool {
						return branch.ID == tt.branches[i].ID
					}))
					require.True(t, slices.ContainsFunc(branches, func(branch store.Branch) bool {
						return branch.Name == tt.branches[i].Name
					}))
					require.True(t, slices.ContainsFunc(branches, func(branch store.Branch) bool {
						return branch.ParentID == tt.branches[i].ParentID
					}))
				}
			} else {
				require.Error(t, err)
				require.Empty(t, branches)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	// create a branch to describe
	branch, err := sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("describeBranch", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)
	// The region is created with BackupsEnabled: true, so we need to update the expected value
	branch.BackupsEnabled = true

	branchDescribeTests := []struct {
		name           string
		branchID       string
		organizationID string
		projectID      string
		wantError      bool
		errorMessage   string
	}{
		{
			name:           "describe branch works for correct branch and project",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      false,
		},
		{
			name:           "describe branch fails for wrong project or organization",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      altProject.ID,
			wantError:      true,
			errorMessage:   fmt.Sprintf("project [%s] not found", altProject.ID),
		},
		{
			name:           "describe branch fails for non-existing branch",
			branchID:       "fake-branch-id",
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      true,
			errorMessage:   "branch [fake-branch-id] not found",
		},
	}

	for _, tt := range branchDescribeTests {
		t.Run(tt.name, func(t *testing.T) {
			storedBranch, err := sqlStore.DescribeBranch(ctx, tt.organizationID, tt.projectID, tt.branchID)
			if !tt.wantError {
				require.NoError(t, err)
				require.Equal(t, branch, storedBranch)
			} else {
				require.Error(t, err)
				require.Nil(t, storedBranch)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	branchGetByNameTests := []struct {
		name           string
		branchName     string
		organizationID string
		projectID      string
		wantError      bool
		errorMessage   string
	}{
		{
			name:           "get branch by name works for correct branch and project",
			branchName:     branch.Name,
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      false,
		},
		{
			name:           "get branch by name fails for wrong project or organization",
			branchName:     branch.Name,
			organizationID: "organizationID",
			projectID:      altProject.ID,
			wantError:      true,
			errorMessage:   fmt.Sprintf("project [%s] not found", altProject.ID),
		},
		{
			name:           "get branch by name fails for non-existing branch",
			branchName:     "fake-branch-name",
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      true,
			errorMessage:   "branch [fake-branch-name] not found",
		},
	}

	for _, tt := range branchGetByNameTests {
		t.Run(tt.name, func(t *testing.T) {
			storedBranch, err := sqlStore.GetBranchByName(ctx, tt.organizationID, tt.projectID, tt.branchName)
			if !tt.wantError {
				require.NoError(t, err)
				require.Equal(t, branch, storedBranch)
			} else {
				require.Error(t, err)
				require.Nil(t, storedBranch)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	// create a branch to update and one for name clash
	branch, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("updateBranch", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)
	_, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("alreadyExists", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)

	// create a branch in a foreign project (different org) used for cross-tenant regression tests
	foreignBranch, err := sqlStore.CreateBranch(ctx, "otherWS", altProject.ID, "cell", createBranchConfig("foreignBranch", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)

	branchUpdateTests := []struct {
		name                string
		branchName          *string
		branchDescription   *string
		branchConfiguration any
		branchID            string
		organizationID      string
		projectID           string
		wantError           bool
		errorMessage        string
	}{
		{
			name:              "update branch name works for correct branch and project",
			branchName:        new("newName"),
			branchDescription: nil,
			branchID:          branch.ID,
			organizationID:    "organizationID",
			projectID:         project.ID,
			wantError:         false,
		},
		{
			name:              "update branch description works for correct branch and project",
			branchName:        nil,
			branchDescription: new("newDesc"),
			branchID:          branch.ID,
			organizationID:    "organizationID",
			projectID:         project.ID,
			wantError:         false,
		},
		{
			name:              "update branch name and description works for correct branch and project",
			branchName:        new("newName"),
			branchDescription: new("newDesc"),
			branchID:          branch.ID,
			organizationID:    "organizationID",
			projectID:         project.ID,
			wantError:         false,
		},
		{
			name:              "update branch configuration works for correct branch and project",
			branchID:          branch.ID,
			branchName:        nil,
			branchDescription: nil,
			branchConfiguration: struct {
				instances int
				storage   int
			}{
				instances: 3,
				storage:   25,
			},
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      false,
		},
		{
			name:           "update branch fails for incorrect project",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      altProject.ID,
			wantError:      true,
			errorMessage:   fmt.Sprintf("project [%s] not found", altProject.ID),
		},
		{
			name:           "update branch fails for non-existing branch",
			branchID:       "fake-branch-id",
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      true,
			errorMessage:   "branch [fake-branch-id] not found",
		},
		{
			name:              "update branch fails when branch belongs to a different project",
			branchID:          foreignBranch.ID,
			branchName:        new("pwned"),
			branchDescription: new("attacker overwrote this"),
			organizationID:    "organizationID",
			projectID:         project.ID,
			wantError:         true,
			errorMessage:      fmt.Sprintf("branch [%s] not found", foreignBranch.ID),
		},
		{
			name:           "update branch name fails for already existing branch with that name",
			branchID:       branch.ID,
			branchName:     new("alreadyExists"),
			organizationID: "organizationID",
			projectID:      project.ID,
			wantError:      true,
			errorMessage:   "branch [alreadyExists] already exists",
		},
	}

	for _, tt := range branchUpdateTests {
		t.Run(tt.name, func(t *testing.T) {
			updatedBranch, err := sqlStore.UpdateBranch(ctx, tt.organizationID, tt.projectID, tt.branchID, updateBranchConfig(tt.branchName, tt.branchDescription), func(b *store.Branch) error {
				return nil
			})
			if !tt.wantError {
				require.NoError(t, err)
				if tt.branchName != nil {
					require.Equal(t, *tt.branchName, updatedBranch.Name)
				}
				if tt.branchDescription != nil {
					require.Equal(t, *tt.branchDescription, *updatedBranch.Description)
				}
			} else {
				require.Error(t, err)
				require.Nil(t, updatedBranch)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	// create a branch to delete
	branch, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("deleteBranch", nil, nil), func(b *store.Branch) error {
		return nil
	})
	require.NoError(t, err)

	branchDeleteTests := []struct {
		name           string
		branchID       string
		organizationID string
		projectID      string
		callbackFunc   func(*store.Branch) error
		wantError      bool
		errorMessage   string
	}{
		{
			name:           "delete branch fails for incorrect project",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      altProject.ID,
			callbackFunc: func(*store.Branch) error {
				return nil
			},
			wantError:    true,
			errorMessage: fmt.Sprintf("project [%s] not found", altProject.ID),
		},
		{
			name:           "delete branch fails for incorrect branch ID",
			branchID:       "fake-branch-id",
			organizationID: "organizationID",
			projectID:      project.ID,
			callbackFunc: func(*store.Branch) error {
				return nil
			},
			wantError:    true,
			errorMessage: "branch [fake-branch-id] not found",
		},
		{
			name:           "delete branch fails if infra call fails",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      project.ID,
			callbackFunc: func(*store.Branch) error {
				return fmt.Errorf("some infra error")
			},
			wantError:    true,
			errorMessage: "some infra error",
		},
		{
			name:           "delete branch works for correct branch and project",
			branchID:       branch.ID,
			organizationID: "organizationID",
			projectID:      project.ID,
			callbackFunc: func(*store.Branch) error {
				return nil
			},
			wantError: false,
		},
	}

	for _, tt := range branchDeleteTests {
		t.Run(tt.name, func(t *testing.T) {
			err := sqlStore.DeleteBranch(ctx, tt.organizationID, tt.projectID, tt.branchID, tt.callbackFunc)
			if !tt.wantError {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			}
		})
	}

	listInstanceTypesTest := []struct {
		name               string
		organizationID     string
		region             string
		wantCount          int
		wantFirst          store.InstanceType
		expectedMultiplier float64
	}{
		{
			name:               "us-east-1 applies 1.0 multiplier",
			organizationID:     "org-abc",
			region:             "us-east-1",
			wantCount:          len(store.InstanceTypes),
			wantFirst:          store.InstanceTypes[0],
			expectedMultiplier: 1.0,
		},
		{
			name:               "eu-central-1 applies 1.15 multiplier",
			organizationID:     "org-xyz",
			region:             "eu-central-1",
			wantCount:          len(store.InstanceTypes),
			wantFirst:          store.InstanceTypes[0],
			expectedMultiplier: 1.15,
		},
		{
			name:               "unknown region applies 1.0 multiplier",
			organizationID:     "org-xyz",
			region:             "unknown-region",
			wantCount:          len(store.InstanceTypes),
			wantFirst:          store.InstanceTypes[0],
			expectedMultiplier: 1.0,
		},
	}

	for _, tt := range listInstanceTypesTest {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := sqlStore.ListInstanceTypes(ctx, tt.organizationID, tt.region)

			require.NoError(t, err)
			require.Len(t, got, tt.wantCount)

			// Check first element as a sanity check
			require.Equal(t, tt.wantFirst.Name, got[0].Name)
			require.Equal(t, tt.wantFirst.VCPUsRequest, got[0].VCPUsRequest)
			require.Equal(t, tt.wantFirst.VCPUsLimit, got[0].VCPUsLimit)
			require.Equal(t, tt.wantFirst.RAM, got[0].RAM)

			// Verify that the hourly rate is multiplied by the expected multiplier and rounded to 3 decimals
			expectedHourlyRate := math.Round(tt.wantFirst.HourlyRate*tt.expectedMultiplier*1000) / 1000
			require.Equal(t, expectedHourlyRate, got[0].HourlyRate)

			// Verify that the storage monthly rate is multiplied by the expected multiplier and rounded to 2 decimals
			expectedStorageMonthlyRate := math.Round(tt.wantFirst.StorageMonthlyRate*tt.expectedMultiplier*100) / 100
			require.Equal(t, expectedStorageMonthlyRate, got[0].StorageMonthlyRate)

			// Verify all instance types have the correct multiplier applied
			for i, instanceType := range got {
				originalInstanceType := store.InstanceTypes[i]
				expectedHourlyRate := math.Round(originalInstanceType.HourlyRate*tt.expectedMultiplier*1000) / 1000
				expectedStorageMonthlyRate := math.Round(originalInstanceType.StorageMonthlyRate*tt.expectedMultiplier*100) / 100

				require.Equal(t, expectedHourlyRate, instanceType.HourlyRate,
					"Instance type %s should have hourly rate %f but got %f",
					instanceType.Name, expectedHourlyRate, instanceType.HourlyRate)
				require.Equal(t, expectedStorageMonthlyRate, instanceType.StorageMonthlyRate,
					"Instance type %s should have storage monthly rate %f but got %f",
					instanceType.Name, expectedStorageMonthlyRate, instanceType.StorageMonthlyRate)
			}
		})
	}

	t.Run("org_limits", func(t *testing.T) {
		const orgID = "test-org"
		const projectID = "test-project"

		t.Run("empty returns no overrides", func(t *testing.T) {
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, projectID)
			require.NoError(t, err)
			require.Empty(t, limits)
		})

		t.Run("set and get org-level", func(t *testing.T) {
			tests := map[string]struct {
				key   store.LimitKey
				value any
				want  any
			}{
				"integer": {key: store.LimitMaxBranchesPerProject, value: int64(200), want: int64(200)},
				"string":  {key: store.LimitMaxAllowedInstanceType, value: "xata.large", want: "xata.large"},
			}
			for name, tc := range tests {
				t.Run(name, func(t *testing.T) {
					require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", tc.key, tc.value))
					limits, err := sqlStore.GetOrgLimits(ctx, orgID, "")
					require.NoError(t, err)
					got, ok := limits[tc.key]
					require.True(t, ok)
					require.Equal(t, tc.want, jsonNumberToInt(got))
				})
			}
		})

		t.Run("project overrides org", func(t *testing.T) {
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject, int64(100)))
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, projectID, store.LimitMaxBranchesPerProject, int64(500)))
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, projectID)
			require.NoError(t, err)
			require.Equal(t, int64(500), jsonNumberToInt(limits[store.LimitMaxBranchesPerProject]))
		})

		t.Run("org-level fallback when no project override", func(t *testing.T) {
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxProjects, int64(10)))
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, "other-project")
			require.NoError(t, err)
			require.Equal(t, int64(10), jsonNumberToInt(limits[store.LimitMaxProjects]))
		})

		t.Run("overwrite updates value", func(t *testing.T) {
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject, int64(1)))
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject, int64(999)))
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, "")
			require.NoError(t, err)
			require.Equal(t, int64(999), jsonNumberToInt(limits[store.LimitMaxBranchesPerProject]))
		})

		t.Run("delete org-level override", func(t *testing.T) {
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject, int64(50)))
			require.NoError(t, sqlStore.DeleteOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject))
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, "")
			require.NoError(t, err)
			_, ok := limits[store.LimitMaxBranchesPerProject]
			require.False(t, ok)
		})

		t.Run("delete project-level falls back to org", func(t *testing.T) {
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitMaxBranchesPerProject, int64(100)))
			require.NoError(t, sqlStore.SetOrgLimit(ctx, orgID, projectID, store.LimitMaxBranchesPerProject, int64(500)))
			require.NoError(t, sqlStore.DeleteOrgLimit(ctx, orgID, projectID, store.LimitMaxBranchesPerProject))
			limits, err := sqlStore.GetOrgLimits(ctx, orgID, projectID)
			require.NoError(t, err)
			require.Equal(t, int64(100), jsonNumberToInt(limits[store.LimitMaxBranchesPerProject]))
		})

		t.Run("invalid key is rejected", func(t *testing.T) {
			err := sqlStore.SetOrgLimit(ctx, orgID, "", store.LimitKey("max_members"), int64(1))
			require.Error(t, err)
		})
	})
}

func createProjectAndBranchesForLimitTest(t *testing.T, ctx context.Context, sqlStore *sqlProjectStore, orgID string, name string) (project *store.Project, cleanup func()) {
	project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig(name, nil))
	require.NoError(t, err)

	for i := range store.MaxBranchesPerProject {
		_, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig(fmt.Sprintf("br%d", i), nil, nil), func(b *store.Branch) error {
			return nil
		})
		require.NoError(t, err)
	}
	return project, func() {
		deleteBranches(t, ctx, sqlStore, orgID, project.ID)
		err := sqlStore.DeleteProject(ctx, orgID, project.ID)
		require.NoError(t, err)
	}
}

func createProjectAndBranchesForDepthTest(t *testing.T, ctx context.Context, sqlStore *sqlProjectStore, orgID string, name string) (project *store.Project, depthBranch *store.Branch, childrenBranch *store.Branch, cleanup func()) {
	project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig(name, nil))
	require.NoError(t, err)

	// create a parent branch
	parent, err := sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b1", nil, nil), noopProvisionFunc)
	require.NoError(t, err)

	// add first child
	childrenBranch, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b11", &parent.ID, nil), noopProvisionFunc)
	require.NoError(t, err)

	// Add children until about to hit the limit
	_, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b111", &childrenBranch.ID, nil), noopProvisionFunc)
	require.NoError(t, err)
	_, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b112", &childrenBranch.ID, nil), noopProvisionFunc)
	require.NoError(t, err)
	// Creating b113 should fail

	// Add nested children until about to hit the limit
	b12, err := sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b12", &parent.ID, nil), noopProvisionFunc)
	require.NoError(t, err)
	depthBranch, err = sqlStore.CreateBranch(ctx, "organizationID", project.ID, "cell", createBranchConfig("b121", &b12.ID, nil), noopProvisionFunc)
	require.NoError(t, err)
	// creating b1211 should fail

	return project, depthBranch, childrenBranch, func() {
		deleteBranches(t, ctx, sqlStore, orgID, project.ID)
		err := sqlStore.DeleteProject(ctx, orgID, project.ID)
		require.NoError(t, err)
	}
}

func deleteBranches(t *testing.T, ctx context.Context, sqlStore *sqlProjectStore, orgID, projectID string) {
	branches, err := sqlStore.ListBranches(ctx, orgID, projectID)
	require.NoError(t, err)

	for _, branch := range branches {
		err := sqlStore.DeleteBranch(ctx, orgID, projectID, branch.ID, func(*store.Branch) error {
			return nil
		})
		require.NoError(t, err)
	}
}

func setupSQLStore(ctx context.Context, t *testing.T, maxDepth, maxChildren int32) *sqlProjectStore {
	// launch postgres container with testcontainers (TODO abstract this with a helper)
	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine", // TODO parametrize version
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	})

	// create a new SQL sqlStore
	config, err := ConfigFromConnectionString(postgresContainer.MustConnectionString(ctx, "sslmode=disable"))
	require.NoError(t, err)
	sqlStore, err := NewSQLProjectStore(ctx, config, maxDepth, maxChildren)
	if err != nil {
		t.Fatalf("failed to create store: %s", err)
	}
	t.Cleanup(func() {
		if err := sqlStore.Close(ctx); err != nil {
			log.Printf("failed to close store: %s", err)
		}
	})

	// run migrations
	err = sqlStore.Setup(ctx)
	require.NoError(t, err)

	return sqlStore
}

func jsonNumberToInt(v any) any {
	if n, ok := v.(interface{ Int64() (int64, error) }); ok {
		if i, err := n.Int64(); err == nil {
			return i
		}
	}
	return v
}

func noopProvisionFunc(b *store.Branch) error {
	return nil
}

func createRegionAndCell(t testing.TB, sqlStore *sqlProjectStore, regionID, cellID string) (*store.Region, *store.Cell) {
	ctx := context.Background()

	region, err := sqlStore.CreateRegion(ctx, regionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true}, "")
	if err != nil {
		t.Fatalf("failed to create region: %s", err)
	}
	t.Cleanup(func() {
		sqlStore.DeleteRegion(ctx, region.ID)
	})

	isPrimaryCell := true
	cell, err := sqlStore.CreateCell(ctx, regionID, cellID, "grpc://localhost:50051", isPrimaryCell)
	if err != nil {
		t.Fatalf("failed to create cell: %v", err)
	}
	t.Cleanup(func() {
		sqlStore.DeleteCell(ctx, cell.ID)
	})
	return region, cell
}

func TestValidateHierarchy(t *testing.T) {
	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)

	createRegionAndCell(t, sqlStore, "region", "cell")

	org1 := "org1"
	org2 := "org2"
	project1, err := sqlStore.CreateProject(ctx, org1, createProjectConfig("project1", nil))
	require.NoError(t, err)
	project2, err := sqlStore.CreateProject(ctx, org2, createProjectConfig("project2", nil))
	require.NoError(t, err)

	branch1, err := sqlStore.CreateBranch(ctx, org1, project1.ID, "cell", createBranchConfig("branch1", nil, nil), noopProvisionFunc)
	require.NoError(t, err)
	branch2, err := sqlStore.CreateBranch(ctx, org2, project2.ID, "cell", createBranchConfig("branch2", nil, nil), noopProvisionFunc)
	require.NoError(t, err)

	tests := []struct {
		name            string
		organizationIds []string
		projectIds      []string
		branchIds       []string
		wantError       bool
		errorType       error
	}{
		{
			name:            "valid hierarchy",
			organizationIds: []string{org1, org2},
			projectIds:      []string{project1.ID, project2.ID},
			branchIds:       []string{branch1.ID, branch2.ID},
			wantError:       false,
			errorType:       nil,
		},
		{
			name:            "empty organization ID",
			organizationIds: []string{""},
			projectIds:      []string{project1.ID},
			branchIds:       []string{branch1.ID},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "organization", ID: ""},
		},
		{
			name:            "empty project ID",
			organizationIds: []string{org1},
			projectIds:      []string{""},
			branchIds:       []string{branch1.ID},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "project", ID: ""},
		},
		{
			name:            "empty branch ID",
			organizationIds: []string{org1},
			projectIds:      []string{project1.ID},
			branchIds:       []string{""},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "branch", ID: ""},
		},
		{
			name:            "project not found in organization",
			organizationIds: []string{org1},
			projectIds:      []string{project2.ID},
			branchIds:       []string{},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "project", ID: project2.ID},
		},
		{
			name:            "branch not found in project",
			organizationIds: []string{org1},
			projectIds:      []string{project1.ID},
			branchIds:       []string{branch2.ID},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "branch", ID: branch2.ID},
		},
		{
			name:            "nonexistent project ID",
			organizationIds: []string{org1},
			projectIds:      []string{"nonexistent"},
			branchIds:       []string{},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "project", ID: "nonexistent"},
		},
		{
			name:            "nonexistent branch ID",
			organizationIds: []string{org1},
			projectIds:      []string{project1.ID},
			branchIds:       []string{"nonexistent"},
			wantError:       true,
			errorType:       store.ErrInvalidHierarchy{Type: "branch", ID: "nonexistent"},
		},
		{
			name:            "valid with empty arrays",
			organizationIds: []string{org1},
			projectIds:      []string{},
			branchIds:       []string{},
			wantError:       false,
			errorType:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sqlStore.ValidateHierarchy(ctx, tt.organizationIds, tt.projectIds, tt.branchIds)
			if tt.wantError {
				require.Error(t, err)
				require.Equal(t, tt.errorType, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSoftDeleteBehavior(t *testing.T) {
	t.Parallel()

	t.Run("active project can reuse terminated project name", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("reusableName", nil))
		require.NoError(t, err)

		err = sqlStore.DeleteProject(ctx, orgID, project.ID)
		require.NoError(t, err)

		newProject, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("reusableName", nil))
		require.NoError(t, err)
		require.NotEqual(t, project.ID, newProject.ID)
		require.Equal(t, "reusableName", newProject.Name)
	})

	t.Run("active branch can reuse terminated branch name", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("branchReuseProject", nil))
		require.NoError(t, err)

		branch, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("reusableBranch", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, branch.ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		newBranch, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("reusableBranch", nil, nil), noopProvisionFunc)
		require.NoError(t, err)
		require.NotEqual(t, branch.ID, newBranch.ID)
		require.Equal(t, "reusableBranch", newBranch.Name)
	})

	t.Run("terminated project not returned by GetProject", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("getTerminated", nil))
		require.NoError(t, err)

		err = sqlStore.DeleteProject(ctx, orgID, project.ID)
		require.NoError(t, err)

		_, err = sqlStore.GetProject(ctx, orgID, project.ID)
		require.Error(t, err)
		require.Equal(t, store.ErrProjectNotFound{ID: project.ID}, err)
	})

	t.Run("terminated project not returned by ListProjects", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		listOrg := "listTerminatedOrg"

		project1, err := sqlStore.CreateProject(ctx, listOrg, createProjectConfig("activeProject", nil))
		require.NoError(t, err)

		project2, err := sqlStore.CreateProject(ctx, listOrg, createProjectConfig("terminatedProject", nil))
		require.NoError(t, err)

		err = sqlStore.DeleteProject(ctx, listOrg, project2.ID)
		require.NoError(t, err)

		projects, err := sqlStore.ListProjects(ctx, listOrg)
		require.NoError(t, err)
		require.Len(t, projects, 1)
		require.Equal(t, project1.ID, projects[0].ID)
	})

	t.Run("terminated branch not returned by DescribeBranch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("describeBranchProject", nil))
		require.NoError(t, err)

		branch, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("terminatedBranch", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, branch.ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		_, err = sqlStore.DescribeBranch(ctx, orgID, project.ID, branch.ID)
		require.Error(t, err)
		require.Equal(t, store.ErrBranchNotFound{ID: branch.ID}, err)
	})

	t.Run("terminated branch not returned by GetBranchByName", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("getBranchByNameProject", nil))
		require.NoError(t, err)

		branch, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("terminatedBranch", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, branch.ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		_, err = sqlStore.GetBranchByName(ctx, orgID, project.ID, branch.Name)
		require.Error(t, err)
		require.Equal(t, store.ErrBranchNotFound{ID: branch.Name}, err)
	})

	t.Run("terminated branch not returned by ListBranches", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("listBranchProject", nil))
		require.NoError(t, err)

		branch1, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("activeBranch", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		branch2, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("terminatedBranch2", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, branch2.ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		branches, err := sqlStore.ListBranches(ctx, orgID, project.ID)
		require.NoError(t, err)
		require.Len(t, branches, 1)
		require.Equal(t, branch1.ID, branches[0].ID)
	})

	t.Run("CountActiveProjectBranches excludes terminated branches", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, cleanup := createProjectAndBranchesForLimitTest(t, ctx, sqlStore, orgID, "branchLimitProject2")
		defer cleanup()

		count, err := sqlStore.CountActiveProjectBranches(ctx, project.ID)
		require.NoError(t, err)
		require.Equal(t, int64(store.MaxBranchesPerProject), count)

		branches, err := sqlStore.ListBranches(ctx, orgID, project.ID)
		require.NoError(t, err)
		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, branches[0].ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		count, err = sqlStore.CountActiveProjectBranches(ctx, project.ID)
		require.NoError(t, err)
		require.Equal(t, int64(store.MaxBranchesPerProject-1), count)
	})

	t.Run("terminated children branches do not count toward children limit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		createRegionAndCell(t, sqlStore, "region", "cell")
		orgID := "softDeleteOrg"

		project, err := sqlStore.CreateProject(ctx, orgID, createProjectConfig("childLimitProject", nil))
		require.NoError(t, err)

		parent, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("parent", nil, nil), noopProvisionFunc)
		require.NoError(t, err)

		firstChild, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("child1", &parent.ID, nil), noopProvisionFunc)
		require.NoError(t, err)

		grandChild1, err := sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("grandchild1", &firstChild.ID, nil), noopProvisionFunc)
		require.NoError(t, err)
		_, err = sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("grandchild2", &firstChild.ID, nil), noopProvisionFunc)
		require.NoError(t, err)

		_, err = sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("grandchild3", &firstChild.ID, nil), noopProvisionFunc)
		require.Equal(t, store.ErrMaxChildrenExceeded{BranchID: firstChild.ID, MaxChildren: maxChildren}, err)

		err = sqlStore.DeleteBranch(ctx, orgID, project.ID, grandChild1.ID, func(*store.Branch) error { return nil })
		require.NoError(t, err)

		_, err = sqlStore.CreateBranch(ctx, orgID, project.ID, "cell", createBranchConfig("grandchild3", &firstChild.ID, nil), noopProvisionFunc)
		require.NoError(t, err)
	})

	t.Run("CleanupTerminatedProjects", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		sqlStore := setupSQLStore(ctx, t, maxDepth, maxChildren)
		cleanupOrg := "cleanupTerminatedOrg"

		// Create and terminate a project
		project, err := sqlStore.CreateProject(ctx, cleanupOrg, createProjectConfig("toBeDeleted", nil))
		require.NoError(t, err)

		err = sqlStore.DeleteProject(ctx, cleanupOrg, project.ID)
		require.NoError(t, err)

		// Project should not be cleaned up with a long retention period
		deleted, err := sqlStore.CleanupTerminatedProjects(ctx, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, int64(0), deleted)

		// Project should be cleaned up with zero retention
		deleted, err = sqlStore.CleanupTerminatedProjects(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted)
	})
}

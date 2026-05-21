package policy_test

import data.policy

# valid_input tests
test_valid_input_allow if {
	policy.valid_input with input as {
		"request": {
			"method": "GET",
			"path": "/path",
			"scopes": [],
		},
		"claims": {
			"scopes": [],
			"organizations": {},
			"projects": [],
			"branches": [],
		},
	}
}

test_valid_input_deny_missing_request_method if {
	not policy.valid_input with input as {
		"request": {
			"path": "/path",
			"scopes": [],
		},
		"claims": {
			"scopes": [],
			"organizations": {},
			"projects": [],
			"branches": [],
		},
	}
}

# valid_scope tests
test_valid_scope_allow_wildcard if {
	policy.valid_scope with input as {
		"request": {"scopes": ["read"]},
		"claims": {"scopes": ["*"]},
	}
}

test_valid_scope_allow_exact_match if {
	policy.valid_scope with input as {
		"request": {"scopes": ["read"]},
		"claims": {"scopes": ["read"]},
	}
}

test_valid_scope_deny_no_match if {
	not policy.valid_scope with input as {
		"request": {"scopes": ["write"]},
		"claims": {"scopes": ["read"]},
	}
}

# valid_organization tests
test_valid_organization_allow_no_org_in_path if {
	policy.valid_organization with input as {
		"request": {"path": "/path"},
		"claims": {"organizations": {"org1": {"ID": "org1", "Status": "enabled"}}},
	}
}

test_valid_organization_allow_match if {
	policy.valid_organization with input as {
		"request": {"path": "/orgs/:organizationID", "organization": "org1"},
		"claims": {"organizations": {"org1": {"ID": "org1", "Status": "enabled"}}},
	}
}

test_valid_organization_deny_no_match if {
	not policy.valid_organization with input as {
		"request": {"path": "/orgs/:organizationID", "organization": "org2"},
		"claims": {"organizations": {"org1": {"ID": "org1", "Status": "enabled"}}},
	}
}

# valid_project tests
test_valid_project_allow_no_project_in_path_unrelated_scope if {
	policy.valid_project with input as {
		"request": {"path": "/path", "scopes": ["org:read"]},
		"claims": {"projects": ["proj1"]},
	}
}

test_valid_project_allow_wildcard if {
	policy.valid_project with input as {
		"request": {"path": "/projects/:projectID", "project": "proj1", "scopes": ["project:read"]},
		"claims": {"projects": ["*"]},
	}
}

test_valid_project_allow_match if {
	policy.valid_project with input as {
		"request": {"path": "/projects/:projectID", "project": "proj1", "scopes": ["project:read"]},
		"claims": {"projects": ["proj1"]},
	}
}

test_valid_project_deny_no_match if {
	not policy.valid_project with input as {
		"request": {"path": "/projects/:projectID", "project": "proj2", "scopes": ["project:read"]},
		"claims": {"projects": ["proj1"]},
	}
}

# Write on collection routes (no :projectID) requires wildcard project access.
test_valid_project_deny_write_collection_route_without_wildcard if {
	not policy.valid_project with input as {
		"request": {"path": "/organizations/:organizationID/projects", "scopes": ["project:write"]},
		"claims": {"projects": ["proj1"]},
	}
}

test_valid_project_allow_write_collection_route_with_wildcard if {
	policy.valid_project with input as {
		"request": {"path": "/organizations/:organizationID/projects", "scopes": ["project:write"]},
		"claims": {"projects": ["*"]},
	}
}

test_valid_project_allow_read_collection_route_with_restricted_key if {
	policy.valid_project with input as {
		"request": {"path": "/organizations/:organizationID/projects", "scopes": ["project:read"]},
		"claims": {"projects": ["proj1"]},
	}
}

# valid_branch tests
test_valid_branch_allow_no_branch_in_path_unrelated_scope if {
	policy.valid_branch with input as {
		"request": {"path": "/path", "scopes": ["org:read"]},
		"claims": {"branches": ["main"]},
	}
}

test_valid_branch_allow_wildcard if {
	policy.valid_branch with input as {
		"request": {"path": "/branches/:branchID", "branch": "dev", "scopes": ["branch:read"]},
		"claims": {"branches": ["*"]},
	}
}

test_valid_branch_allow_match if {
	policy.valid_branch with input as {
		"request": {"path": "/branches/:branchID", "branch": "main", "scopes": ["branch:read"]},
		"claims": {"branches": ["main"]},
	}
}

test_valid_branch_deny_no_match if {
	not policy.valid_branch with input as {
		"request": {"path": "/branches/:branchID", "branch": "dev", "scopes": ["branch:read"]},
		"claims": {"branches": ["main"]},
	}
}

test_valid_branch_deny_write_collection_route_without_wildcard if {
	not policy.valid_branch with input as {
		"request": {"path": "/organizations/:organizationID/projects/:projectID/branches", "scopes": ["branch:write"]},
		"claims": {"branches": ["main"]},
	}
}

test_valid_branch_allow_write_collection_route_with_wildcard if {
	policy.valid_branch with input as {
		"request": {"path": "/organizations/:organizationID/projects/:projectID/branches", "scopes": ["branch:write"]},
		"claims": {"branches": ["*"]},
	}
}

test_valid_branch_allow_read_collection_route_with_restricted_key if {
	policy.valid_branch with input as {
		"request": {"path": "/organizations/:organizationID/projects/:projectID/branches", "scopes": ["branch:read"]},
		"claims": {"branches": ["main"]},
	}
}

# allow rule tests
test_allow_simple_get if {
	policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/path",
			"scopes": ["read"],
		},
		"claims": {
			"scopes": ["read"],
			"organizations": {},
			"projects": [],
			"branches": [],
		},
	}
}

test_deny_missing_scope if {
	not policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/path",
			"scopes": ["read", "write"],
		},
		"claims": {
			"scopes": ["read"],
			"organizations": {},
			"projects": [],
			"branches": [],
		},
	}
}

test_allow_org_project_branch_access if {
	policy.allow with input as {
		"request": {
			"method": "POST",
			"path": "/orgs/:organizationID/projects/:projectID/branches/:branchID",
			"scopes": ["write"],
			"organization": "myorg",
			"project": "myproject",
			"branch": "mybranch",
		},
		"claims": {
			"scopes": ["write"],
			"organizations": {"myorg": {"ID": "myorg", "Status": "enabled"}},
			"projects": ["myproject"],
			"branches": ["mybranch"],
		},
	}
}

test_deny_org_mismatch if {
	not policy.allow with input as {
		"request": {
			"method": "POST",
			"path": "/orgs/:organizationID/projects/:projectID/branches/:branchID",
			"scopes": ["write"],
			"organization": "otherorg",
			"project": "myproject",
			"branch": "mybranch",
		},
		"claims": {
			"scopes": ["write"],
			"organizations": {"myorg": {"ID": "myorg", "Status": "enabled"}},
			"projects": ["myproject"],
			"branches": ["mybranch"],
		},
	}
}

test_valid_scope_allow_write_implies_read if {
	policy.valid_scope with input as {
		"request": {"scopes": ["branch:read"]},
		"claims": {"scopes": ["branch:write"]},
	}
}

test_valid_scope_deny_read_does_not_imply_write if {
	not policy.valid_scope with input as {
		"request": {"scopes": ["branch:write"]},
		"claims": {"scopes": ["branch:read"]},
	}
}

test_valid_scope_allow_mixed_read_and_write if {
	policy.valid_scope with input as {
		"request": {"scopes": ["branch:read", "branch:write"]},
		"claims": {"scopes": ["branch:write"]},
	}
}

test_valid_scope_deny_partial_missing_write if {
	not policy.valid_scope with input as {
		"request": {"scopes": ["branch:read", "branch:write"]},
		"claims": {"scopes": ["branch:read"]},
	}
}

test_allow_restricted_key_listing_branches_in_allowed_project if {
	policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/organizations/:organizationID/projects/:projectID/branches",
			"scopes": ["branch:read"],
			"organization": "org1",
			"project": "projA",
		},
		"claims": {
			"scopes": ["branch:read"],
			"organizations": {"org1": {"ID": "org1", "Status": "enabled"}},
			"projects": ["projA"],
			"branches": ["branchY"],
		},
	}
}

test_deny_restricted_key_creating_branch_in_allowed_project if {
	not policy.allow with input as {
		"request": {
			"method": "POST",
			"path": "/organizations/:organizationID/projects/:projectID/branches",
			"scopes": ["branch:write"],
			"organization": "org1",
			"project": "projA",
		},
		"claims": {
			"scopes": ["branch:write"],
			"organizations": {"org1": {"ID": "org1", "Status": "enabled"}},
			"projects": ["projA"],
			"branches": ["branchY"],
		},
	}
}

test_allow_restricted_key_listing_projects if {
	policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/organizations/:organizationID/projects",
			"scopes": ["project:read"],
			"organization": "org1",
		},
		"claims": {
			"scopes": ["project:read"],
			"organizations": {"org1": {"ID": "org1", "Status": "enabled"}},
			"projects": ["projA"],
			"branches": ["*"],
		},
	}
}

test_allow_wildcard_key_listing_projects if {
	policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/organizations/:organizationID/projects",
			"scopes": ["project:read"],
			"organization": "org1",
		},
		"claims": {
			"scopes": ["project:read"],
			"organizations": {"org1": {"ID": "org1", "Status": "enabled"}},
			"projects": ["*"],
			"branches": ["*"],
		},
	}
}

# Org-level routes work fine with a project/branch-restricted key.
test_allow_restricted_key_org_route if {
	policy.allow with input as {
		"request": {
			"method": "GET",
			"path": "/organizations/:organizationID/members",
			"scopes": ["org:read"],
			"organization": "org1",
		},
		"claims": {
			"scopes": ["org:read"],
			"organizations": {"org1": {"ID": "org1", "Status": "enabled"}},
			"projects": ["projA"],
			"branches": ["branchY"],
		},
	}
}

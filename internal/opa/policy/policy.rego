package policy

import rego.v1

default allow := false

allow if {
	valid_input
	valid_scope
	valid_organization
	valid_project
	valid_branch
}

# Input validation checks
valid_input if {
	input.request.method != ""
	input.request.path != ""
	input.request.scopes != null
	is_array(input.request.scopes)

	input.claims.organizations != null
	is_object(input.claims.organizations)
	input.claims.scopes != null
	is_array(input.claims.scopes)
	input.claims.projects != null
	is_array(input.claims.projects)
	input.claims.branches != null
	is_array(input.claims.branches)
}

# Scope check: all required scopes must be granted, unless wildcard is present
valid_scope if {
	"*" in input.claims.scopes
} else if {
	every scope in input.request.scopes {
		scope_allowed(scope, input.claims.scopes)
	}
}

scope_allowed(scope, claims) if {
	scope in claims
} else if {
	# Allow read scopes to be satisfied by corresponding write scopes
	# e.g., "project:read" can be satisfied by "project:write"
	endswith(scope, ":read")
	replace(scope, ":read", ":write") in claims
}

# Organization access check
valid_organization if {
	not contains(input.request.path, ":organizationID")
} else if {
	input.request.organization in object.keys(input.claims.organizations)
}

# Project access check: write on collection routes (no :projectID) requires wildcard.
valid_project if {
	contains(input.request.path, ":projectID")
	permission_granted(input.request.project, input.claims.projects)
}

valid_project if {
	not contains(input.request.path, ":projectID")
	not requires_project_write_scope
}

valid_project if {
	not contains(input.request.path, ":projectID")
	"*" in input.claims.projects
}

# input.request.scopes are the scopes the route requires, not the caller's claim scopes.
requires_project_write_scope if {
	"project:write" in input.request.scopes
}

# Branch access check: write on collection routes (no :branchID) requires wildcard.
valid_branch if {
	contains(input.request.path, ":branchID")
	permission_granted(input.request.branch, input.claims.branches)
}

valid_branch if {
	not contains(input.request.path, ":branchID")
	not requires_branch_write_scope
}

valid_branch if {
	not contains(input.request.path, ":branchID")
	"*" in input.claims.branches
}

# input.request.scopes are the scopes the route requires, not the caller's claim scopes.
requires_branch_write_scope if {
	"branch:write" in input.request.scopes
}

# Permission check with support for "*"
permission_granted(value, allowed) if {
	"*" in allowed
} else if {
	value in allowed
}

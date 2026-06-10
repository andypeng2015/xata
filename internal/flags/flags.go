package flags

import "xata/internal/openfeature"

var (
	// XataUser controls whether branch connection strings use the 'app' user (safe default).
	// Kill-switch: set to false to fall back to the superuser DSN.
	XataUser = openfeature.FeatureFlag{
		Name:           "xataUser",
		DefaultEnabled: true,
	}
	OrgAutoWindDown = openfeature.FeatureFlag{
		Name:           "orgAutoWindDown",
		DefaultEnabled: true,
	}
	OrganizationCreation = openfeature.FeatureFlag{
		Name:           "organizationCreation",
		DefaultEnabled: true,
	}
	// WARNING: Feature Flags should have positive names. Avoid disabled suffix in future
	BranchCreationDisabled = openfeature.FeatureFlag{
		Name:           "branchCreationDisabled",
		DefaultEnabled: false,
	}
	ChildBranchCreationDisabled = openfeature.FeatureFlag{
		Name:           "childBranchCreationDisabled",
		DefaultEnabled: false,
	}
	// ExperimentalImages flag to enable experimental PostgreSQL images (for internal users)
	ExperimentalImages = openfeature.FeatureFlag{
		Name:           "experimentalImages",
		DefaultEnabled: false,
	}
	// AnalyticsImages flag to enable analytics PostgreSQL images
	AnalyticsImages = openfeature.FeatureFlag{
		Name:           "analyticsImages",
		DefaultEnabled: false,
	}
	UseClusterPool = openfeature.FeatureFlag{
		Name:           "useClusterPool",
		DefaultEnabled: false,
	}
	UseXatastor = openfeature.FeatureFlag{
		Name:           "useXatastor",
		DefaultEnabled: false,
	}
	UsePgBackRest = openfeature.FeatureFlag{
		Name:           "usePgBackRest",
		DefaultEnabled: false,
	}
	PgAuditLogs = openfeature.FeatureFlag{
		Name:           "pgAuditLogs",
		DefaultEnabled: false,
	}
	// WARNING: Feature Flags should have positive names. Avoid disabled suffix in future
)

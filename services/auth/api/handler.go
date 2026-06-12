package api

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"xata/internal/token"

	"xata/internal/analytics"
	"xata/services/auth/billing"

	"github.com/labstack/echo/v4"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/internal/api"
	"xata/internal/api/key"
	"xata/internal/o11y"
	"xata/internal/openfeature"
	"xata/services/auth/api/spec"
	"xata/services/auth/keycloak"
	"xata/services/auth/store"
)

// MaxBulkDeleteAPIKeys is the maximum number of API keys that can be deleted in a single request
const MaxBulkDeleteAPIKeys = 50

// MaxAPIKeyNameLength caps the API key name field. Listings echo this back, so
// an uncapped name is DoS-adjacent (1MB name × N keys × repeated reads).
const MaxAPIKeyNameLength = 256

type publicHandler struct {
	keyCloak       keycloak.KeyCloak
	realm          string
	store          store.AuthStore
	feat           openfeature.Client
	analytics      analytics.Client
	projectsClient projectsv1.ProjectsServiceClient
	billing        billing.Client
	defaultOrg     spec.Organization
}

func NewPublicAPIHandler(feat openfeature.Client, keyCloak keycloak.KeyCloak, realm string, store store.AuthStore, projectsClient projectsv1.ProjectsServiceClient, billing billing.Client, analytics analytics.Client, defaultOrgID, defaultOrgName string) spec.ServerInterface {
	return &publicHandler{
		realm:          realm,
		keyCloak:       keyCloak,
		store:          store,
		feat:           feat,
		projectsClient: projectsClient,
		billing:        billing,
		analytics:      analytics,
		defaultOrg: spec.Organization{
			Id:   defaultOrgID,
			Name: defaultOrgName,
			Status: spec.OrganizationStatus{
				Status:          spec.Enabled,
				BillingStatus:   spec.Ok,
				DisabledByAdmin: false,
				UsageTier:       spec.T2,
				LastUpdated:     time.Now(),
			},
		},
	}
}

func (s *publicHandler) withOrganizationAccess(c echo.Context, organizationID spec.OrganizationID, fn func() error) error {
	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	if !claims.HasAccessToOrganization(organizationID) {
		return ErrorNoOrganizationAccess{OrganizationID: organizationID}
	}

	o11y.SetReqAttribute(c, api.OrganizationO11yK, organizationID)
	o11y.SetReqAttribute(c, api.UserIDO11yK, claims.UserID())

	return fn()
}

func (s *publicHandler) withAuthenticatedUser(c echo.Context, fn func(userId string) error) error {
	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	userID := claims.UserID()
	if userID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Endpoint only available for authenticated users")
	}

	o11y.SetReqAttribute(c, api.UserIDO11yK, userID)
	return fn(userID)
}

// Bulk delete API Keys for the authenticated user
// (DELETE /api-keys)
func (s *publicHandler) DeleteUserAPIKeys(ctx echo.Context) error {
	return s.withAuthenticatedUser(ctx, func(userId string) error {
		var body spec.DeleteUserAPIKeysJSONRequestBody
		if err := api.ReadBody(ctx, &body); err != nil {
			return err
		}

		// Limit the number of keys that can be deleted in a single request
		if len(body.Ids) > MaxBulkDeleteAPIKeys {
			return ErrorTooManyAPIKeys{Limit: MaxBulkDeleteAPIKeys, Count: len(body.Ids)}
		}

		err := s.store.DeleteAPIKeys(ctx.Request().Context(), store.KeyTargetUser, userId, body.Ids)
		if err != nil {
			return err
		}

		return ctx.NoContent(http.StatusNoContent)
	})
}

// List API Keys for the authenticated user
// (GET /api-keys)
func (s *publicHandler) ListUserAPIKeys(ctx echo.Context) error {
	return s.withAuthenticatedUser(ctx, func(userId string) error {
		apiKeys, err := s.store.ListAPIKeys(ctx.Request().Context(), store.KeyTargetUser, userId)
		if err != nil {
			return err
		}

		return ctx.JSON(http.StatusOK, struct {
			Keys []spec.APIKeyPreview `json:"keys"`
		}{mapAPIKeyPreviews(apiKeys)})
	})
}

// Create a User API Key
// (POST /api-keys)
func (s *publicHandler) CreateUserAPIKey(ctx echo.Context) error {
	return s.withAuthenticatedUser(ctx, func(userId string) error {
		var body spec.CreateUserAPIKeyJSONRequestBody
		if err := api.ReadBody(ctx, &body); err != nil {
			return err
		}

		return s.createAPIKey(ctx, store.KeyTargetUser, userId, body.Name, body.Expiry, body.Scopes, body.Projects, body.Branches)
	})
}

func (s *publicHandler) RegisterMarketplace(ctx echo.Context) error {
	return echo.ErrNotImplemented
}

// Get list of organizations
// (GET /organizations)
func (s *publicHandler) GetOrganizationsList(c echo.Context) error {
	return c.JSON(http.StatusOK, struct {
		Organizations []spec.Organization `json:"organizations"`
	}{Organizations: []spec.Organization{s.defaultOrg}})
}

func (s *publicHandler) CreateBillingCheckoutSession(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

func (s *publicHandler) CreateBillingPaymentMethodSession(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

func (s *publicHandler) GetBillingCustomer(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

func (s *publicHandler) UpdateBillingCustomer(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

func (s *publicHandler) GetBillingInvoices(ctx echo.Context, organizationID spec.OrganizationIDParam, params spec.GetBillingInvoicesParams) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

func (s *publicHandler) GetBillingUpcomingInvoice(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

// Create a new organization
// (POST /organizations)
func (s *publicHandler) CreateOrganization(ctx echo.Context) error {
	return echo.NewHTTPError(http.StatusForbidden, "organization creation not allowed")
}

// Delete an existing organization
// (DELETE /organizations/{organization_id})
func (s *publicHandler) DeleteOrganization(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return echo.NewHTTPError(http.StatusForbidden, "organization deletion not allowed")
}

// GetOrganizationMembershipLimits returns the membership limits for an organization
// (GET /organizations/{organizationID}/membership-limits)
func (s *publicHandler) GetOrganizationMembershipLimits(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		return echo.ErrNotImplemented
	})
}

// (POST /organizations/{organization_id}/deletion-request)
func (s *publicHandler) RequestOrganizationDeletion(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return echo.NewHTTPError(http.StatusForbidden, "organization deletion not allowed")
}

// Get an existing organization
// (GET /organizations/{organization_id})
func (s *publicHandler) GetOrganization(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	if organizationID != s.defaultOrg.Id {
		return echo.NewHTTPError(http.StatusNotFound, "organization not found")
	}
	return ctx.JSON(http.StatusOK, s.defaultOrg)
}

// Update an existing organization
// (PUT /organizations/{organization_id})
func (s *publicHandler) UpdateOrganization(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return echo.NewHTTPError(http.StatusForbidden, "organization update not allowed")
}

// Bulk delete API Keys for an organization
// (DELETE /organizations/{organizationID}/api-keys)
func (s *publicHandler) DeleteOrganizationAPIKeys(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		var body spec.DeleteOrganizationAPIKeysJSONRequestBody
		if err := api.ReadBody(ctx, &body); err != nil {
			return err
		}

		// Limit the number of keys that can be deleted in a single request
		if len(body.Ids) > MaxBulkDeleteAPIKeys {
			return ErrorTooManyAPIKeys{}
		}

		err := s.store.DeleteAPIKeys(ctx.Request().Context(), store.KeyTargetOrganization, organizationID, body.Ids)
		if err != nil {
			return err
		}

		return ctx.NoContent(http.StatusNoContent)
	})
}

// List API Keys for an organization
// (GET /organizations/{organizationID}/api-keys)
func (s *publicHandler) ListOrganizationAPIKeys(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		apiKeys, err := s.store.ListAPIKeys(ctx.Request().Context(), store.KeyTargetOrganization, organizationID)
		if err != nil {
			return err
		}

		return ctx.JSON(http.StatusOK, struct {
			Keys []spec.APIKeyPreview `json:"keys"`
		}{mapAPIKeyPreviews(apiKeys)})
	})
}

// Create an Organization API Key
// (POST /organizations/{organizationID}/api-keys)
func (s *publicHandler) CreateOrganizationAPIKey(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return s.withOrganizationAccess(ctx, organizationID, func() error {
		var body spec.CreateOrganizationAPIKeyJSONRequestBody
		if err := api.ReadBody(ctx, &body); err != nil {
			return err
		}

		return s.createAPIKey(ctx, store.KeyTargetOrganization, organizationID, body.Name, body.Expiry, body.Scopes, body.Projects, body.Branches)
	})
}

// List members of an organization
// (GET /organizations/{organizationID}/members)
func (s *publicHandler) ListOrganizationMembers(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return echo.ErrNotImplemented
}

// Remove a member from an organization
// (DELETE /organizations/{organizationID}/members/{user_id})
func (s *publicHandler) RemoveOrganizationMember(ctx echo.Context, organizationID spec.OrganizationIDParam, userID spec.UserIDParam) error {
	return echo.ErrNotImplemented
}

// Create an invitation to join an organization
// (POST /organizations/{organizationID}/invitations)
func (s *publicHandler) CreateOrganizationInvitation(ctx echo.Context, organizationID spec.OrganizationIDParam) error {
	return echo.ErrNotImplemented
}

func (s *publicHandler) ListOrganizationInvitations(ctx echo.Context, organizationID spec.OrganizationIDParam, params spec.ListOrganizationInvitationsParams) error {
	return echo.ErrNotImplemented
}

func (s *publicHandler) GetOrganizationInvitation(ctx echo.Context, organizationID spec.OrganizationIDParam, invitationID string) error {
	return echo.ErrNotImplemented
}

func (s *publicHandler) DeleteOrganizationInvitation(ctx echo.Context, organizationID spec.OrganizationIDParam, invitationID string) error {
	return echo.ErrNotImplemented
}

func (s *publicHandler) ResendOrganizationInvitation(ctx echo.Context, organizationID spec.OrganizationIDParam, invitationID string) error {
	return echo.ErrNotImplemented
}

// createAPIKey handles the logic for creating API keys (both user and organization)
func (s *publicHandler) createAPIKey(ctx echo.Context, targetType store.KeyTargetType, targetID string, name string, expiry *time.Time, providedScopes *[]string, providedProjects *[]string, providedBranches *[]string) error {
	if name == "" {
		return ErrorMissingRequiredField{Field: "name"}
	}
	if len(name) > MaxAPIKeyNameLength {
		return ErrInvalidName{Reason: fmt.Sprintf("must be at most %d characters", MaxAPIKeyNameLength)}
	}

	if expiry != nil && !expiry.After(time.Now()) {
		return ErrInvalidExpiry{Reason: "expiry must be in the future"}
	}

	// Reject mixed wildcard + specific values rather than silently upgrading to
	// "*", which would be the wrong direction for a least-privilege system.
	for _, p := range []struct {
		slice *[]string
		field string
	}{
		{providedScopes, "scopes"},
		{providedProjects, "projects"},
		{providedBranches, "branches"},
	} {
		if err := rejectMixedWildcard(p.slice, p.field); err != nil {
			return err
		}
	}

	scopes := cleanPermissionArray(providedScopes)
	projects := cleanPermissionArray(providedProjects)
	branches := cleanPermissionArray(providedBranches)

	claims := api.GetUserClaims(ctx)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	// Validate user-provided scopes against known scopes. Inherited scopes
	// (set below) are trusted: they passed validation when the parent was
	// created, and deprecating a scope must not break key rotation.
	for _, scope := range scopes {
		if scope != "*" && !slices.Contains(spec.GetAllScopes(), scope) {
			return ErrInvalidResourceRestrictions{Message: fmt.Sprintf("invalid scope: %s", scope)}
		}
	}

	// allowedScopes/Projects/Branches are the upper bound on what the new
	// key can hold. For user callers this is the unrestricted "*"; for API
	// key callers it's the parent key's own restrictions, used both for
	// inheriting omitted fields and for the subset check below — single
	// source of truth.
	allowedScopes := claims.Scopes
	allowedProjects := claims.Projects
	allowedBranches := claims.Branches

	if claims.APIKeyID() != "" {
		parent, err := s.store.GetAPIKey(ctx.Request().Context(), claims.APIKeyID())
		if err != nil {
			return fmt.Errorf("get parent api key: %w", err)
		}

		// A parent with empty permission lists would let the store-side
		// default of ["*"] leak through to the child. Refuse to use such
		// a parent rather than silently escalate.
		if len(parent.Scopes) == 0 || len(parent.Projects) == 0 || len(parent.Branches) == 0 {
			return ErrInvalidResourceRestrictions{Message: "parent API key has invalid permissions"}
		}

		allowedScopes = parent.Scopes
		allowedProjects = parent.Projects
		allowedBranches = parent.Branches

		if len(scopes) == 0 {
			scopes = slices.Clone(allowedScopes)
		}
		if len(projects) == 0 {
			projects = slices.Clone(allowedProjects)
		}
		if len(branches) == 0 {
			branches = slices.Clone(allowedBranches)
		}

		// The child must not outlive the parent: after the parent expires
		// the child would still be usable, which is a privilege escalation
		// in time.
		if parent.Expiry != nil {
			if expiry == nil || expiry.After(*parent.Expiry) {
				return ErrInvalidExpiry{Reason: "expiry must not exceed parent API key expiry"}
			}
		}
	}

	// Validate resource hierarchy exists. Skip wildcards — those were validated
	// at the parent level when the parent's permissions were granted.
	concreteProjects := projects
	if slices.Contains(concreteProjects, "*") {
		concreteProjects = nil
	}
	concreteBranches := branches
	if slices.Contains(concreteBranches, "*") {
		concreteBranches = nil
	}
	if len(concreteProjects) > 0 || len(concreteBranches) > 0 {
		_, err := s.projectsClient.ValidateHierarchy(ctx.Request().Context(), &projectsv1.ValidateHierarchyRequest{
			OrganizationIds: organizationIDs(claims.Organizations),
			ProjectIds:      concreteProjects,
			BranchIds:       concreteBranches,
		})
		if err != nil {
			return ErrInvalidResourceRestrictions{Message: err.Error()}
		}
	}

	// Validate scope access
	for _, s := range scopes {
		if !scopeAllowed(s, allowedScopes) {
			return ErrInvalidResourceRestrictions{Message: fmt.Sprintf("insufficient access to scope: %s", s)}
		}
	}

	// Validate project access
	if err := validateResourceList(projects, allowedProjects, "project"); err != nil {
		return err
	}

	// Validate branch access
	if err := validateResourceList(branches, allowedBranches, "branch"); err != nil {
		return err
	}

	// Create the API key
	token, apiKey, err := s.store.CreateAPIKey(ctx.Request().Context(), targetType, targetID, &store.APIKeyCreate{
		Name:         name,
		Expiry:       expiry,
		Scopes:       scopes,
		Projects:     projects,
		Branches:     branches,
		CreatedBy:    new(claims.UserID()),
		CreatedByKey: new(claims.APIKeyID()),
	})
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusCreated, struct {
		Key spec.FullAPIKey `json:"key"`
	}{
		Key: mapAPIKeyResponse(apiKey, token),
	})
}

func organizationIDs(orgs map[string]token.Organization) []string {
	ids := make([]string, len(orgs))
	i := 0
	for id := range orgs {
		ids[i] = id
		i++
	}
	return ids
}

// rejectMixedWildcard fails when a permission list mixes "*" with specific
// values. cleanPermissionArray collapses such input to ["*"], silently
// upgrading the caller's intent.
func rejectMixedWildcard(slice *[]string, field string) error {
	if slice == nil {
		return nil
	}
	if !slices.Contains(*slice, "*") {
		return nil
	}
	for _, s := range *slice {
		if s != "*" && s != "" {
			return ErrInvalidResourceRestrictions{Message: fmt.Sprintf("%s cannot mix '*' with specific values", field)}
		}
	}
	return nil
}

// cleanPermissionArray removes duplicate permissions and handles wildcard entries
func cleanPermissionArray(slice *[]string) []string {
	if slice == nil {
		return []string{}
	}

	if slices.Contains(*slice, "*") {
		// If wildcard is present, return it as the only value
		return []string{"*"}
	}

	seen := make(map[string]struct{}, len(*slice))
	result := make([]string, 0, len(*slice))

	for _, s := range *slice {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}

	return result
}

// scopeAllowed mirrors policy.rego's scope_allowed: write implies read,
// read does not imply write. Keep in sync with internal/opa/policy/policy.rego.
func scopeAllowed(scope string, allowed []string) bool {
	if slices.Contains(allowed, "*") || slices.Contains(allowed, scope) {
		return true
	}
	if base, ok := strings.CutSuffix(scope, ":read"); ok {
		return slices.Contains(allowed, base+":write")
	}
	return false
}

// validateResourceList validates access to a list of resources
func validateResourceList(requested, userAccess []string, resourceType string) error {
	if len(requested) == 0 {
		return nil
	}

	// Check if user has wildcard access
	if slices.Contains(userAccess, "*") {
		return nil
	}

	userResourceMap := make(map[string]bool, len(userAccess))
	for _, resource := range userAccess {
		userResourceMap[resource] = true
	}

	for _, resource := range requested {
		if !userResourceMap[resource] {
			return ErrInvalidResourceRestrictions{Message: fmt.Sprintf("insufficient access to %s: %s", resourceType, resource)}
		}
	}

	return nil
}

func mapAPIKeyResponse(apiKey *store.APIKey, token key.Key) spec.FullAPIKey {
	return spec.FullAPIKey{
		Id:           apiKey.ID,
		Name:         apiKey.Name,
		Preview:      apiKey.KeyPreview,
		Token:        token.String(),
		Expiry:       apiKey.Expiry,
		CreatedAt:    apiKey.CreatedAt,
		LastUsed:     apiKey.LastUsed,
		Scopes:       apiKey.Scopes,
		Projects:     apiKey.Projects,
		Branches:     apiKey.Branches,
		CreatedBy:    apiKey.CreatedBy,
		CreatedByKey: apiKey.CreatedByKey,
	}
}

func mapAPIKeyPreviews(apiKeys []store.APIKey) []spec.APIKeyPreview {
	keys := make([]spec.APIKeyPreview, 0, len(apiKeys))
	for _, k := range apiKeys {

		apiKeyPreview := spec.APIKeyPreview{
			Id:           k.ID,
			Name:         k.Name,
			Preview:      k.KeyPreview,
			Expiry:       k.Expiry,
			CreatedAt:    k.CreatedAt,
			LastUsed:     k.LastUsed,
			Scopes:       k.Scopes,
			Projects:     k.Projects,
			Branches:     k.Branches,
			CreatedBy:    k.CreatedBy,
			CreatedByKey: k.CreatedByKey,
		}
		keys = append(keys, apiKeyPreview)
	}

	return keys
}

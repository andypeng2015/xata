package rpc

import (
	"context"
	"fmt"
	"log"

	"xata/services/auth/orgs"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/internal/api/key"
	"xata/internal/token"
	"xata/services/auth/keycloak"

	authv1 "xata/gen/proto/auth/v1"
	"xata/internal/opa"
	"xata/services/auth/store"

	"github.com/Nerzal/gocloak/v13"
	"github.com/open-policy-agent/opa/v1/rego"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ensure AuthService implements GRPCService interface.
var _ authv1.AuthServiceServer = (*AuthService)(nil)

// AuthService is a GRPC service for interacting with auth service.
type AuthService struct {
	authv1.UnsafeAuthServiceServer

	kcRest         keycloak.KeyCloak
	realm          string
	kc             *gocloak.GoCloak
	store          store.AuthStore
	policy         rego.PreparedEvalQuery
	projectsClient projectsv1.ProjectsServiceClient
	orgs           orgs.Organizations
	defaultOrgID   string
}

// NewAuthService creates a new AuthService.
func NewAuthService(store store.AuthStore, kcClient *gocloak.GoCloak, kcRest keycloak.KeyCloak, projectsClient projectsv1.ProjectsServiceClient, orgs orgs.Organizations, realm, defaultOrgID string) *AuthService {
	r := rego.New(
		rego.Query("data.policy.allow"),
		rego.Module("policy.rego", opa.Policy),
	)

	policy, err := r.PrepareForEval(context.Background())
	if err != nil {
		log.Fatalf("failed to prepare OPA policy: %v", err)
	}

	return &AuthService{
		store:          store,
		kc:             kcClient,
		kcRest:         kcRest,
		realm:          realm,
		policy:         policy,
		projectsClient: projectsClient,
		orgs:           orgs,
		defaultOrgID:   defaultOrgID,
	}
}

// validateJWT checks if the provided string is a valid JWT token and returns the user claims.
func (a *AuthService) validateJWT(ctx context.Context, tokenStr string) (*token.Claims, error) {
	jwt, claims, err := a.kc.DecodeAccessToken(ctx, tokenStr, a.realm)
	if err != nil {
		return nil, &store.ErrFailedToDecodeJWT{Err: err}
	}

	if !jwt.Valid {
		return nil, &store.ErrInvalidJWTToken{}
	}

	userID, err := claims.GetSubject()
	if err != nil {
		return nil, fmt.Errorf("failed to get user ID from JWT claims: %w", err)
	}

	return a.buildUserClaims(ctx, userID)
}

// ValidateAccess checks if the given token can call the specified endpoint
func (a *AuthService) ValidateAccess(ctx context.Context, req *authv1.ValidateAccessRequest) (*authv1.ValidateAccessResponse, error) {
	tokenStr := req.GetToken()

	var claims *token.Claims
	var err error
	k := key.Key(tokenStr)
	if !k.IsValid() {
		// Not a valid API key, run JWT validation
		claims, err = a.validateJWT(ctx, tokenStr)
		if err != nil {
			return nil, err
		}
	} else {
		// Get API Key claims
		storeAPIKey, err := a.store.ValidateAPIKey(ctx, k)
		if err != nil {
			return nil, err
		}

		switch storeAPIKey.TargetType {
		case store.KeyTargetOrganization:
			orgClaims, err := a.buildOrgClaims(ctx, storeAPIKey.TargetID)
			if err != nil {
				return nil, err
			}

			orgClaims.Scopes = storeAPIKey.Scopes
			orgClaims.Projects = storeAPIKey.Projects
			orgClaims.Branches = storeAPIKey.Branches
			orgClaims.KeyID = storeAPIKey.ID
			claims = orgClaims
		case store.KeyTargetUser:
			userClaims, err := a.buildUserClaims(ctx, storeAPIKey.TargetID)
			if err != nil {
				return nil, err
			}

			userClaims.Scopes = storeAPIKey.Scopes
			userClaims.Projects = storeAPIKey.Projects
			userClaims.Branches = storeAPIKey.Branches
			userClaims.KeyID = storeAPIKey.ID
			claims = userClaims
		default:
			return nil, &store.ErrInvalidAPIKeyTargetType{TargetType: string(storeAPIKey.TargetType)}
		}
	}

	// Check if the request is allowed by the policy
	policyOrgs := make(map[string]opa.Organization)
	for orgID, orgStatus := range claims.Organizations {
		policyOrgs[orgID] = opa.Organization{
			ID:     orgStatus.ID,
			Status: orgStatus.Status,
		}
	}
	input := opa.PolicyInput{
		Request: opa.RequestInput{
			Method:       req.GetMethod(),
			Path:         req.GetPath(),
			Scopes:       req.GetScopes(),
			Organization: req.GetOrganizationId(),
			Project:      req.GetProjectId(),
			Branch:       req.GetBranchId(),
		},
		Claims: opa.ClaimsInput{
			Scopes:        claims.Scopes,
			Organizations: policyOrgs,
			Projects:      claims.Projects,
			Branches:      claims.Branches,
		},
	}

	allowed := false
	res, err := a.policy.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return nil, fmt.Errorf("OPA policy evaluation failed: %w", err)
	}

	if len(res) > 0 && len(res[0].Expressions) > 0 {
		if val, ok := res[0].Expressions[0].Value.(bool); ok {
			allowed = val
		}
	}

	specOrgs := make(map[string]*authv1.Organization)
	for orgID, orgStatus := range claims.Organizations {
		specOrgs[orgID] = &authv1.Organization{
			Id:        orgStatus.ID,
			Status:    orgStatus.Status,
			CreatedAt: timestamppb.New(orgStatus.CreatedAt),
			UsageTier: orgStatus.UsageTier,
		}
	}
	return &authv1.ValidateAccessResponse{
		Allow:         allowed,
		UserId:        claims.UserID(),
		ApiKeyId:      claims.APIKeyID(),
		UserEmail:     claims.UserEmail(),
		Scopes:        claims.Scopes,
		Organizations: specOrgs,
		Projects:      claims.Projects,
		Branches:      claims.Branches,
	}, nil
}

func (a *AuthService) UpdateOrganization(ctx context.Context, req *authv1.UpdateOrganizationRequest) (*authv1.UpdateOrganizationResponse, error) {
	org, err := a.orgs.UpdateOrganization(ctx, req.GetOrganizationId(), orgs.UpdateOrganizationOptions{
		DisabledByAdmin:       &req.DisabledByAdmin,
		DisabledByAdminReason: req.DisabledByAdminReason,
	})
	if err != nil {
		return nil, err
	}
	return &authv1.UpdateOrganizationResponse{
		Organization: &authv1.Organization{
			Id:                    org.Id,
			Status:                string(org.Status.Status),
			DisabledByAdmin:       org.Status.DisabledByAdmin,
			DisabledByAdminReason: org.Status.AdminReason,
			BillingStatus:         string(org.Status.BillingStatus),
			BillingReason:         org.Status.BillingReason,
		},
	}, nil
}

// buildUserClaims constructs a token.Claims object for a user based on their Keycloak user ID.
func (a *AuthService) buildUserClaims(ctx context.Context, userID string) (*token.Claims, error) {
	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}

	user, err := a.kcRest.GetUserRepresentation(ctx, a.realm, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user from Keycloak: %w", err)
	}
	if user.ID == "" {
		return nil, fmt.Errorf("user not found in Keycloak: %s", userID)
	}

	orgList, err := a.kcRest.ListOrganizations(ctx, a.realm, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list user organizations: %w", err)
	}

	organizations := make(map[string]token.Organization, len(orgList))
	for _, org := range orgList {
		tokenOrg := token.Organization{
			ID:        org.Id,
			Status:    string(org.Status.Status),
			UsageTier: string(org.Status.UsageTier),
		}
		if org.Status.CreatedAt != nil {
			tokenOrg.CreatedAt = *org.Status.CreatedAt
		}
		organizations[org.Id] = tokenOrg
	}

	if a.defaultOrgID != "" {
		if _, exists := organizations[a.defaultOrgID]; !exists {
			organizations[a.defaultOrgID] = token.Organization{
				ID:     a.defaultOrgID,
				Status: token.OrgEnabledStatus,
			}
		}
	}

	return &token.Claims{
		ID:            user.ID,
		Email:         user.Email,
		Organizations: organizations,
		Scopes:        []string{"*"},
		Projects:      []string{"*"},
		Branches:      []string{"*"},
	}, nil
}

func (a *AuthService) buildOrgClaims(ctx context.Context, orgID string) (*token.Claims, error) {
	if orgID == "" {
		return nil, fmt.Errorf("organization ID cannot be empty")
	}

	organization, err := a.kcRest.GetOrganization(ctx, a.realm, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to get organization [%s] from Keycloak: %w", orgID, err)
	}

	organizations := make(map[string]token.Organization, 1)
	tokenOrg := token.Organization{
		ID:        organization.Id,
		Status:    string(organization.Status.Status),
		UsageTier: string(organization.Status.UsageTier),
	}
	if organization.Status.CreatedAt != nil {
		tokenOrg.CreatedAt = *organization.Status.CreatedAt
	}
	organizations[organization.Id] = tokenOrg
	return &token.Claims{
		Organizations: organizations,
		Scopes:        []string{"*"},
		Projects:      []string{"*"},
		Branches:      []string{"*"},
	}, nil
}

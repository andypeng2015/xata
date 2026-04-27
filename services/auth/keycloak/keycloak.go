package keycloak

import (
	"context"
	"fmt"

	"xata/services/auth/api/spec"
)

type User struct {
	ID            string              `json:"id"`
	Username      string              `json:"username"`
	FirstName     string              `json:"firstName"`
	LastName      string              `json:"lastName"`
	Email         string              `json:"email"`
	EmailVerified bool                `json:"emailVerified"`
	Attributes    map[string][]string `json:"attributes,omitempty"`

	Marketplace   string `json:"-"`
	AWSCustomerID string `json:"-"`
	AWSProductID  string `json:"-"`
	AWSAccountID  string `json:"-"`
}

type Domain struct {
	Name string `json:"name"`
}

// MaxOrganizationMembers is the maximum number of users allowed in an organization
const MaxOrganizationMembers = 100

type KeycloakOrganization struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Alias       string              `json:"alias"`
	Description string              `json:"description"`
	Domains     []Domain            `json:"domains"`
	Attributes  map[string][]string `json:"attributes,omitempty"`
	RedirectURL string              `json:"redirectUrl,omitempty"`
}

type OrganizationCreate struct {
	Name        string
	Marketplace MarketplaceProvider
	UsageTier   spec.OrganizationStatusUsageTier
}

func (o OrganizationCreate) usageTierOrDefault() spec.OrganizationStatusUsageTier {
	if o.UsageTier == "" {
		return spec.T1
	}
	return o.UsageTier
}

type MarketplaceProvider interface {
	Validate() error
	BuildKeycloakAttributes() map[string][]string
}

const AWSMarketplaceProviderName = "aws"

type AWSMarketplace struct {
	CustomerID string
	ProductID  string
	AccountID  string
}

func (a AWSMarketplace) Validate() error {
	if a.CustomerID == "" {
		return fmt.Errorf("aws marketplace: customerID is required")
	}
	if a.ProductID == "" {
		return fmt.Errorf("aws marketplace: productID is required")
	}
	if a.AccountID == "" {
		return fmt.Errorf("aws marketplace: accountID is required")
	}
	return nil
}

func (a AWSMarketplace) BuildKeycloakAttributes() map[string][]string {
	return map[string][]string{
		OrganizationMarketplaceKey:   {AWSMarketplaceProviderName},
		OrganizationAWSCustomerIDKey: {a.CustomerID},
		OrganizationAWSProductIDKey:  {a.ProductID},
		OrganizationAWSAccountIDKey:  {a.AccountID},
	}
}

type OrganizationUpdate struct {
	Name               *string `json:"name"`
	BillingStatus      *string `json:"billingStatus,omitempty"`
	BillingReason      *string `json:"billingReason,omitempty"`
	AdminReason        *string `json:"adminReason,omitempty"`
	DisabledByAdmin    *bool   `json:"disabledByAdmin,omitempty"`
	ResourcesCleanedAt *string `json:"resourcesCleanedAt,omitempty"`
	UsageTier          *string `json:"usageTier,omitempty"`
}

type UserAttributesUpdate struct {
	Marketplace             *string `json:"marketplace,omitempty"`
	MarketplaceRegisteredAt *string `json:"marketplaceRegisteredAt,omitempty"`
	AWSAccountID            *string `json:"awsAccountId,omitempty"`
	AWSCustomerID           *string `json:"awsCustomerId,omitempty"`
	AWSProductID            *string `json:"awsProductId,omitempty"`
}

type OrganizationInvitation struct {
	ID             string  `json:"id"`
	OrganizationID string  `json:"organizationId"`
	Email          string  `json:"email"`
	FirstName      *string `json:"firstName,omitempty"`
	LastName       *string `json:"lastName,omitempty"`
	CreatedAt      int64   `json:"sentDate"`
	ExpiresAt      int64   `json:"expiresAt"`
	Status         string  `json:"status"`
	InviteLink     string  `json:"inviteLink"`
}

type ListInvitationsParams struct {
	Status    *string
	Email     *string
	FirstName *string
	LastName  *string
	Search    *string
	First     *int
	Max       *int
}

//go:generate go run github.com/vektra/mockery/v3 --with-expecter --name KeyCloak

// KeyCloak is the interface for interacting with the KeyCloak service.
type KeyCloak interface {
	// CreateOrganization creates a new organization in the given realm.
	CreateOrganization(c context.Context, realm string, params OrganizationCreate) (spec.Organization, error)
	// GetOrganization returns the organization by name in the given realm.
	GetOrganization(c context.Context, realm, name string) (spec.Organization, error)
	// ListOrganizations returns a list of organizations the user is a member of in the given realm.
	ListOrganizations(c context.Context, realm, userID string) ([]spec.Organization, error)
	// AddMember adds a user to the organization in the given realm.
	AddMember(c context.Context, realm string, organizationID string, userID string) error
	// RemoveMember removes a user from the organization in the given realm.
	RemoveMember(c context.Context, realm string, organizationID string, userID string) error
	// ListMembers lists all members of the organization in the given realm.
	ListMembers(c context.Context, realm string, organizationID string) ([]spec.UserWithID, error)
	// CreateInvitation sends an invitation for a user to join the organization.
	CreateInvitation(c context.Context, realm string, organizationID string, email string) error
	// ListInvitations retrieves all invitations for an organization with optional filtering.
	ListInvitations(c context.Context, realm string, organizationID string, params ListInvitationsParams) ([]OrganizationInvitation, error)
	// GetInvitation retrieves a specific invitation by ID.
	GetInvitation(c context.Context, realm string, organizationID string, invitationID string) (OrganizationInvitation, error)
	// ResendInvitation resends a pending invitation with a fresh expiration.
	ResendInvitation(c context.Context, realm string, organizationID string, invitationID string) error
	// DeleteInvitation permanently deletes an invitation record.
	DeleteInvitation(c context.Context, realm string, organizationID string, invitationID string) error
	// UpdateOrganization updates an organization's attributes in the given realm.
	UpdateOrganization(c context.Context, realm, organizationID string, update OrganizationUpdate) (spec.Organization, error)
	// DeleteOrganization marks an organization as deleted by setting the deletedAt attribute.
	DeleteOrganization(ctx context.Context, realm, organizationID string) error
	// GetUserRepresentation returns the user representation for the given user ID in the given realm.
	GetUserRepresentation(c context.Context, realm string, userID string) (User, error)
	// ListDisabledOrganizations returns organizations where disabledByAdmin=true OR billingStatus!=ok.
	// When returnCleanedUpOrgs is false, orgs with a resourcesCleanedAt attribute are excluded.
	ListDisabledOrganizations(ctx context.Context, realm string, returnCleanedUpOrgs bool) ([]spec.Organization, error)
	// UpdateUserAttributes merges the given attributes into the user's existing attributes.
	UpdateUserAttributes(ctx context.Context, realm, userID string, update UserAttributesUpdate) error
}

package token

import (
	"slices"
	"time"
)

const OrgEnabledStatus = "enabled"

// Claims present in the access token
type Claims struct {
	Organizations map[string]Organization `json:"organizations"`

	// The user's unique identifier
	ID string `json:"id"`
	// The API key ID if this claim was created from an API key
	KeyID string `json:"key_id,omitempty"`

	// The user's email address
	Email string `json:"email"`

	// Resource restrictions for API keys
	Scopes   []string `json:"scopes"`
	Projects []string `json:"projects,omitempty"`
	Branches []string `json:"branches,omitempty"`
}

type Organization struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UsageTier string    `json:"usage_tier"`
}

func (o *Organization) IsNewOrganization() bool {
	if o == nil || o.CreatedAt.IsZero() {
		return false
	}
	return time.Since(o.CreatedAt) < 12*time.Hour
}

// UserID returns the UserID of the claim.
// Returns an empty string if the claim is nil or no userID is set.
func (c *Claims) UserID() string {
	if c == nil {
		return ""
	}
	return c.ID
}

func (c *Claims) UserEmail() string {
	if c == nil {
		return ""
	}
	return c.Email
}

// APIKeyID returns the API key ID if this claim was created from an API key
func (c *Claims) APIKeyID() string {
	if c == nil {
		return ""
	}
	return c.KeyID
}

// HasAccessToOrganization checks if the claim allows access to the specified organization
func (c *Claims) HasAccessToOrganization(organizationID string) bool {
	if c == nil || organizationID == "" {
		return false
	}

	if _, ok := c.Organizations[organizationID]; ok {
		return true
	}
	return false
}

func (c *Claims) IsEnabledOrganization(organizationID string) bool {
	if c == nil || organizationID == "" {
		return false
	}
	if o, ok := c.Organizations[organizationID]; ok && o.Status == OrgEnabledStatus {
		return true
	}

	return false
}

func (c *Claims) HasAccessToProject(projectID string) bool {
	if c == nil || projectID == "" {
		return false
	}
	return slices.Contains(c.Projects, "*") || slices.Contains(c.Projects, projectID)
}

func (c *Claims) HasAccessToBranch(branchID string) bool {
	if c == nil || branchID == "" {
		return false
	}
	return slices.Contains(c.Branches, "*") || slices.Contains(c.Branches, branchID)
}

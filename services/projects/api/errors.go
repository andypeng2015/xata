package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"xata/internal/xvalidator"
)

const (
	MaxBranchDescriptionLength = 50
)

var validBranchDescriptionRegex = regexp.MustCompile(`^[a-zA-Z0-9]+[a-zA-Z0-9- ]*$`)

type ErrorInvalidDescription struct {
	Message     string
	Description string
}

func (e ErrorInvalidDescription) Error() string {
	return fmt.Sprintf("description %s invalid: %s", e.Description, e.Message)
}

func (e ErrorInvalidDescription) StatusCode() int {
	return http.StatusBadRequest
}

func IsBranchDescriptionValid(e string) error {
	if len(e) > MaxBranchDescriptionLength {
		return xvalidator.ErrorMaxLength{
			Limit: MaxBranchDescriptionLength,
		}
	}
	if !validBranchDescriptionRegex.MatchString(e) {
		return ErrorInvalidDescription{
			Description: e,
			Message:     fmt.Sprintf("invalid branch description %s", e),
		}
	}
	return nil
}

type ErrorInvalidParam struct {
	ProjectID  string
	BranchName string
	Param      string
	Message    string
}

func (e ErrorInvalidParam) Error() string {
	errMsg := strings.Builder{}
	if e.ProjectID != "" {
		fmt.Fprintf(&errMsg, "Project [%s]: ", e.ProjectID)
	}
	if e.BranchName != "" {
		fmt.Fprintf(&errMsg, "Branch [%s]: ", e.BranchName)
	}

	fmt.Fprintf(&errMsg, "invalid parameter [%s]: %s", e.Param, e.Message)
	return errMsg.String()
}

func (e ErrorInvalidParam) StatusCode() int {
	return http.StatusBadRequest
}

type ErrorBranchNotFound struct {
	BranchID string
}

func (e ErrorBranchNotFound) Error() string {
	return fmt.Sprintf("Branch [%s]: not found", e.BranchID)
}

func (e ErrorBranchNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrorCredentialsForBranchNotFound struct {
	BranchID string
	Username string
}

func (e ErrorCredentialsForBranchNotFound) Error() string {
	return fmt.Sprintf("Credentials for username [%s] on branch [%s]: not found", e.Username, e.BranchID)
}

func (e ErrorCredentialsForBranchNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrorBranchCreationDisabled struct{}

func (e ErrorBranchCreationDisabled) Error() string {
	return "Branch creation is temporarily disabled"
}

func (e ErrorBranchCreationDisabled) StatusCode() int {
	return http.StatusServiceUnavailable
}

type ErrorOrganizationDisabled struct {
	OrganizationID string
}

func (e ErrorOrganizationDisabled) Error() string {
	return fmt.Sprintf("Organization [%s] is disabled, please check your billing settings or contact support", e.OrganizationID)
}

func (e ErrorOrganizationDisabled) StatusCode() int {
	return http.StatusForbidden
}

type ErrorChildBranchCreationDisabled struct{}

func (e ErrorChildBranchCreationDisabled) Error() string {
	return "Child branch creation is temporarily disabled"
}

func (e ErrorChildBranchCreationDisabled) StatusCode() int {
	return http.StatusServiceUnavailable
}

type ErrorParentBranchUnhealthy struct {
	ParentID string
}

func (e ErrorParentBranchUnhealthy) Error() string {
	return fmt.Sprintf("Cannot create child branch because parent branch [%s] is not healthy", e.ParentID)
}

func (e ErrorParentBranchUnhealthy) StatusCode() int {
	return http.StatusPreconditionFailed
}

type ErrorBranchUpdateForbidden struct {
	BranchID string
}

func (e ErrorBranchUpdateForbidden) Error() string {
	// Assume that forbidden branch updates are temporary for now, they should
	// only originate from Kubernetes admission policies used to temporarily
	// block updates.
	return fmt.Sprintf("Branch [%s] update is temporarily unavailable", e.BranchID)
}

func (e ErrorBranchUpdateForbidden) StatusCode() int {
	return http.StatusForbidden
}

type ErrorBackupNotFound struct {
	ID string
}

func (e ErrorBackupNotFound) Error() string {
	return fmt.Sprintf("Backup [%s]: not found", e.ID)
}

func (e ErrorBackupNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrorBranchConflict struct {
	BranchID string
}

func (e ErrorBranchConflict) Error() string {
	return fmt.Sprintf("branch [%s] was modified concurrently, please retry", e.BranchID)
}

func (e ErrorBranchConflict) StatusCode() int {
	return http.StatusConflict
}

type ErrorNewOrgBranchLimitExceeded struct {
	OrganizationID string
}

func (e ErrorNewOrgBranchLimitExceeded) Error() string {
	return fmt.Sprintf("Organization [%s] has reached the branch limit for new organizations", e.OrganizationID)
}

func (e ErrorNewOrgBranchLimitExceeded) StatusCode() int {
	return http.StatusForbidden
}

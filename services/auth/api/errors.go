package api

import (
	"fmt"
	"net/http"
)

type ErrorOrganizationCreationDisabled struct{}

func (e ErrorOrganizationCreationDisabled) Error() string {
	return "Organization creation is temporarily disabled"
}

func (e ErrorOrganizationCreationDisabled) StatusCode() int {
	return http.StatusForbidden
}

// ErrorMissingRequiredField is returned when a required field is missing
type ErrorMissingRequiredField struct {
	Field string
}

func (e ErrorMissingRequiredField) Error() string {
	return fmt.Sprintf("%s is required", e.Field)
}

func (e ErrorMissingRequiredField) StatusCode() int {
	return http.StatusBadRequest
}

// ErrorNoOrganizationAccess is returned when a user doesn't have access to an organization
type ErrorNoOrganizationAccess struct {
	OrganizationID string
}

func (e ErrorNoOrganizationAccess) Error() string {
	return fmt.Sprintf("No access to organization %s", e.OrganizationID)
}

func (e ErrorNoOrganizationAccess) StatusCode() int {
	return http.StatusForbidden
}

// ErrorTooManyAPIKeys is returned when attempting to delete too many API keys at once
type ErrorTooManyAPIKeys struct {
	Limit int
	Count int
}

func (e ErrorTooManyAPIKeys) Error() string {
	return fmt.Sprintf("Cannot delete %d API keys at once. Maximum allowed is %d", e.Count, e.Limit)
}

func (e ErrorTooManyAPIKeys) StatusCode() int {
	return http.StatusBadRequest
}

// ErrorCannotRemoveSelf is returned when a user tries to remove themselves from an organization
type ErrorCannotRemoveSelf struct{}

func (e ErrorCannotRemoveSelf) Error() string {
	return "You cannot remove yourself from an organization"
}

func (e ErrorCannotRemoveSelf) StatusCode() int {
	return http.StatusForbidden
}

// ErrInvalidResourceRestrictions is returned when resource restrictions are invalid
type ErrInvalidResourceRestrictions struct {
	Message string
}

func (e ErrInvalidResourceRestrictions) Error() string {
	return fmt.Sprintf("invalid resource restrictions: %s", e.Message)
}

func (e ErrInvalidResourceRestrictions) StatusCode() int {
	return http.StatusBadRequest
}

// ErrInvalidExpiry is returned when the requested API key expiry is invalid.
type ErrInvalidExpiry struct {
	Reason string
}

func (e ErrInvalidExpiry) Error() string {
	return fmt.Sprintf("invalid expiry: %s", e.Reason)
}

func (e ErrInvalidExpiry) StatusCode() int {
	return http.StatusBadRequest
}

// ErrInvalidName is returned when the API key name is invalid (e.g. too long).
type ErrInvalidName struct {
	Reason string
}

func (e ErrInvalidName) Error() string {
	return fmt.Sprintf("invalid name: %s", e.Reason)
}

func (e ErrInvalidName) StatusCode() int {
	return http.StatusBadRequest
}

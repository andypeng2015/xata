package api

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"

	"xata/services/auth/billing"

	"xata/gen/protomocks"
	analyticsmocks "xata/internal/analytics/client/mocks"
	"xata/internal/api/key"
	"xata/internal/apitest"
	"xata/internal/apitest/validation"
	openfeaturetest "xata/internal/openfeature/client/mocks"
	"xata/internal/token"
	"xata/services/auth/api/spec"
	keycloakMocks "xata/services/auth/keycloak/mocks"
	"xata/services/auth/store"
	storeMocks "xata/services/auth/store/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var authSpec *openapi3.T

func TestMain(m *testing.M) {
	var err error
	authSpec, err = validation.LoadAuthSpec()
	if err != nil {
		log.Fatalf("failed to load auth spec: %v", err)
	}
	os.Exit(m.Run())
}

func TestOrganizations(t *testing.T) {
	const defaultOrgID = "default-org"
	const defaultOrgName = "Default Organization"

	t.Run("CreateOrganization", func(t *testing.T) {
		t.Run("create organization returns forbidden", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, _ := e.POST("/organizations").WithJSONBody(map[string]string{"name": "new-org"}).Context()
			err := handler.CreateOrganization(c)

			require.Error(t, err)
			var httpErr *echo.HTTPError
			require.True(t, errors.As(err, &httpErr))
			assert.Equal(t, http.StatusForbidden, httpErr.Code)
		})
	})

	t.Run("ListOrganizations", func(t *testing.T) {
		t.Run("list organizations returns default org", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, rec := e.GET("/organizations").Context()
			err := handler.GetOrganizationsList(c)

			require.NoError(t, err)
			var resp struct {
				Organizations []spec.Organization `json:"organizations"`
			}
			rec.MustCode(http.StatusOK)
			rec.ReadBody(&resp)

			require.Len(t, resp.Organizations, 1)
			assert.Equal(t, defaultOrgID, resp.Organizations[0].Id)
			assert.Equal(t, defaultOrgName, resp.Organizations[0].Name)
		})
	})

	t.Run("GetOrganization", func(t *testing.T) {
		t.Run("get default organization succeeds", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, rec := e.GET("/organizations/" + defaultOrgID).Context()
			err := handler.GetOrganization(c, defaultOrgID)

			require.NoError(t, err)
			var resp spec.Organization
			rec.MustCode(http.StatusOK)
			rec.ReadBody(&resp)

			assert.Equal(t, defaultOrgID, resp.Id)
			assert.Equal(t, defaultOrgName, resp.Name)
		})

		t.Run("get non-existent organization returns not found", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, _ := e.GET("/organizations/non-existent").Context()
			err := handler.GetOrganization(c, "non-existent")

			require.Error(t, err)
			var httpErr *echo.HTTPError
			require.True(t, errors.As(err, &httpErr))
			assert.Equal(t, http.StatusNotFound, httpErr.Code)
		})
	})

	t.Run("UpdateOrganization", func(t *testing.T) {
		t.Run("update organization returns forbidden", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, _ := e.PUT("/organizations/" + defaultOrgID).WithJSONBody(map[string]string{"name": "new-name"}).Context()
			err := handler.UpdateOrganization(c, defaultOrgID)

			require.Error(t, err)
			var httpErr *echo.HTTPError
			require.True(t, errors.As(err, &httpErr))
			assert.Equal(t, http.StatusForbidden, httpErr.Code)
		})
	})

	t.Run("DeleteOrganization", func(t *testing.T) {
		t.Run("delete organization returns forbidden", func(t *testing.T) {
			mockKc := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjectsClient := protomocks.NewProjectsServiceClient(t)

			handler := NewPublicAPIHandler(feat, mockKc, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

			c, _ := e.DELETE("/organizations/" + defaultOrgID).Context()
			err := handler.DeleteOrganization(c, defaultOrgID)

			require.Error(t, err)
			var httpErr *echo.HTTPError
			require.True(t, errors.As(err, &httpErr))
			assert.Equal(t, http.StatusForbidden, httpErr.Code)
		})
	})
}

func TestAPIKeys(t *testing.T) {
	// Common setup
	mockKC := keycloakMocks.NewKeyCloak(t)
	mockStore := storeMocks.NewAuthStore(t)
	feat := openfeaturetest.NewClient(nil)
	mockProjectsClient := protomocks.NewProjectsServiceClient(t)
	handler := NewPublicAPIHandler(feat, mockKC, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), "default-org", "Default Organization")
	e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)

	// Test data
	now := time.Now()
	fixedTime := time.Date(2025, 6, 4, 12, 0, 0, 0, time.UTC)
	fixedExpiry := now.Add(24 * time.Hour)

	t.Run("OrganizationAPIKeys", func(t *testing.T) {
		// Test cases for creating organization API keys
		createOrgKeyTests := []struct {
			name           string
			keyName        string
			jsonBody       any
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
			validateResp   func(t *testing.T, resp struct {
				Key spec.FullAPIKey `json:"key"`
			})
		}{
			{
				name:    "create organization api key succeeds without expiry",
				keyName: "test-org-key",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:   "test-org-key",
					Expiry: nil,
				},
				setupMocks: func() {
					apiKeyResult := &store.APIKey{
						ID:         "generated-id",
						Name:       "test-org-key",
						KeyPreview: "abc123",
						TargetType: store.KeyTargetOrganization,
						TargetID:   apitest.TestOrganization,
						Expiry:     nil,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						Scopes:     []string{},
						Projects:   []string{},
						Branches:   []string{},
					}
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-org-key" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-secret-key"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-org-key", resp.Key.Name)
					assert.Equal(t, "full-secret-key", resp.Key.Token)
					assert.Equal(t, "abc123", resp.Key.Preview)
					assert.Nil(t, resp.Key.Expiry)
				},
			},
			{
				name:    "create organization api key succeeds with expiry",
				keyName: "test-org-key-with-expiry",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:   "test-org-key-with-expiry",
					Expiry: &fixedExpiry,
				},
				setupMocks: func() {
					apiKeyResult := &store.APIKey{
						ID:         "generated-id-2",
						Name:       "test-org-key-with-expiry",
						KeyPreview: "def456",
						TargetType: store.KeyTargetOrganization,
						TargetID:   apitest.TestOrganization,
						Expiry:     &fixedExpiry,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						Branches:   []string{},
						Projects:   []string{},
						Scopes:     []string{},
					}
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-org-key-with-expiry" &&
								apiKeyCreate.Expiry != nil &&
								apiKeyCreate.Expiry.Equal(fixedExpiry) &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-secret-key-2"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-org-key-with-expiry", resp.Key.Name)
					assert.Equal(t, "full-secret-key-2", resp.Key.Token)
					assert.Equal(t, "def456", resp.Key.Preview)
					assert.NotNil(t, resp.Key.Expiry)
					assert.Equal(t, fixedExpiry.Format(time.RFC3339), resp.Key.Expiry.Format(time.RFC3339))
				},
			},
			{
				name:    "create organization api key with scopes and restrictions",
				keyName: "test-org-key-restricted",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:     "test-org-key-restricted",
					Scopes:   &[]string{"project:read", "branch:read"},
					Projects: &[]string{"proj1"},
					Branches: &[]string{"main"},
				},
				setupMocks: func() {
					apiKeyResult := &store.APIKey{
						ID:         "org-key-restricted",
						Name:       "test-org-key-restricted",
						KeyPreview: "abc456",
						TargetType: store.KeyTargetOrganization,
						TargetID:   apitest.TestOrganization,
						Expiry:     nil,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						Scopes:     []string{"project:read", "branch:read"},
						Projects:   []string{"proj1"},
						Branches:   []string{"main"},
					}
					mockProjectsClient.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).
						Return(nil, nil)
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-org-key-restricted" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 2 &&
								apiKeyCreate.Scopes[0] == "project:read" &&
								apiKeyCreate.Scopes[1] == "branch:read" &&
								len(apiKeyCreate.Projects) == 1 &&
								apiKeyCreate.Projects[0] == "proj1" &&
								len(apiKeyCreate.Branches) == 1 &&
								apiKeyCreate.Branches[0] == "main" &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-org-key-restricted"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-org-key-restricted", resp.Key.Name)
					assert.Equal(t, "full-org-key-restricted", resp.Key.Token)
					assert.Equal(t, "abc456", resp.Key.Preview)
					assert.Nil(t, resp.Key.Expiry)
					assert.NotNil(t, resp.Key.Scopes)
					assert.Contains(t, resp.Key.Scopes, "project:read")
					assert.Contains(t, resp.Key.Scopes, "branch:read")
					assert.NotNil(t, resp.Key.Projects)
					assert.Contains(t, resp.Key.Projects, "proj1")
					assert.NotNil(t, resp.Key.Branches)
					assert.Contains(t, resp.Key.Branches, "main")
				},
			},
			{
				name:    "create organization api key fails when limit reached",
				keyName: "limit",
				jsonBody: spec.CreateAPIKeyRequest{
					Name: "limit",
				},
				setupMocks: func() {
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "limit" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).Return(key.Key(""), nil, &store.ErrAPIKeyLimitReached{Limit: store.MaxAPIKeysPerTarget}).Once()
				},
				wantError:      true,
				expectedError:  store.ErrAPIKeyLimitReached{Limit: store.MaxAPIKeysPerTarget},
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:    "create organization api key fails with invalid name",
				keyName: "",
				jsonBody: spec.CreateAPIKeyRequest{
					Name: "",
				},
				setupMocks: func() {
					// No mock needed - validation happens before store is called
				},
				wantError:      true,
				expectedError:  fmt.Errorf("name is required"),
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:    "create organization api key fails with invalid expiry",
				keyName: "test-org-key",
				jsonBody: map[string]any{
					"name":   "test-org-key",
					"expiry": "invalid-date", // Invalid format to trigger error
				},
				setupMocks: func() {
					// No store call expected due to validation error
				},
				wantError:      true,
				expectedError:  fmt.Errorf("parsing time"),
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range createOrgKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/api-keys").WithJSONBody(tt.jsonBody).Context()
				err := handler.CreateOrganizationAPIKey(c, apitest.TestOrganization)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				var resp struct {
					Key spec.FullAPIKey `json:"key"`
				}
				rec.MustCode(tt.expectedStatus)
				rec.ReadBody(&resp)
				tt.validateResp(t, resp)
			})
		}

		// Test cases for listing organization API keys
		listOrgKeyTests := []struct {
			name           string
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
			validateResp   func(t *testing.T, resp struct {
				Keys []spec.APIKeyPreview `json:"keys"`
			})
		}{
			{
				name: "list organization api keys succeeds with keys",
				setupMocks: func() {
					keys := []store.APIKey{
						{
							ID:         "key1",
							Name:       "key-name-1",
							KeyPreview: "prev1",
							TargetType: store.KeyTargetOrganization,
							TargetID:   apitest.TestOrganization,
							Expiry:     nil,
							KeyHash:    "hash1",
							LastUsed:   &now,
							CreatedAt:  now,
							Branches:   []string{},
							Projects:   []string{},
							Scopes:     []string{},
						},
						{
							ID:         "key2",
							Name:       "key-name-2",
							KeyPreview: "prev2",
							TargetType: store.KeyTargetOrganization,
							TargetID:   apitest.TestOrganization,
							Expiry:     &fixedExpiry,
							KeyHash:    "hash2",
							LastUsed:   nil,
							CreatedAt:  now,
							Branches:   []string{},
							Projects:   []string{},
							Scopes:     []string{},
						},
					}
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
					).Return(keys, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					require.Len(t, resp.Keys, 2)
					assert.Equal(t, "key1", resp.Keys[0].Id)
					assert.Equal(t, "key-name-1", resp.Keys[0].Name)
					assert.Equal(t, "prev1", resp.Keys[0].Preview)
					assert.Nil(t, resp.Keys[0].Expiry)
					assert.NotNil(t, resp.Keys[0].LastUsed)
					assert.Equal(t, now.Format(time.RFC3339Nano), resp.Keys[0].LastUsed.Format(time.RFC3339Nano))
					assert.Equal(t, now.Format(time.RFC3339Nano), resp.Keys[0].CreatedAt.Format(time.RFC3339Nano))

					assert.Equal(t, "key2", resp.Keys[1].Id)
					assert.Equal(t, "key-name-2", resp.Keys[1].Name)
					assert.Equal(t, "prev2", resp.Keys[1].Preview)
					assert.NotNil(t, resp.Keys[1].Expiry)
					assert.Equal(t, fixedExpiry.Format(time.RFC3339Nano), resp.Keys[1].Expiry.Format(time.RFC3339Nano))
					assert.Nil(t, resp.Keys[1].LastUsed)
				},
			},
			{
				name: "list organization api keys with scopes and restrictions",
				setupMocks: func() {
					keys := []store.APIKey{
						{
							ID:         "org-key-with-scopes",
							Name:       "org-key-with-scopes",
							KeyPreview: "prev-org-scopes",
							TargetType: store.KeyTargetOrganization,
							TargetID:   apitest.TestOrganization,
							Expiry:     nil,
							KeyHash:    "hash-org-scopes",
							LastUsed:   &now,
							CreatedAt:  now,
							Scopes:     []string{"org:read", "project:write", "branch:read"},
							Projects:   []string{"proj1", "proj2"},
							Branches:   []string{"main", "dev"},
						},
						{
							ID:         "org-key-no-restrictions",
							Name:       "org-key-no-restrictions",
							KeyPreview: "prev-org-no-restrictions",
							TargetType: store.KeyTargetOrganization,
							TargetID:   apitest.TestOrganization,
							Expiry:     &fixedExpiry,
							KeyHash:    "hash-org-no-restrictions",
							LastUsed:   nil,
							CreatedAt:  now,
							Scopes:     []string{"org:admin"},
							Branches:   []string{},
							Projects:   []string{},
						},
					}
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
					).Return(keys, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					require.Len(t, resp.Keys, 2)

					// First key with scopes and restrictions
					assert.Equal(t, "org-key-with-scopes", resp.Keys[0].Id)
					assert.Equal(t, "org-key-with-scopes", resp.Keys[0].Name)
					assert.Equal(t, "prev-org-scopes", resp.Keys[0].Preview)
					assert.NotNil(t, resp.Keys[0].Scopes)
					assert.Contains(t, resp.Keys[0].Scopes, "org:read")
					assert.Contains(t, resp.Keys[0].Scopes, "project:write")
					assert.Contains(t, resp.Keys[0].Scopes, "branch:read")
					assert.NotNil(t, resp.Keys[0].Projects)
					assert.Contains(t, resp.Keys[0].Projects, "proj1")
					assert.Contains(t, resp.Keys[0].Projects, "proj2")
					assert.NotNil(t, resp.Keys[0].Branches)
					assert.Contains(t, resp.Keys[0].Branches, "main")
					assert.Contains(t, resp.Keys[0].Branches, "dev")

					// Second key with only scopes
					assert.Equal(t, "org-key-no-restrictions", resp.Keys[1].Id)
					assert.Equal(t, "org-key-no-restrictions", resp.Keys[1].Name)
					assert.NotNil(t, resp.Keys[1].Scopes)
					assert.Contains(t, resp.Keys[1].Scopes, "org:admin")
					// Check that restriction fields are empty arrays when not set
					assert.Empty(t, resp.Keys[1].Projects)
					assert.Empty(t, resp.Keys[1].Branches)
				},
			},
			{
				name: "list organization api keys succeeds with empty list",
				setupMocks: func() {
					// We explicitly use a separate mock expectation here
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
					).Return([]store.APIKey{}, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					assert.Empty(t, resp.Keys)
				},
			},
		}

		for _, tt := range listOrgKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/api-keys").Context()
				err := handler.ListOrganizationAPIKeys(c, apitest.TestOrganization)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				var resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				}
				rec.MustCode(tt.expectedStatus)
				rec.ReadBody(&resp)
				tt.validateResp(t, resp)
			})
		}

		// Test cases for deleting organization API keys
		deleteOrgKeyTests := []struct {
			name           string
			keyIDs         []string
			jsonBody       any
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
		}{
			{
				name:   "delete organization api keys succeeds with single key",
				keyIDs: []string{"key1"},
				jsonBody: spec.DeleteOrganizationAPIKeysJSONRequestBody{
					Ids: []string{"key1"},
				},
				setupMocks: func() {
					mockStore.EXPECT().DeleteAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
						[]string{"key1"},
					).Return(nil)
				},
				wantError:      false,
				expectedStatus: http.StatusNoContent,
			},
			{
				name:   "delete organization api keys succeeds with multiple keys",
				keyIDs: []string{"key1", "key2"},
				jsonBody: spec.DeleteOrganizationAPIKeysJSONRequestBody{
					Ids: []string{"key1", "key2"},
				},
				setupMocks: func() {
					mockStore.EXPECT().DeleteAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
						[]string{"key1", "key2"},
					).Return(nil)
				},
				wantError:      false,
				expectedStatus: http.StatusNoContent,
			},
			{
				name:   "delete organization api keys with empty ids list",
				keyIDs: []string{},
				jsonBody: spec.DeleteOrganizationAPIKeysJSONRequestBody{
					Ids: []string{},
				},
				setupMocks: func() {
					// We need to expect a call with empty IDs list, as the code doesn't special-case empty lists
					mockStore.EXPECT().DeleteAPIKeys(
						mock.Anything,
						store.KeyTargetOrganization,
						apitest.TestOrganization,
						[]string{},
					).Return(nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusNoContent,
			},
			{
				name:   "delete organization api keys fails with too many keys",
				keyIDs: make([]string, 51), // 51 keys to exceed the limit of 50
				jsonBody: spec.DeleteOrganizationAPIKeysJSONRequestBody{
					Ids: func() []string {
						ids := make([]string, 51)
						for i := range 51 {
							ids[i] = fmt.Sprintf("key%d", i+1)
						}
						return ids
					}(),
				},
				setupMocks: func() {
					// No store mock needed as validation should fail before reaching the store
				},
				wantError:      true,
				expectedError:  ErrorTooManyAPIKeys{},
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range deleteOrgKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.DELETE("/organizations/" + apitest.TestOrganization + "/api-keys").WithJSONBody(tt.jsonBody).Context()
				err := handler.DeleteOrganizationAPIKeys(c, apitest.TestOrganization)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				rec.MustCode(tt.expectedStatus)
			})
		}
	})

	t.Run("UserAPIKeys", func(t *testing.T) {
		// Create a fixed time for testing
		fixedTime := time.Date(2025, 6, 4, 12, 0, 0, 0, time.UTC)

		// Test cases for creating user API keys
		createUserKeyTests := []struct {
			name           string
			keyName        string
			jsonBody       any
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
			validateResp   func(t *testing.T, resp struct {
				Key spec.FullAPIKey `json:"key"`
			})
		}{
			{
				name:    "create user api key succeeds without expiry",
				keyName: "test-user-key",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:   "test-user-key",
					Expiry: nil,
				},
				setupMocks: func() {
					apiKeyResult := &store.APIKey{
						ID:         "user-generated-id",
						Name:       "test-user-key",
						KeyPreview: "xyz456",
						TargetType: store.KeyTargetUser,
						TargetID:   apitest.TestUserID,
						Expiry:     nil,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						Branches:   []string{},
						Projects:   []string{},
						Scopes:     []string{},
					}
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-user-key" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-user-key"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-user-key", resp.Key.Name)
					assert.Equal(t, "full-user-key", resp.Key.Token)
					assert.Equal(t, "xyz456", resp.Key.Preview)
					assert.Nil(t, resp.Key.Expiry)
				},
			},
			{
				name:    "create user api key succeeds with expiry",
				keyName: "test-user-key-with-expiry",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:   "test-user-key-with-expiry",
					Expiry: &fixedExpiry,
				},
				setupMocks: func() {
					expiryTime := fixedExpiry
					apiKeyResult := &store.APIKey{
						ID:         "user-generated-id-2",
						Name:       "test-user-key-with-expiry",
						KeyPreview: "uvw789",
						TargetType: store.KeyTargetUser,
						TargetID:   apitest.TestUserID,
						Expiry:     &expiryTime,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						Branches:   []string{},
						Projects:   []string{},
						Scopes:     []string{},
					}
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-user-key-with-expiry" &&
								apiKeyCreate.Expiry != nil &&
								apiKeyCreate.Expiry.Equal(expiryTime) &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-user-key-2"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-user-key-with-expiry", resp.Key.Name)
					assert.Equal(t, "full-user-key-2", resp.Key.Token)
					assert.Equal(t, "uvw789", resp.Key.Preview)
					assert.NotNil(t, resp.Key.Expiry)
					assert.Equal(t, fixedExpiry.Format(time.RFC3339), resp.Key.Expiry.Format(time.RFC3339))
				},
			},
			{
				name:    "create user api key with scopes",
				keyName: "test-user-key-scopes",
				jsonBody: spec.CreateAPIKeyRequest{
					Name:   "test-user-key-scopes",
					Scopes: &[]string{"org:read", "project:read"},
				},
				setupMocks: func() {
					apiKeyResult := &store.APIKey{
						ID:         "user-generated-id-scopes",
						Name:       "test-user-key-scopes",
						KeyPreview: "xyz456",
						TargetType: store.KeyTargetUser,
						TargetID:   apitest.TestUserID,
						Expiry:     nil,
						CreatedAt:  fixedTime,
						LastUsed:   nil,
						// Duplicate scopes are de-duplicated in the store
						Scopes:   []string{"org:read", "project:read", "org:read"},
						Branches: []string{},
						Projects: []string{},
					}
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "test-user-key-scopes" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 2 &&
								apiKeyCreate.Scopes[0] == "org:read" &&
								apiKeyCreate.Scopes[1] == "project:read" &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key("full-user-key-scopes"), apiKeyResult, nil)
				},
				wantError:      false,
				expectedStatus: http.StatusCreated,
				validateResp: func(t *testing.T, resp struct {
					Key spec.FullAPIKey `json:"key"`
				},
				) {
					assert.Equal(t, "test-user-key-scopes", resp.Key.Name)
					assert.Equal(t, "full-user-key-scopes", resp.Key.Token)
					assert.Equal(t, "xyz456", resp.Key.Preview)
					assert.Nil(t, resp.Key.Expiry)
					assert.NotNil(t, resp.Key.Scopes)
					assert.Contains(t, resp.Key.Scopes, "org:read")
					assert.Contains(t, resp.Key.Scopes, "project:read")
				},
			},
			{
				name:     "create user api key fails when limit reached",
				keyName:  "limit",
				jsonBody: spec.CreateAPIKeyRequest{Name: "limit"},
				setupMocks: func() {
					mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
						mock.MatchedBy(func(apiKeyCreate *store.APIKeyCreate) bool {
							return apiKeyCreate.Name == "limit" &&
								apiKeyCreate.Expiry == nil &&
								len(apiKeyCreate.Scopes) == 0 &&
								len(apiKeyCreate.Projects) == 0 &&
								len(apiKeyCreate.Branches) == 0 &&
								apiKeyCreate.CreatedBy != nil &&
								*apiKeyCreate.CreatedBy == apitest.TestUserID &&
								apiKeyCreate.CreatedByKey != nil &&
								*apiKeyCreate.CreatedByKey == ""
						})).
						Return(key.Key(""), nil, &store.ErrAPIKeyLimitReached{Limit: store.MaxAPIKeysPerTarget}).Once()
				},
				wantError:      true,
				expectedError:  store.ErrAPIKeyLimitReached{Limit: store.MaxAPIKeysPerTarget},
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range createUserKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.POST("/api-keys").WithJSONBody(tt.jsonBody).Context()
				err := handler.CreateUserAPIKey(c)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				var resp struct {
					Key spec.FullAPIKey `json:"key"`
				}
				rec.MustCode(tt.expectedStatus)
				rec.ReadBody(&resp)
				tt.validateResp(t, resp)
			})
		}

		// Test cases for listing user API keys
		listUserKeyTests := []struct {
			name           string
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
			validateResp   func(t *testing.T, resp struct {
				Keys []spec.APIKeyPreview `json:"keys"`
			})
		}{
			{
				name: "list user api keys succeeds with keys",
				setupMocks: func() {
					keys := []store.APIKey{
						{
							ID:         "user-key1",
							Name:       "key-name-user-1",
							KeyPreview: "prev-user-1",
							TargetType: store.KeyTargetUser,
							TargetID:   apitest.TestUserID,
							Expiry:     nil,
							KeyHash:    "hash1",
							LastUsed:   &now,
							CreatedAt:  now,
							Branches:   []string{},
							Projects:   []string{},
							Scopes:     []string{},
						},
						{
							ID:         "user-key2",
							Name:       "key-name-user-2",
							KeyPreview: "prev-user-2",
							TargetType: store.KeyTargetUser,
							TargetID:   apitest.TestUserID,
							Expiry:     &fixedExpiry,
							KeyHash:    "hash2",
							LastUsed:   nil,
							CreatedAt:  now,
							Branches:   []string{},
							Projects:   []string{},
							Scopes:     []string{},
						},
					}
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetUser,
						apitest.TestUserID,
					).Return(keys, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					require.Len(t, resp.Keys, 2)
					assert.Equal(t, "user-key1", resp.Keys[0].Id)
					assert.Equal(t, "key-name-user-1", resp.Keys[0].Name)
					assert.Equal(t, "prev-user-1", resp.Keys[0].Preview)
					assert.Nil(t, resp.Keys[0].Expiry)
					assert.NotNil(t, resp.Keys[0].LastUsed)

					assert.Equal(t, "user-key2", resp.Keys[1].Id)
					assert.Equal(t, "key-name-user-2", resp.Keys[1].Name)
					assert.Equal(t, "prev-user-2", resp.Keys[1].Preview)
					assert.NotNil(t, resp.Keys[1].Expiry)
					assert.Nil(t, resp.Keys[1].LastUsed)
				},
			},
			{
				name: "list user api keys with scopes and restrictions",
				setupMocks: func() {
					keys := []store.APIKey{
						{
							ID:         "user-key-with-scopes",
							Name:       "key-with-scopes",
							KeyPreview: "prev-scopes",
							TargetType: store.KeyTargetUser,
							TargetID:   apitest.TestUserID,
							Expiry:     nil,
							KeyHash:    "hash-scopes",
							LastUsed:   &now,
							CreatedAt:  now,
							Scopes:     []string{"org:read", "project:read"},
							Projects:   []string{"proj1"},
							Branches:   []string{"main"},
						},
						{
							ID:         "user-key-no-restrictions",
							Name:       "key-no-restrictions",
							KeyPreview: "prev-no-restrictions",
							TargetType: store.KeyTargetUser,
							TargetID:   apitest.TestUserID,
							Expiry:     nil,
							KeyHash:    "hash-no-restrictions",
							LastUsed:   nil,
							CreatedAt:  now,
							Scopes:     []string{"org:write"},
							Branches:   []string{},
							Projects:   []string{},
						},
					}
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetUser,
						apitest.TestUserID,
					).Return(keys, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					require.Len(t, resp.Keys, 2)

					// First key with scopes and restrictions
					assert.Equal(t, "user-key-with-scopes", resp.Keys[0].Id)
					assert.Equal(t, "key-with-scopes", resp.Keys[0].Name)
					assert.Equal(t, "prev-scopes", resp.Keys[0].Preview)
					assert.NotNil(t, resp.Keys[0].Scopes)
					assert.Contains(t, resp.Keys[0].Scopes, "org:read")
					assert.Contains(t, resp.Keys[0].Scopes, "project:read")
					assert.NotNil(t, resp.Keys[0].Projects)
					assert.Contains(t, resp.Keys[0].Projects, "proj1")
					assert.NotNil(t, resp.Keys[0].Branches)
					assert.Contains(t, resp.Keys[0].Branches, "main")

					// Second key with only scopes
					assert.Equal(t, "user-key-no-restrictions", resp.Keys[1].Id)
					assert.Equal(t, "key-no-restrictions", resp.Keys[1].Name)
					assert.NotNil(t, resp.Keys[1].Scopes)
					assert.Contains(t, resp.Keys[1].Scopes, "org:write")
					// Check that restriction fields are empty arrays when not set
					assert.Empty(t, resp.Keys[1].Projects)
					assert.Empty(t, resp.Keys[1].Branches)
				},
			},
			{
				name: "list user api keys succeeds with empty list",
				setupMocks: func() {
					mockStore.EXPECT().ListAPIKeys(
						mock.Anything,
						store.KeyTargetUser,
						apitest.TestUserID,
					).Return([]store.APIKey{}, nil).Once()
				},
				wantError:      false,
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				},
				) {
					assert.Empty(t, resp.Keys)
				},
			},
		}

		for _, tt := range listUserKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.GET("/api-keys").Context()
				err := handler.ListUserAPIKeys(c)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				var resp struct {
					Keys []spec.APIKeyPreview `json:"keys"`
				}
				rec.MustCode(tt.expectedStatus)
				rec.ReadBody(&resp)
				tt.validateResp(t, resp)
			})
		}

		// Test cases for deleting user API keys
		deleteUserKeyTests := []struct {
			name           string
			keyIDs         []string
			jsonBody       any
			setupMocks     func()
			wantError      bool
			expectedError  error
			expectedStatus int
		}{
			{
				name:   "delete user api keys succeeds with single key",
				keyIDs: []string{"user-key1"},
				jsonBody: struct {
					Ids []string `json:"ids"`
				}{
					Ids: []string{"user-key1"},
				},
				setupMocks: func() {
					mockStore.EXPECT().DeleteAPIKeys(
						mock.Anything,
						store.KeyTargetUser,
						apitest.TestUserID,
						[]string{"user-key1"},
					).Return(nil)
				},
				wantError:      false,
				expectedStatus: http.StatusNoContent,
			},
			{
				name:   "delete user api keys fails with too many keys",
				keyIDs: make([]string, 51), // 51 keys to exceed the limit of 50
				jsonBody: struct {
					Ids []string `json:"ids"`
				}{
					Ids: func() []string {
						ids := make([]string, 51)
						for i := range 51 {
							ids[i] = fmt.Sprintf("user-key%d", i+1)
						}
						return ids
					}(),
				},
				setupMocks: func() {
					// No store mock needed as validation should fail before reaching the store
				},
				wantError:      true,
				expectedError:  ErrorTooManyAPIKeys{Limit: 50, Count: 51},
				expectedStatus: http.StatusBadRequest,
			},
		}

		for _, tt := range deleteUserKeyTests {
			t.Run(tt.name, func(t *testing.T) {
				tt.setupMocks()

				c, rec := e.DELETE("/api-keys").WithJSONBody(tt.jsonBody).Context()
				err := handler.DeleteUserAPIKeys(c)

				if tt.wantError {
					assert.Error(t, err)
					if tt.expectedError != nil && err != nil {
						assert.Contains(t, err.Error(), tt.expectedError.Error())
					}
					return
				}

				assert.NoError(t, err)
				rec.MustCode(tt.expectedStatus)
			})
		}
	})

	// Final mock assertions
	mockKC.AssertExpectations(t)
	mockStore.AssertExpectations(t)
}

func TestValidateResourceList(t *testing.T) {
	tests := []struct {
		name           string
		requested      []string
		userAccess     []string
		resourceType   string
		wantErr        bool
		expectedErrMsg string
	}{
		{
			name:         "no resources requested, no user access",
			requested:    []string{},
			userAccess:   []string{},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:           "resources requested, no user access (but not empty requested list)",
			requested:      []string{"resA"},
			userAccess:     []string{},
			resourceType:   "project",
			wantErr:        true,
			expectedErrMsg: "insufficient access to project: resA",
		},
		{
			name:         "no resources requested, user has specific access",
			requested:    []string{},
			userAccess:   []string{"resA"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:         "no resources requested, user has wildcard access",
			requested:    []string{},
			userAccess:   []string{"*"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:         "user has required specific resource",
			requested:    []string{"resA"},
			userAccess:   []string{"resA", "resB"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:         "user has required specific resources (exact match)",
			requested:    []string{"resA", "resB"},
			userAccess:   []string{"resA", "resB"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:         "user has wildcard access, requests specific resources",
			requested:    []string{"resA", "resB"},
			userAccess:   []string{"*"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:         "user has wildcard access, requests wildcard (should be cleaned by cleanPermissionArray first)",
			requested:    []string{"*"}, // cleanPermissionArray would make this ["*"]
			userAccess:   []string{"*"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:           "user has specific access, requests superset",
			requested:      []string{"resA", "resB"},
			userAccess:     []string{"resA"},
			resourceType:   "project",
			wantErr:        true,
			expectedErrMsg: "insufficient access to project: resB",
		},
		{
			name:           "user has specific access, requests unrelated resource",
			requested:      []string{"resB"},
			userAccess:     []string{"resA"},
			resourceType:   "branch",
			wantErr:        true,
			expectedErrMsg: "insufficient access to branch: resB",
		},
		{
			name:           "user has specific access, requests wildcard (should fail as user doesn't have wildcard)",
			requested:      []string{"*"}, // cleanPermissionArray would make this ["*"]
			userAccess:     []string{"resA"},
			resourceType:   "project",
			wantErr:        true,
			expectedErrMsg: "insufficient access to project: *",
		},
		{
			name:         "empty requested list, user has specific access",
			requested:    []string{},
			userAccess:   []string{"resA"},
			resourceType: "project",
			wantErr:      false,
		},
		{
			name:           "empty user access list, non-empty requested list",
			requested:      []string{"resA"},
			userAccess:     []string{},
			resourceType:   "project",
			wantErr:        true,
			expectedErrMsg: "insufficient access to project: resA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResourceList(tt.requested, tt.userAccess, tt.resourceType)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestScopeAllowed mirrors the scope cases in
// internal/opa/policy/policy_test.rego so the createAPIKey subset check and
// the runtime OPA evaluator stay in sync.
func TestScopeAllowed(t *testing.T) {
	tests := map[string]struct {
		scope   string
		allowed []string
		want    bool
	}{
		"exact match":                  {"branch:read", []string{"branch:read"}, true},
		"wildcard satisfies any scope": {"branch:write", []string{"*"}, true},
		"write satisfies read":         {"branch:read", []string{"branch:write"}, true},
		"read does not satisfy write":  {"branch:write", []string{"branch:read"}, false},
		"unrelated scope is denied":    {"branch:read", []string{"keys:write"}, false},
		"empty allowed denies":         {"branch:read", []string{}, false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, scopeAllowed(tt.scope, tt.allowed))
		})
	}
}

func TestCreateAPIKeyPreventsPrivilegeEscalation(t *testing.T) {
	const defaultOrgID = "default-org"
	const defaultOrgName = "Default Organization"

	parentExpiry := time.Now().Add(24 * time.Hour)
	earlierExpiry := parentExpiry.Add(-1 * time.Hour)
	laterExpiry := parentExpiry.Add(1 * time.Hour)

	defaultParent := &store.APIKey{
		ID:         "parent-key-id",
		Name:       "parent",
		TargetType: store.KeyTargetUser,
		TargetID:   apitest.TestUserID,
		Scopes:     []string{"keys:write", "branch:read"},
		Projects:   []string{"proj1"},
		Branches:   []string{"main"},
	}
	wildcardParent := &store.APIKey{
		ID:         "parent-key-id",
		TargetType: store.KeyTargetUser,
		TargetID:   apitest.TestUserID,
		Scopes:     []string{"*"},
		Projects:   []string{"*"},
		Branches:   []string{"*"},
	}
	multiResourceParent := &store.APIKey{
		ID:         "parent-key-id",
		TargetType: store.KeyTargetUser,
		TargetID:   apitest.TestUserID,
		Scopes:     []string{"keys:write", "branch:read", "project:read"},
		Projects:   []string{"proj1", "proj2"},
		Branches:   []string{"main", "dev"},
	}
	emptyScopesParent := &store.APIKey{
		ID:         "parent-key-id",
		TargetType: store.KeyTargetUser,
		TargetID:   apitest.TestUserID,
		Scopes:     []string{},
		Projects:   []string{"*"},
		Branches:   []string{"*"},
	}

	withExpiry := func(p *store.APIKey, e time.Time) *store.APIKey {
		clone := *p
		clone.Expiry = &e
		return &clone
	}

	parentClaims := func(p *store.APIKey) token.Claims {
		return token.Claims{
			ID:            apitest.TestUserID,
			Email:         apitest.TestUserEmail,
			Organizations: map[string]token.Organization{apitest.TestOrganization: {ID: apitest.TestOrganization, Status: "enabled"}},
			KeyID:         p.ID,
			Scopes:        p.Scopes,
			Projects:      p.Projects,
			Branches:      p.Branches,
		}
	}

	tests := map[string]struct {
		parent         *store.APIKey
		body           any
		setupMocks     func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient)
		wantErr        bool
		expectedErrMsg string
		wantStatus     int
	}{
		"child inherits parent restrictions when scopes/projects/branches omitted": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "child-inherits"},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return c.Name == "child-inherits" &&
							slices.Equal(c.Scopes, []string{"keys:write", "branch:read"}) &&
							slices.Equal(c.Projects, []string{"proj1"}) &&
							slices.Equal(c.Branches, []string{"main"}) &&
							c.CreatedByKey != nil && *c.CreatedByKey == "parent-key-id"
					})).Return(key.Key("child-token"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"keys:write", "branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
				}, nil)
			},
		},
		"child cannot request wildcard scopes when parent is restricted": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "escalate-scopes", Scopes: &[]string{"*"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to scope: *",
			wantStatus:     http.StatusBadRequest,
		},
		"child cannot request a scope not held by the parent": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "escalate-org-write", Scopes: &[]string{"org:write"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to scope: org:write",
			wantStatus:     http.StatusBadRequest,
		},
		"child cannot request a project not held by the parent": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "escalate-project", Projects: &[]string{"other-proj"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to project: other-proj",
			wantStatus:     http.StatusBadRequest,
		},
		"child cannot request a branch not held by the parent": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "escalate-branch", Branches: &[]string{"other-branch"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to branch: other-branch",
			wantStatus:     http.StatusBadRequest,
		},
		"child can downgrade parent write scope to read (mirrors OPA write-implies-read)": {
			parent: &store.APIKey{
				ID: "write-parent", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
				Scopes: []string{"branch:write"}, Projects: []string{"proj1"}, Branches: []string{"main"},
			},
			body: spec.CreateAPIKeyRequest{Name: "downgrade-child", Scopes: &[]string{"branch:read"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, []string{"branch:read"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "downgrade-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
				}, nil)
			},
		},
		"child cannot upgrade parent read scope to write": {
			parent: &store.APIKey{
				ID: "read-parent", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
				Scopes: []string{"branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
			},
			body: spec.CreateAPIKeyRequest{Name: "upgrade-child", Scopes: &[]string{"branch:write"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to scope: branch:write",
			wantStatus:     http.StatusBadRequest,
		},
		"child with subset of parent scopes succeeds": {
			parent: defaultParent,
			body:   spec.CreateAPIKeyRequest{Name: "child-subset", Scopes: &[]string{"branch:read"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return c.Name == "child-subset" &&
							slices.Equal(c.Scopes, []string{"branch:read"}) &&
							slices.Equal(c.Projects, []string{"proj1"}) &&
							slices.Equal(c.Branches, []string{"main"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
				}, nil)
			},
		},
		"child cannot outlive parent (later expiry)": {
			parent:         withExpiry(defaultParent, parentExpiry),
			body:           spec.CreateAPIKeyRequest{Name: "child-later", Expiry: &laterExpiry},
			wantErr:        true,
			expectedErrMsg: "expiry must not exceed parent API key expiry",
			wantStatus:     http.StatusBadRequest,
		},
		"child cannot omit expiry when parent has expiry": {
			parent:         withExpiry(defaultParent, parentExpiry),
			body:           spec.CreateAPIKeyRequest{Name: "child-no-expiry"},
			wantErr:        true,
			expectedErrMsg: "expiry must not exceed parent API key expiry",
			wantStatus:     http.StatusBadRequest,
		},
		"child with earlier expiry than parent succeeds": {
			parent: withExpiry(defaultParent, parentExpiry),
			body:   spec.CreateAPIKeyRequest{Name: "child-earlier", Expiry: &earlierExpiry},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return c.Name == "child-earlier" && c.Expiry != nil && c.Expiry.Equal(earlierExpiry)
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"keys:write", "branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
					Expiry: &earlierExpiry,
				}, nil)
			},
		},
		"child with equal expiry to parent succeeds": {
			parent: withExpiry(defaultParent, parentExpiry),
			body:   spec.CreateAPIKeyRequest{Name: "child-equal", Expiry: &parentExpiry},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return c.Name == "child-equal" && c.Expiry != nil && c.Expiry.Equal(parentExpiry)
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"keys:write", "branch:read"}, Projects: []string{"proj1"}, Branches: []string{"main"},
					Expiry: &parentExpiry,
				}, nil)
			},
		},
		"wildcard parent allows specific scope subset": {
			parent: wildcardParent,
			body:   spec.CreateAPIKeyRequest{Name: "from-wildcard", Scopes: &[]string{"keys:write"}, Projects: &[]string{"proj1"}, Branches: &[]string{"main"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, []string{"keys:write"}) &&
							slices.Equal(c.Projects, []string{"proj1"}) &&
							slices.Equal(c.Branches, []string{"main"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"keys:write"}, Projects: []string{"proj1"}, Branches: []string{"main"},
				}, nil)
			},
		},
		"wildcard parent allows wildcard child": {
			parent: wildcardParent,
			body:   spec.CreateAPIKeyRequest{Name: "all-wildcard", Scopes: &[]string{"*"}, Projects: &[]string{"*"}, Branches: &[]string{"*"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, []string{"*"}) &&
							slices.Equal(c.Projects, []string{"*"}) &&
							slices.Equal(c.Branches, []string{"*"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"*"}, Projects: []string{"*"}, Branches: []string{"*"},
				}, nil)
			},
		},
		"partial overlap with parent scope set is rejected": {
			parent: multiResourceParent,
			body:   spec.CreateAPIKeyRequest{Name: "partial", Scopes: &[]string{"keys:write", "branch:read", "org:write"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to scope: org:write",
			wantStatus:     http.StatusBadRequest,
		},
		"multi-element subset of parent projects succeeds": {
			parent: multiResourceParent,
			body:   spec.CreateAPIKeyRequest{Name: "multi-proj", Projects: &[]string{"proj1", "proj2"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Projects, []string{"proj1", "proj2"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: multiResourceParent.Scopes, Projects: []string{"proj1", "proj2"}, Branches: multiResourceParent.Branches,
				}, nil)
			},
		},
		"multi-element subset of parent branches succeeds": {
			parent: multiResourceParent,
			body:   spec.CreateAPIKeyRequest{Name: "multi-branch", Branches: &[]string{"dev"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Branches, []string{"dev"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: multiResourceParent.Scopes, Projects: multiResourceParent.Projects, Branches: []string{"dev"},
				}, nil)
			},
		},
		"parent with empty scopes is rejected (no privilege leak through store default)": {
			parent:         emptyScopesParent,
			body:           spec.CreateAPIKeyRequest{Name: "from-empty"},
			wantErr:        true,
			expectedErrMsg: "parent API key has invalid permissions",
			wantStatus:     http.StatusBadRequest,
		},
		"empty strings in requested scopes are stripped": {
			parent: wildcardParent,
			body:   spec.CreateAPIKeyRequest{Name: "with-empty", Scopes: &[]string{"", "keys:write", ""}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetUser, apitest.TestUserID,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, []string{"keys:write"})
					})).Return(key.Key("t"), &store.APIKey{
					ID: "child-id", TargetType: store.KeyTargetUser, TargetID: apitest.TestUserID,
					Scopes: []string{"keys:write"}, Projects: []string{"*"}, Branches: []string{"*"},
				}, nil)
			},
		},
		"mixed wildcard and specific scopes is rejected": {
			parent:         wildcardParent,
			body:           spec.CreateAPIKeyRequest{Name: "mixed", Scopes: &[]string{"*", "keys:read"}},
			wantErr:        true,
			expectedErrMsg: "scopes cannot mix '*' with specific values",
			wantStatus:     http.StatusBadRequest,
		},
		"mixed wildcard and specific projects is rejected": {
			parent:         wildcardParent,
			body:           spec.CreateAPIKeyRequest{Name: "mixed-proj", Projects: &[]string{"*", "proj1"}},
			wantErr:        true,
			expectedErrMsg: "projects cannot mix '*' with specific values",
			wantStatus:     http.StatusBadRequest,
		},
		"name longer than the max length is rejected": {
			parent:         wildcardParent,
			body:           spec.CreateAPIKeyRequest{Name: strings.Repeat("a", MaxAPIKeyNameLength+1)},
			wantErr:        true,
			expectedErrMsg: fmt.Sprintf("must be at most %d characters", MaxAPIKeyNameLength),
			wantStatus:     http.StatusBadRequest,
		},
		"expiry in the past is rejected before any other check": {
			parent:         wildcardParent,
			body:           spec.CreateAPIKeyRequest{Name: "past", Expiry: new(time.Time)},
			wantErr:        true,
			expectedErrMsg: "expiry must be in the future",
			wantStatus:     http.StatusBadRequest,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockKC := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjects := protomocks.NewProjectsServiceClient(t)
			handler := NewPublicAPIHandler(feat, mockKC, apitest.TestRealm, mockStore, mockProjects, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(parentClaims(tt.parent))

			mockStore.EXPECT().GetAPIKey(mock.Anything, tt.parent.ID).Return(tt.parent, nil).Maybe()
			if tt.setupMocks != nil {
				tt.setupMocks(t, mockStore, mockProjects)
			}

			c, _ := e.POST("/api-keys").WithJSONBody(tt.body).Context()
			err := handler.CreateUserAPIKey(c)

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
				if tt.wantStatus != 0 {
					var coded interface{ StatusCode() int }
					require.ErrorAs(t, err, &coded)
					assert.Equal(t, tt.wantStatus, coded.StatusCode())
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCreateOrganizationAPIKeyPreventsPrivilegeEscalation(t *testing.T) {
	const defaultOrgID = "default-org"
	const defaultOrgName = "Default Organization"

	parent := &store.APIKey{
		ID:         "org-parent-id",
		TargetType: store.KeyTargetOrganization,
		TargetID:   apitest.TestOrganization,
		Scopes:     []string{"keys:write", "branch:read"},
		Projects:   []string{"proj1"},
		Branches:   []string{"main"},
	}
	parentClaims := token.Claims{
		Organizations: map[string]token.Organization{apitest.TestOrganization: {ID: apitest.TestOrganization, Status: "enabled"}},
		KeyID:         parent.ID,
		Scopes:        parent.Scopes,
		Projects:      parent.Projects,
		Branches:      parent.Branches,
	}

	tests := map[string]struct {
		body           any
		setupMocks     func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient)
		wantErr        bool
		expectedErrMsg string
		wantStatus     int
	}{
		"org child with subset of parent scopes succeeds": {
			body: spec.CreateAPIKeyRequest{Name: "org-subset", Scopes: &[]string{"branch:read"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, []string{"branch:read"}) &&
							c.CreatedByKey != nil && *c.CreatedByKey == "org-parent-id"
					})).Return(key.Key("t"), &store.APIKey{
					ID: "org-child", TargetType: store.KeyTargetOrganization, TargetID: apitest.TestOrganization,
					Scopes: []string{"branch:read"}, Projects: parent.Projects, Branches: parent.Branches,
				}, nil)
			},
		},
		"org child cannot request scope not held by parent": {
			body: spec.CreateAPIKeyRequest{Name: "org-escalate", Scopes: &[]string{"org:write"}},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
			},
			wantErr:        true,
			expectedErrMsg: "insufficient access to scope: org:write",
			wantStatus:     http.StatusBadRequest,
		},
		"org child inherits parent restrictions when omitted": {
			body: spec.CreateAPIKeyRequest{Name: "org-inherit"},
			setupMocks: func(t *testing.T, mockStore *storeMocks.AuthStore, mockProjects *protomocks.ProjectsServiceClient) {
				mockProjects.EXPECT().ValidateHierarchy(mock.Anything, mock.Anything).Return(nil, nil)
				mockStore.EXPECT().CreateAPIKey(mock.Anything, store.KeyTargetOrganization, apitest.TestOrganization,
					mock.MatchedBy(func(c *store.APIKeyCreate) bool {
						return slices.Equal(c.Scopes, parent.Scopes) &&
							slices.Equal(c.Projects, parent.Projects) &&
							slices.Equal(c.Branches, parent.Branches)
					})).Return(key.Key("t"), &store.APIKey{
					ID: "org-child", TargetType: store.KeyTargetOrganization, TargetID: apitest.TestOrganization,
					Scopes: parent.Scopes, Projects: parent.Projects, Branches: parent.Branches,
				}, nil)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockKC := keycloakMocks.NewKeyCloak(t)
			mockStore := storeMocks.NewAuthStore(t)
			feat := openfeaturetest.NewClient(nil)
			mockProjects := protomocks.NewProjectsServiceClient(t)
			handler := NewPublicAPIHandler(feat, mockKC, apitest.TestRealm, mockStore, mockProjects, &billing.NoopBilling{}, analyticsmocks.NewClient(t), defaultOrgID, defaultOrgName)
			e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(parentClaims)

			mockStore.EXPECT().GetAPIKey(mock.Anything, parent.ID).Return(parent, nil)
			if tt.setupMocks != nil {
				tt.setupMocks(t, mockStore, mockProjects)
			}

			c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/api-keys").WithJSONBody(tt.body).Context()
			err := handler.CreateOrganizationAPIKey(c, apitest.TestOrganization)

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
				if tt.wantStatus != 0 {
					var coded interface{ StatusCode() int }
					require.ErrorAs(t, err, &coded)
					assert.Equal(t, tt.wantStatus, coded.StatusCode())
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestOrganizationMemberships(t *testing.T) {
	mockKC := keycloakMocks.NewKeyCloak(t)
	mockStore := storeMocks.NewAuthStore(t)
	feat := openfeaturetest.NewClient(nil)
	mockProjectsClient := protomocks.NewProjectsServiceClient(t)
	handler := NewPublicAPIHandler(feat, mockKC, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), "default-org", "Default Organization")

	t.Run("ListMembers returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.GET("/organizations/" + apitest.TestOrganization + "/members").Context()
		err := handler.ListOrganizationMembers(c, apitest.TestOrganization)
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})

	t.Run("RemoveMember returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.DELETE("/organizations/" + apitest.TestOrganization + "/members/u1").Context()
		err := handler.RemoveOrganizationMember(c, apitest.TestOrganization, "u1")
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})
}

func TestOrganizationInvitations(t *testing.T) {
	mockKC := keycloakMocks.NewKeyCloak(t)
	mockStore := storeMocks.NewAuthStore(t)
	feat := openfeaturetest.NewClient(nil)
	mockProjectsClient := protomocks.NewProjectsServiceClient(t)
	handler := NewPublicAPIHandler(feat, mockKC, apitest.TestRealm, mockStore, mockProjectsClient, &billing.NoopBilling{}, analyticsmocks.NewClient(t), "default-org", "Default Organization")

	t.Run("CreateInvitation returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/invitations").WithJSONBody(map[string]string{"email": "test@example.com"}).Context()
		err := handler.CreateOrganizationInvitation(c, apitest.TestOrganization)
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})

	t.Run("ListInvitations returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.GET("/organizations/" + apitest.TestOrganization + "/invitations").Context()
		err := handler.ListOrganizationInvitations(c, apitest.TestOrganization, spec.ListOrganizationInvitationsParams{})
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})

	t.Run("GetInvitation returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.GET("/organizations/" + apitest.TestOrganization + "/invitations/inv-1").Context()
		err := handler.GetOrganizationInvitation(c, apitest.TestOrganization, "inv-1")
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})

	t.Run("DeleteInvitation returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.DELETE("/organizations/" + apitest.TestOrganization + "/invitations/inv-1").Context()
		err := handler.DeleteOrganizationInvitation(c, apitest.TestOrganization, "inv-1")
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})

	t.Run("ResendInvitation returns not implemented", func(t *testing.T) {
		e := apitest.New(t).WithOpenAPISpec(authSpec).WithClaims(apitest.TestClaims)
		c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/invitations/inv-1/resend").Context()
		err := handler.ResendOrganizationInvitation(c, apitest.TestOrganization, "inv-1")
		require.ErrorIs(t, err, echo.ErrNotImplemented)
	})
}

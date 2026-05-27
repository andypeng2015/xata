package apitest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"

	"xata/internal/api"
	"xata/internal/token"
)

const (
	TestRealm                = "test-realm"
	TestUserID               = "test-user-id"
	TestUserEmail            = "dev@xata.io"
	TestOrganization         = "123xyz"
	TestOrganizationDisabled = "123xyz-disabled"
)

var TestClaims = token.Claims{
	ID:            TestUserID,
	Email:         TestUserEmail,
	Organizations: map[string]token.Organization{TestOrganization: {ID: TestOrganization, Status: "enabled", UsageTier: "t2"}},
	Scopes:        []string{"*"},
	Projects:      []string{"*"},
	Branches:      []string{"*"},
}

var TestClaimsDisabled = token.Claims{
	ID:            TestUserID,
	Email:         TestUserEmail,
	Organizations: map[string]token.Organization{TestOrganizationDisabled: {ID: TestOrganizationDisabled, Status: "disabled"}},
	Scopes:        []string{"*"},
	Projects:      []string{"*"},
	Branches:      []string{"*"},
}

type apitest struct {
	e        *echo.Echo
	t        testing.TB
	claims   *token.Claims
	spec     *openapi3.T
	skipSpec bool
}

type testrequest struct {
	t            testing.TB
	e            *echo.Echo
	claims       *token.Claims
	req          *http.Request
	rec          *ResponseRecorder
	spec         *openapi3.T
	skipSpec     bool
	reqBodyBytes []byte // Cache of request body bytes for validation
}

type ResponseRecorder struct {
	*httptest.ResponseRecorder
	t            testing.TB
	req          *http.Request
	spec         *openapi3.T
	skipSpec     bool
	validated    bool
	reqBodyBytes []byte // Cache of request body bytes for validation
}

type mockRequestValidator struct{}

func (m *mockRequestValidator) Validate(i any) error {
	return nil
}

// New creates a new apitest instance.
// The apitest instance is used to create test requests.
func New(t testing.TB) *apitest {
	ech := echo.New()
	ech.Validator = &mockRequestValidator{}
	return &apitest{
		t: t,
		e: ech,
	}
}

// WithClaims sets the claims to be used in the test requests.
func (a *apitest) WithClaims(claims token.Claims) *apitest {
	a.claims = &claims
	return a
}

// WithOpenAPISpec sets the OpenAPI spec to use for automatic validation.
// When set, all requests and responses will be validated against this spec.
func (a *apitest) WithOpenAPISpec(spec *openapi3.T) *apitest {
	a.spec = spec
	return a
}

// SkipOpenAPIValidation disables automatic OpenAPI validation for this test.
// This is useful for tests that intentionally send invalid requests or expect invalid responses.
func (a *apitest) SkipOpenAPIValidation() *apitest {
	a.skipSpec = true
	return a
}

func (a *apitest) newRequest(method, path string) *testrequest {
	req := httptest.NewRequest(method, path, nil)

	return &testrequest{
		t:        a.t,
		e:        a.e,
		claims:   a.claims,
		req:      req,
		spec:     a.spec,
		skipSpec: a.skipSpec,
		rec: &ResponseRecorder{
			ResponseRecorder: httptest.NewRecorder(),
			t:                a.t,
			req:              nil,
			spec:             a.spec,
			skipSpec:         a.skipSpec,
		},
	}
}

// POST creates a new test request with the POST method.
func (a *apitest) POST(path string) *testrequest {
	return a.newRequest(http.MethodPost, path)
}

// GET creates a new test request with the GET method.
func (a *apitest) GET(path string) *testrequest {
	return a.newRequest(http.MethodGet, path)
}

// PUT creates a new test request with the PUT method.
func (a *apitest) PUT(path string) *testrequest {
	return a.newRequest(http.MethodPut, path)
}

// DELETE creates a new test request with the DELETE method.
func (a *apitest) DELETE(path string) *testrequest {
	return a.newRequest(http.MethodDelete, path)
}

// PATCH creates a new test request with the PATCH method.
func (a *apitest) PATCH(path string) *testrequest {
	return a.newRequest(http.MethodPatch, path)
}

// WithJSONBody sets the request body to the given value.
func (t *testrequest) WithJSONBody(in any) *testrequest {
	body, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}

	// Cache the body bytes for later validation
	t.reqBodyBytes = body
	t.req.Body = io.NopCloser(bytes.NewReader(body))
	t.req.ContentLength = int64(len(body))
	t.req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	return t
}

// WithHeader sets a request header.
func (t *testrequest) WithHeader(key, value string) *testrequest {
	t.req.Header.Set(key, value)
	return t
}

// SkipOpenAPIValidation disables automatic OpenAPI validation for this specific request.
// This is useful when testing error cases that intentionally violate the schema.
func (t *testrequest) SkipOpenAPIValidation() *testrequest {
	t.skipSpec = true
	t.rec.skipSpec = true
	return t
}

// Context returns the echo context and the response recorder to
// be used in the test.
func (t *testrequest) Context() (echo.Context, *ResponseRecorder) {
	c := t.e.NewContext(t.req, t.rec)

	// configure request context with claims
	if t.claims != nil {
		ctx := context.WithValue(c.Request().Context(), api.UserClaimsKey{}, t.claims)
		c.SetRequest(c.Request().WithContext(ctx))
	}

	// Store request and body bytes in recorder for validation
	t.rec.req = c.Request()
	t.rec.reqBodyBytes = t.reqBodyBytes

	return c, t.rec
}

// ReadBody reads the response body and unmarshals it into the given value.
func (r *ResponseRecorder) ReadBody(out any) {
	r.t.Helper()
	if err := json.Unmarshal(r.Body.Bytes(), out); err != nil {
		r.t.Fatalf("failed to unmarshal response: %v", err)
	}
}

// MustCode checks if the response status code is the expected one.
// If an OpenAPI spec was configured, it also automatically validates the request and response.
func (r *ResponseRecorder) MustCode(want int) {
	r.t.Helper()
	if got := r.Code; got != want {
		r.t.Fatalf("unexpected status code: want %d, got %d", want, got)
	}

	// Automatically validate against OpenAPI spec if configured
	r.autoValidate()
}

// autoValidate performs automatic OpenAPI validation if a spec is configured and validation hasn't been skipped.
func (r *ResponseRecorder) autoValidate() {
	r.t.Helper()

	// Skip if already validated or validation is disabled
	if r.validated || r.skipSpec || r.spec == nil {
		return
	}

	// Mark as validated to prevent duplicate validation
	r.validated = true

	// Perform validation
	if err := r.ValidateAgainstOpenAPI(r.spec); err != nil {
		r.t.Fatalf("automatic OpenAPI validation failed: %v", err)
	}
}

// Request returns the HTTP request used in the test.
// This is needed by validation methods to access the original request.
func (r *ResponseRecorder) Request() *http.Request {
	return r.req
}

// WithRequest associates an HTTP request with this response recorder.
// This is used internally by the test framework.
func (r *ResponseRecorder) WithRequest(req *http.Request) *ResponseRecorder {
	r.req = req
	return r
}

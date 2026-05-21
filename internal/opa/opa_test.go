package opa

import (
	"context"
	"sync"
	"testing"

	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/stretchr/testify/require"
)

// Compiling the Rego module dominates test runtime, so reuse one prepared
// query across every case — matches how AuthService holds it in prod.
var preparedAllow = sync.OnceValues(func() (rego.PreparedEvalQuery, error) {
	return rego.New(
		rego.Query("data.policy.allow"),
		rego.Module("policy.rego", Policy),
	).PrepareForEval(context.Background())
})

func evalAllow(t *testing.T, input PolicyInput) bool {
	t.Helper()

	q, err := preparedAllow()
	require.NoError(t, err)

	res, err := q.Eval(context.Background(), rego.EvalInput(input))
	require.NoError(t, err)
	require.NotEmpty(t, res)
	require.NotEmpty(t, res[0].Expressions)
	allow, ok := res[0].Expressions[0].Value.(bool)
	require.True(t, ok)
	return allow
}

func TestPolicyCollectionRouteBypass(t *testing.T) {
	t.Parallel()

	enabledOrg := map[string]Organization{
		"org1": {ID: "org1", Status: "enabled"},
	}

	cases := map[string]struct {
		input PolicyInput
		want  bool
	}{
		"restricted key listing branches in allowed project is allowed": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects/:projectID/branches",
					Scopes:       []string{"branch:read"},
					Organization: "org1",
					Project:      "projA",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"branch:read"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"branchY"},
				},
			},
			want: true,
		},
		"restricted key creating branch in allowed project is denied": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "POST",
					Path:         "/organizations/:organizationID/projects/:projectID/branches",
					Scopes:       []string{"branch:write"},
					Organization: "org1",
					Project:      "projA",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"branch:write"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"branchY"},
				},
			},
			want: false,
		},
		"restricted key listing projects in org is allowed": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects",
					Scopes:       []string{"project:read"},
					Organization: "org1",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"project:read"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"*"},
				},
			},
			want: true,
		},
		"restricted key creating project in org is denied": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "POST",
					Path:         "/organizations/:organizationID/projects",
					Scopes:       []string{"project:write"},
					Organization: "org1",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"project:write"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"*"},
				},
			},
			want: false,
		},
		"wildcard key listing projects is allowed": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects",
					Scopes:       []string{"project:read"},
					Organization: "org1",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"project:read"},
					Organizations: enabledOrg,
					Projects:      []string{"*"},
					Branches:      []string{"*"},
				},
			},
			want: true,
		},
		"wildcard key listing branches is allowed": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects/:projectID/branches",
					Scopes:       []string{"branch:read"},
					Organization: "org1",
					Project:      "projA",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"branch:read"},
					Organizations: enabledOrg,
					Projects:      []string{"*"},
					Branches:      []string{"*"},
				},
			},
			want: true,
		},
		"org-only route works with restricted project/branch key": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/members",
					Scopes:       []string{"org:read"},
					Organization: "org1",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"org:read"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"branchY"},
				},
			},
			want: true,
		},
		"restricted key on its own branch is allowed": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects/:projectID/branches/:branchID",
					Scopes:       []string{"branch:read"},
					Organization: "org1",
					Project:      "projA",
					Branch:       "branchY",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"branch:read"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"branchY"},
				},
			},
			want: true,
		},
		"restricted key on other project is denied": {
			input: PolicyInput{
				Request: RequestInput{
					Method:       "GET",
					Path:         "/organizations/:organizationID/projects/:projectID",
					Scopes:       []string{"project:read"},
					Organization: "org1",
					Project:      "projB",
				},
				Claims: ClaimsInput{
					Scopes:        []string{"project:read"},
					Organizations: enabledOrg,
					Projects:      []string{"projA"},
					Branches:      []string{"*"},
				},
			},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := evalAllow(t, tc.input)
			require.Equal(t, tc.want, got)
		})
	}
}

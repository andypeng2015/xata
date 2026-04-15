package reconciler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"xata/services/branch-operator/api/v1alpha1"
)

func TestDefaultPoolSize(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		override       string
		maxConnections string
		noPostgres     bool
		want           string
	}{
		"override wins over derivation": {
			override:       "250",
			maxConnections: "200",
			want:           "250",
		},
		"derives from max_connections": {
			maxConnections: "200",
			want:           "180",
		},
		"derives and floors": {
			maxConnections: "55",
			want:           "49",
		},
		"empty override falls through to derivation": {
			override:       "",
			maxConnections: "100",
			want:           "90",
		},
		"malformed override falls back to derivation": {
			override:       "not-a-number",
			maxConnections: "200",
			want:           "180",
		},
		"zero override falls back to derivation": {
			override:       "0",
			maxConnections: "200",
			want:           "180",
		},
		"no postgres section returns empty": {
			noPostgres: true,
			want:       "",
		},
		"missing max_connections returns empty": {
			want: "",
		},
		"malformed max_connections returns empty": {
			maxConnections: "abc",
			want:           "",
		},
		"zero max_connections returns empty": {
			maxConnections: "0",
			want:           "",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			branch := &v1alpha1.Branch{
				Spec: v1alpha1.BranchSpec{
					Pooler: &v1alpha1.PoolerSpec{DefaultPoolSize: tc.override},
				},
			}
			if !tc.noPostgres {
				branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{}
				if tc.maxConnections != "" {
					branch.Spec.ClusterSpec.Postgres.Parameters = []v1alpha1.PostgresParameter{
						{Name: "max_connections", Value: tc.maxConnections},
					}
				}
			}

			require.Equal(t, tc.want, defaultPoolSize(branch))
		})
	}
}

func TestMaxConnectionsFromBranch(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		noPostgres bool
		parameters []v1alpha1.PostgresParameter
		want       int
	}{
		"no postgres section": {
			noPostgres: true,
			want:       0,
		},
		"no parameters": {
			want: 0,
		},
		"max_connections set": {
			parameters: []v1alpha1.PostgresParameter{
				{Name: "max_connections", Value: "400"},
			},
			want: 400,
		},
		"max_connections alongside other params": {
			parameters: []v1alpha1.PostgresParameter{
				{Name: "shared_buffers", Value: "512MB"},
				{Name: "max_connections", Value: "100"},
			},
			want: 100,
		},
		"malformed value returns 0": {
			parameters: []v1alpha1.PostgresParameter{
				{Name: "max_connections", Value: "oops"},
			},
			want: 0,
		},
		"negative value returns 0": {
			parameters: []v1alpha1.PostgresParameter{
				{Name: "max_connections", Value: "-5"},
			},
			want: 0,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			branch := &v1alpha1.Branch{}
			if !tc.noPostgres {
				branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{
					Parameters: tc.parameters,
				}
			}

			require.Equal(t, tc.want, maxConnectionsFromBranch(branch))
		})
	}
}

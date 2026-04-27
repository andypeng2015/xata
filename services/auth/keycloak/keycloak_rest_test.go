package keycloak

import (
	"encoding/json"
	"testing"
	"time"

	"xata/services/auth/config"

	"xata/services/auth/api/spec"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFirstAttr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		attrs   map[string][]string
		key     string
		wantVal string
		wantOK  bool
	}{
		{
			name:    "nil map",
			attrs:   nil,
			key:     "anything",
			wantVal: "",
			wantOK:  false,
		},
		{
			name:    "key missing",
			attrs:   map[string][]string{"other": {"val"}},
			key:     "missing",
			wantVal: "",
			wantOK:  false,
		},
		{
			name:    "key present but empty slice",
			attrs:   map[string][]string{"k": {}},
			key:     "k",
			wantVal: "",
			wantOK:  false,
		},
		{
			name:    "key present with whitespace-only value",
			attrs:   map[string][]string{"k": {"   "}},
			key:     "k",
			wantVal: "",
			wantOK:  false,
		},
		{
			name:    "key present with valid value",
			attrs:   map[string][]string{"k": {"hello"}},
			key:     "k",
			wantVal: "hello",
			wantOK:  true,
		},
		{
			name:    "trims leading and trailing whitespace",
			attrs:   map[string][]string{"k": {"  trimmed  "}},
			key:     "k",
			wantVal: "trimmed",
			wantOK:  true,
		},
		{
			name:    "returns first element only",
			attrs:   map[string][]string{"k": {"first", "second"}},
			key:     "k",
			wantVal: "first",
			wantOK:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := firstAttr(tc.attrs, tc.key)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantVal, got)
		})
	}
}

func TestUserAttributesDeserialization(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		jsonBody   string
		wantAttrs  map[string][]string
		wantFields User
	}{
		{
			name:     "no attributes field",
			jsonBody: `{"id":"u1","username":"alice","email":"alice@example.com","emailVerified":true}`,
			wantFields: User{
				ID:            "u1",
				Username:      "alice",
				Email:         "alice@example.com",
				EmailVerified: true,
			},
		},
		{
			name:     "empty attributes",
			jsonBody: `{"id":"u2","username":"bob","attributes":{}}`,
			wantFields: User{
				ID:       "u2",
				Username: "bob",
			},
			wantAttrs: map[string][]string{},
		},
		{
			name: "marketplace attributes present",
			jsonBody: `{
				"id":"u3",
				"username":"carol",
				"email":"carol@example.com",
				"emailVerified":true,
				"attributes":{
					"marketplace":["aws"],
					"marketplaceRegisteredAt":["2026-03-23T00:00:00Z"],
					"awsAccountId":["123456789"],
					"awsCustomerId":["cust-abc"],
					"awsProductId":["prod-xyz"]
				}
			}`,
			wantFields: User{
				ID:            "u3",
				Username:      "carol",
				Email:         "carol@example.com",
				EmailVerified: true,
			},
			wantAttrs: map[string][]string{
				"marketplace":             {"aws"},
				"marketplaceRegisteredAt": {"2026-03-23T00:00:00Z"},
				"awsAccountId":            {"123456789"},
				"awsCustomerId":           {"cust-abc"},
				"awsProductId":            {"prod-xyz"},
			},
		},
		{
			name: "partial attributes",
			jsonBody: `{
				"id":"u4",
				"username":"dave",
				"attributes":{
					"marketplace":["aws"],
					"awsCustomerId":["cust-only"]
				}
			}`,
			wantFields: User{
				ID:       "u4",
				Username: "dave",
			},
			wantAttrs: map[string][]string{
				"marketplace":   {"aws"},
				"awsCustomerId": {"cust-only"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got User
			err := json.Unmarshal([]byte(tc.jsonBody), &got)
			require.NoError(t, err)

			assert.Equal(t, tc.wantFields.ID, got.ID)
			assert.Equal(t, tc.wantFields.Username, got.Username)
			assert.Equal(t, tc.wantFields.Email, got.Email)
			assert.Equal(t, tc.wantFields.EmailVerified, got.EmailVerified)

			if tc.wantAttrs != nil {
				assert.Equal(t, tc.wantAttrs, got.Attributes)
			} else {
				assert.Nil(t, got.Attributes)
			}
		})
	}
}

func TestExtractStatus(t *testing.T) {
	r := &restKC{}

	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC) // RFC3339-friendly

	cases := []struct {
		name   string
		attrs  map[string][]string
		expect spec.OrganizationStatus
	}{
		{
			name:  "when nil attributes the organization is enabled and lastUpdated is epoch",
			attrs: nil,
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Ok,
				AdminReason:     nil,
				BillingReason:   nil,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Enabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "admin disabled true (case-insensitive)",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"TrUe"},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: true,
				BillingStatus:   spec.Ok,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "admin disabled false, billing ok → enabled",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {string(spec.Ok)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Ok,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Enabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "billing overdue disables org",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {string(spec.InvoiceOverdue)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.InvoiceOverdue,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "billing no payment method disables org",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {string(spec.NoPaymentMethod)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.NoPaymentMethod,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "billing unrecognized disables org",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {"some-new-status"},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Unknown,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "reasons and valid lastUpdated",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"true"},
				OrganizationBillingStatusKey:   {string(spec.InvoiceOverdue)},
				OrganizationAdminReasonKey:     {"policy violation"},
				OrganizationBillingReasonKey:   {"card declined"},
				OrganizationLastUpdatedKey:     {ts.Format(time.RFC3339)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: true,
				BillingStatus:   spec.InvoiceOverdue,
				AdminReason:     new("policy violation"),
				BillingReason:   new("card declined"),
				LastUpdated:     ts,
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "invalid lastUpdated falls back to epoch",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationLastUpdatedKey:     {"not-a-timestamp"},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Ok,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Enabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "trims values via firstAttr",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {" false  "},
				OrganizationBillingStatusKey:   {"   ok "},
				OrganizationAdminReasonKey:     {"  "}, // empty after trim → nil
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Ok,
				AdminReason:     nil,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Enabled,
				UsageTier:       spec.T1,
			},
		},
		{
			name: "usage tier t2",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {string(spec.Ok)},
				OrganizationUsageTierKey:       {string(spec.T2)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.Ok,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Enabled,
				UsageTier:       spec.T2,
			},
		},
		{
			name: "deletion_requested disables org",
			attrs: map[string][]string{
				OrganizationDisabledByAdminKey: {"false"},
				OrganizationBillingStatusKey:   {string(spec.DeletionRequested)},
			},
			expect: spec.OrganizationStatus{
				DisabledByAdmin: false,
				BillingStatus:   spec.DeletionRequested,
				LastUpdated:     time.Unix(0, 0).UTC(),
				Status:          spec.Disabled,
				UsageTier:       spec.T1,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := r.extractStatus(tc.attrs)
			assert.Equal(t, tc.expect, got)
		})
	}
}

func TestBuildCreateOrganizationPayload_BillingRequired(t *testing.T) {
	t.Parallel()

	fixedID := "org_123"

	t.Run("BillingRequired=true -> no_payment_method", func(t *testing.T) {
		r := &restKC{
			authConfig: config.AuthConfig{
				BillingRequired: true,
			},
		}

		org := r.buildCreateOrganizationPayload(fixedID, OrganizationCreate{Name: "Acme"})

		require.NotNil(t, org.Attributes)
		assert.Equal(t, OrganizationBillingStatusNoPaymentMethod, org.Attributes[OrganizationBillingStatusKey][0])
		assert.Equal(t, "Organization created, no payment method set", org.Attributes[OrganizationBillingReasonKey][0])

		// sanity checks
		assert.Equal(t, "Acme", org.Attributes["displayName"][0])
		assert.Equal(t, "false", org.Attributes[OrganizationDisabledByAdminKey][0])
		assert.Equal(t, fixedID, org.Name)
		assert.Equal(t, fixedID, org.Alias)

		lu := org.Attributes[OrganizationLastUpdatedKey][0]
		_, err := time.Parse(time.RFC3339, lu)
		assert.NoError(t, err)
	})

	t.Run("BillingRequired=false -> ok", func(t *testing.T) {
		r := &restKC{
			authConfig: config.AuthConfig{
				BillingRequired: false,
			},
		}

		org := r.buildCreateOrganizationPayload(fixedID, OrganizationCreate{Name: "Acme"})

		require.NotNil(t, org.Attributes)
		assert.Equal(t, string(spec.Ok), org.Attributes[OrganizationBillingStatusKey][0])
		assert.Equal(t, "Organization enabled by default since billing is not required", org.Attributes[OrganizationBillingReasonKey][0])

		lu := org.Attributes[OrganizationLastUpdatedKey][0]
		_, err := time.Parse(time.RFC3339, lu)
		assert.NoError(t, err)
	})
}

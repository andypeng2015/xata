package extensions

import (
	"slices"
	"testing"
)

func TestYAMLLoading(t *testing.T) {
	// Test that extensions are loaded from embedded YAML
	if imageExtensions == nil {
		t.Fatal("imageExtensions is nil after init()")
	}

	if len(imageExtensions.Offerings) == 0 {
		t.Fatal("No offerings loaded from YAML")
	}

	if imageExtensions.LastUpdated == "" {
		t.Error("LastUpdated field should not be empty")
	}
}

func TestGetExtensions(t *testing.T) {
	testCases := []struct {
		name           string
		image          string
		expectNil      bool
		expectMinCount int
		checkExtension string
		checkPreload   bool
	}{
		{
			name:           "analytics:17 returns extensions",
			image:          "analytics:17",
			expectNil:      false,
			expectMinCount: 1,
			checkExtension: "pg_duckdb",
			checkPreload:   true,
		},
		{
			name:           "postgres:17 returns extensions",
			image:          "postgres:17",
			expectNil:      false,
			expectMinCount: 1,
			checkExtension: "pg_stat_statements",
			checkPreload:   true,
		},
		{
			name:      "non-existent offering returns nil",
			image:     "nonexistent:17",
			expectNil: true,
		},
		{
			name:      "non-existent version returns nil",
			image:     "analytics:99",
			expectNil: true,
		},
		{
			name:      "invalid format returns nil",
			image:     "invalid-format",
			expectNil: true,
		},
		{
			name:      "empty string returns nil",
			image:     "",
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			extensions := GetExtensions(tc.image)

			if tc.expectNil {
				if extensions != nil {
					t.Errorf("Expected nil, got %d extensions", len(extensions))
				}
				return
			}

			if extensions == nil {
				t.Fatal("Expected extensions, got nil")
			}

			if len(extensions) < tc.expectMinCount {
				t.Errorf("Expected at least %d extensions, got %d", tc.expectMinCount, len(extensions))
			}

			if tc.checkExtension != "" {
				found := false
				for _, ext := range extensions {
					if ext.Name == tc.checkExtension {
						found = true
						if ext.Version == "" {
							t.Errorf("%s should have a version", tc.checkExtension)
						}
						if tc.checkPreload && !ext.PreloadRequired {
							t.Errorf("%s should require preload", tc.checkExtension)
						}
						break
					}
				}
				if !found {
					t.Errorf("%s should be in extensions", tc.checkExtension)
				}
			}
		})
	}
}

func TestIsExtensionAvailable(t *testing.T) {
	testCases := []struct {
		name          string
		image         string
		extensionName string
		expected      bool
	}{
		{
			name:          "pg_duckdb available in analytics:17",
			image:         "analytics:17",
			extensionName: "pg_duckdb",
			expected:      true,
		},
		{
			name:          "pg_stat_statements available in analytics:17",
			image:         "analytics:17",
			extensionName: "pg_stat_statements",
			expected:      true,
		},
		{
			name:          "pg_stat_statements available in postgres:17",
			image:         "postgres:17",
			extensionName: "pg_stat_statements",
			expected:      true,
		},
		{
			name:          "non-existent extension not available",
			image:         "analytics:17",
			extensionName: "nonexistent_extension",
			expected:      false,
		},
		{
			name:          "extension not available for non-existent offering",
			image:         "nonexistent:17",
			extensionName: "pg_stat_statements",
			expected:      false,
		},
		{
			name:          "extension not available for invalid format",
			image:         "invalid-format",
			extensionName: "pg_stat_statements",
			expected:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsExtensionAvailable(tc.image, tc.extensionName)
			if result != tc.expected {
				t.Errorf("IsExtensionAvailable(%s, %s): expected %v, got %v",
					tc.image, tc.extensionName, tc.expected, result)
			}
		})
	}
}

func TestGetExtension(t *testing.T) {
	testCases := []struct {
		name          string
		image         string
		extensionName string
		expectNil     bool
		checkVersion  bool
		checkDesc     bool
	}{
		{
			name:          "get pg_duckdb from analytics:17",
			image:         "analytics:17",
			extensionName: "pg_duckdb",
			expectNil:     false,
			checkVersion:  true,
			checkDesc:     true,
		},
		{
			name:          "get pg_stat_statements from postgres:17",
			image:         "postgres:17",
			extensionName: "pg_stat_statements",
			expectNil:     false,
			checkVersion:  true,
			checkDesc:     true,
		},
		{
			name:          "non-existent extension returns nil",
			image:         "analytics:17",
			extensionName: "nonexistent",
			expectNil:     true,
		},
		{
			name:          "extension from non-existent offering returns nil",
			image:         "nonexistent:17",
			extensionName: "pg_duckdb",
			expectNil:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ext := GetExtension(tc.image, tc.extensionName)

			if tc.expectNil {
				if ext != nil {
					t.Errorf("Expected nil, got extension %s", ext.Name)
				}
				return
			}

			if ext == nil {
				t.Fatal("Expected extension, got nil")
			} else if ext.Name != tc.extensionName {
				t.Errorf("Expected name %s, got %s", tc.extensionName, ext.Name)
			}

			if tc.checkVersion && ext.Version == "" {
				t.Error("Extension version should not be empty")
			}

			if tc.checkDesc && ext.Description == "" {
				t.Error("Extension description should not be empty")
			}
		})
	}
}

func TestGetPreloadRequiredExtensions(t *testing.T) {
	testCases := []struct {
		name           string
		image          string
		expectNil      bool
		expectMinCount int
		mustInclude    []string
	}{
		{
			name:           "analytics:17 has preload extensions",
			image:          "analytics:17",
			expectNil:      false,
			expectMinCount: 1,
			mustInclude:    []string{"pg_duckdb", "pg_stat_statements"},
		},
		{
			name:           "postgres:17 has preload extensions",
			image:          "postgres:17",
			expectNil:      false,
			expectMinCount: 1,
			mustInclude:    []string{"pg_stat_statements"},
		},
		{
			name:      "non-existent offering returns nil",
			image:     "nonexistent:17",
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			preloadExtensions := GetPreloadRequiredExtensions(tc.image)

			if tc.expectNil {
				if preloadExtensions != nil {
					t.Errorf("Expected nil, got %d extensions", len(preloadExtensions))
				}
				return
			}

			if preloadExtensions == nil {
				t.Fatal("Expected extensions, got nil")
			}

			if len(preloadExtensions) < tc.expectMinCount {
				t.Errorf("Expected at least %d extensions, got %d", tc.expectMinCount, len(preloadExtensions))
			}

			// Verify all returned extensions have PreloadRequired=true
			for _, ext := range preloadExtensions {
				if !ext.PreloadRequired {
					t.Errorf("Extension %s should have PreloadRequired=true", ext.Name)
				}
			}

			// Check that required extensions are included
			for _, requiredExt := range tc.mustInclude {
				found := false
				for _, ext := range preloadExtensions {
					if ext.Name == requiredExt {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s should be in preload required extensions", requiredExt)
				}
			}
		})
	}
}

func TestGetAllOfferings(t *testing.T) {
	testCases := []struct {
		name        string
		mustInclude []string
	}{
		{
			name:        "includes expected offerings",
			mustInclude: []string{"analytics", "postgres", "experimental"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			offerings := GetAllOfferings()
			if len(offerings) == 0 {
				t.Fatal("GetAllOfferings returned empty list")
			}

			for _, expected := range tc.mustInclude {
				found := slices.Contains(offerings, expected)
				if !found {
					t.Errorf("%s should be in offerings list", expected)
				}
			}
		})
	}
}

func TestGetVersionsForOffering(t *testing.T) {
	testCases := []struct {
		name        string
		offering    string
		expectNil   bool
		mustInclude []string
	}{
		{
			name:        "analytics has version 17",
			offering:    "analytics",
			expectNil:   false,
			mustInclude: []string{"17"},
		},
		{
			name:        "postgres has versions 14-17",
			offering:    "postgres",
			expectNil:   false,
			mustInclude: []string{"14", "15", "16", "17"},
		},
		{
			name:      "non-existent offering returns nil",
			offering:  "nonexistent",
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			versions := GetVersionsForOffering(tc.offering)

			if tc.expectNil {
				if versions != nil {
					t.Errorf("Expected nil, got %d versions", len(versions))
				}
				return
			}

			if versions == nil {
				t.Fatal("Expected versions, got nil")
			}

			for _, expected := range tc.mustInclude {
				found := slices.Contains(versions, expected)
				if !found {
					t.Errorf("%s should be in versions for %s", expected, tc.offering)
				}
			}
		})
	}
}

func TestExtensionSpecFields(t *testing.T) {
	testCases := []struct {
		name  string
		image string
	}{
		{name: "analytics:17", image: "analytics:17"},
		{name: "postgres:17", image: "postgres:17"},
		{name: "experimental:17", image: "experimental:17"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			extensions := GetExtensions(tc.image)
			if len(extensions) == 0 {
				t.Fatal("No extensions loaded")
			}

			// Verify all extensions have required fields
			for _, ext := range extensions {
				if ext.Name == "" {
					t.Error("Extension name should not be empty")
				}
				if ext.Version == "" {
					t.Errorf("Extension %s version should not be empty", ext.Name)
				}
				if ext.Description == "" {
					t.Errorf("Extension %s description should not be empty", ext.Name)
				}
			}
		})
	}
}

func TestExtensionTypes(t *testing.T) {
	testCases := []struct {
		name          string
		image         string
		extensionName string
		expectedType  string
	}{
		{
			name:          "wal2json is a plugin",
			image:         "postgres:17",
			extensionName: "wal2json",
			expectedType:  "plugin",
		},
		{
			name:          "auto_explain is a module",
			image:         "postgres:17",
			extensionName: "auto_explain",
			expectedType:  "module",
		},
		{
			name:          "pg_stat_statements is an extension",
			image:         "postgres:17",
			extensionName: "pg_stat_statements",
			expectedType:  "extension",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ext := GetExtension(tc.image, tc.extensionName)
			if ext == nil {
				t.Fatalf("Extension %s not found in %s", tc.extensionName, tc.image)
			}
			if ext.Type != tc.expectedType {
				t.Errorf("Expected type %s for %s, got %s", tc.expectedType, tc.extensionName, ext.Type)
			}
		})
	}
}

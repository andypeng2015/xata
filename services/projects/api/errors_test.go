package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xata/internal/xvalidator"
)

func TestErrorStatusCodes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err        error
		wantCode   int
		wantSubstr string
	}{
		"ErrorBranchConflict": {
			err:        ErrorBranchConflict{BranchID: "br-1"},
			wantCode:   409,
			wantSubstr: "modified concurrently",
		},
		"ErrorBranchNotFound": {
			err:        ErrorBranchNotFound{BranchID: "br-1"},
			wantCode:   404,
			wantSubstr: "not found",
		},
		"ErrorInvalidParam": {
			err:        ErrorInvalidParam{BranchName: "br-1", Param: "name", Message: "too long"},
			wantCode:   400,
			wantSubstr: "invalid parameter",
		},
		"ErrorBranchUpdateForbidden": {
			err:        ErrorBranchUpdateForbidden{BranchID: "br-1"},
			wantCode:   403,
			wantSubstr: "temporarily unavailable",
		},
		"ErrorParentBranchUnhealthy": {
			err:        ErrorParentBranchUnhealthy{ParentID: "br-1"},
			wantCode:   412,
			wantSubstr: "not healthy",
		},
		"ErrorBranchCreationDisabled": {
			err:        ErrorBranchCreationDisabled{},
			wantCode:   503,
			wantSubstr: "temporarily disabled",
		},
		"ErrorCredentialsForBranchNotFound": {
			err:        ErrorCredentialsForBranchNotFound{BranchID: "br-1", Username: "xata"},
			wantCode:   404,
			wantSubstr: "not found",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			type statusCoder interface {
				StatusCode() int
			}
			sc, ok := tt.err.(statusCoder)
			require.True(t, ok)
			require.Equal(t, tt.wantCode, sc.StatusCode())
			require.Contains(t, tt.err.Error(), tt.wantSubstr)
		})
	}
}

func TestIsDescriptionValid(t *testing.T) {
	t.Parallel()

	longDescription := "averylongdescriptionmadefortestingaverylongdescriptionmadefortesting"
	shortDescription := "shortokdescription-09"
	invalidCharsDescription := "-shortinvalid"

	errorMaxLength := xvalidator.ErrorMaxLength{Limit: MaxBranchDescriptionLength}
	errorInvalid := ErrorInvalidDescription{
		Message:     fmt.Sprintf("invalid branch description %s", invalidCharsDescription),
		Description: invalidCharsDescription,
	}

	tests := []struct {
		name         string
		description  string
		wantError    bool
		errorMessage string
	}{
		{
			name:         "tooLong",
			description:  longDescription,
			wantError:    true,
			errorMessage: errorMaxLength.Error(),
		},
		{
			name:        "ok",
			description: shortDescription,
			wantError:   false,
		},
		{
			name:         "invalid",
			description:  invalidCharsDescription,
			wantError:    true,
			errorMessage: errorInvalid.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsBranchDescriptionValid(tt.description)
			if tt.wantError == true {
				assert.Error(t, got)
				assert.Equal(t, tt.errorMessage, got.Error())

			} else {
				assert.NoError(t, got)
			}
		})
	}
}

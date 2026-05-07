package o11y

import (
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCErrorLogLevel(t *testing.T) {
	tests := map[string]struct {
		err  error
		want zerolog.Level
	}{
		"not found": {
			err:  status.Error(codes.NotFound, "missing"),
			want: zerolog.WarnLevel,
		},
		"permission denied": {
			err:  status.Error(codes.PermissionDenied, "denied"),
			want: zerolog.ErrorLevel,
		},
		"internal": {
			err:  status.Error(codes.Internal, "broken"),
			want: zerolog.ErrorLevel,
		},
		"plain error": {
			err:  errors.New("broken"),
			want: zerolog.ErrorLevel,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, grpcLogLevel(tc.err))
		})
	}
}

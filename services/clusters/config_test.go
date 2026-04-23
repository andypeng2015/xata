package clusters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	tests := map[string]struct {
		cfg     Config
		wantErr string
	}{
		"valid config": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
			},
		},
		"missing storage class": {
			cfg: Config{
				ClustersVolumeSnapshotClass: "xatastor",
			},
			wantErr: "storage class is required",
		},
		"missing volume snapshot class": {
			cfg: Config{
				ClustersStorageClass: "xatastor",
			},
			wantErr: "volume snapshot class is required",
		},
		"xvol child storage class not listed in xvol storage classes": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				XVolStorageClasses:          []string{"xatastor"},
				XVolChildStorageClass:       "xatastor-slot",
			},
			wantErr: `xvol child storage class "xatastor-slot" must be listed in xvol storage classes`,
		},
		"xvol disabled (both empty)": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
			},
		},
		"xvol configured": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				XVolStorageClasses:          []string{"xatastor", "xatastor-slot"},
				XVolChildStorageClass:       "xatastor-slot",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

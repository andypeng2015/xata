package clusters

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"

	clustersv1 "xata/gen/proto/clusters/v1"
)

func TestBuildClusterStatus(t *testing.T) {
	tests := map[string]struct {
		cluster        *apiv1.Cluster
		wantStatus     string
		wantStatusType clustersv1.ClusterStatus_StatusType
		wantInstances  map[string]*clustersv1.InstanceStatus
	}{
		"healthy with reconciled instances": {
			cluster: &apiv1.Cluster{
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					Instances:      2,
					ReadyInstances: 2,
					CurrentPrimary: "c-1",
					InstancesStatus: map[apiv1.PodStatus][]string{
						apiv1.PodHealthy: {"c-1", "c-2"},
					},
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: string(apiv1.PodHealthy), Primary: true},
				"c-2": {Status: string(apiv1.PodHealthy)},
			},
		},
		"healthy with empty instance status downgrades to transient": {
			cluster: &apiv1.Cluster{
				Status: apiv1.ClusterStatus{
					Phase:           apiv1.PhaseHealthy,
					Instances:       2,
					ReadyInstances:  0,
					InstanceNames:   []string{"c-1", "c-2"},
					InstancesStatus: nil,
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: InstanceStatusUnknown},
				"c-2": {Status: InstanceStatusUnknown},
			},
		},
		"healthy with partial ready count downgrades to transient": {
			cluster: &apiv1.Cluster{
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					Instances:      2,
					ReadyInstances: 1,
					CurrentPrimary: "c-1",
					InstancesStatus: map[apiv1.PodStatus][]string{
						apiv1.PodHealthy: {"c-1", "c-2"},
					},
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: string(apiv1.PodHealthy), Primary: true},
				"c-2": {Status: string(apiv1.PodHealthy)},
			},
		},
		"healthy with primary missing from instance status downgrades to transient": {
			cluster: &apiv1.Cluster{
				Status: apiv1.ClusterStatus{
					Phase:           apiv1.PhaseHealthy,
					Instances:       2,
					ReadyInstances:  2,
					CurrentPrimary:  "c-1",
					InstancesStatus: map[apiv1.PodStatus][]string{},
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: InstanceStatusUnknown, Primary: true},
			},
		},
		"healthy with xata role pending downgrades to transient": {
			cluster: &apiv1.Cluster{
				Spec: apiv1.ClusterSpec{
					Managed: &apiv1.ManagedConfiguration{
						Roles: []apiv1.RoleConfiguration{{Name: "xata"}},
					},
				},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					Instances:      1,
					ReadyInstances: 1,
					CurrentPrimary: "c-1",
					InstancesStatus: map[apiv1.PodStatus][]string{
						apiv1.PodHealthy: {"c-1"},
					},
					ManagedRolesStatus: apiv1.ManagedRoles{
						ByStatus: map[apiv1.RoleStatus][]string{
							apiv1.RoleStatusPendingReconciliation: {"xata"},
						},
					},
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: string(apiv1.PodHealthy), Primary: true},
			},
		},
		"healthy with xata role reconciled stays healthy": {
			cluster: &apiv1.Cluster{
				Spec: apiv1.ClusterSpec{
					Managed: &apiv1.ManagedConfiguration{
						Roles: []apiv1.RoleConfiguration{{Name: "xata"}},
					},
				},
				Status: apiv1.ClusterStatus{
					Phase:          apiv1.PhaseHealthy,
					Instances:      1,
					ReadyInstances: 1,
					CurrentPrimary: "c-1",
					InstancesStatus: map[apiv1.PodStatus][]string{
						apiv1.PodHealthy: {"c-1"},
					},
					ManagedRolesStatus: apiv1.ManagedRoles{
						ByStatus: map[apiv1.RoleStatus][]string{
							apiv1.RoleStatusReconciled: {"xata"},
						},
					},
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
			wantInstances: map[string]*clustersv1.InstanceStatus{
				"c-1": {Status: string(apiv1.PodHealthy), Primary: true},
			},
		},
		"hibernated trumps healthy phase": {
			cluster: &apiv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{hibernationAnnotation: hibernationEnabled},
				},
				Status: apiv1.ClusterStatus{
					Phase: apiv1.PhaseHealthy,
				},
			},
			wantStatus:     apiv1.PhaseHealthy,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
			wantInstances:  map[string]*clustersv1.InstanceStatus{},
		},
		"transient phase stays transient": {
			cluster: &apiv1.Cluster{
				Status: apiv1.ClusterStatus{
					Phase:     apiv1.PhaseCreatingReplica,
					Instances: 2,
				},
			},
			wantStatus:     apiv1.PhaseCreatingReplica,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances:  map[string]*clustersv1.InstanceStatus{},
		},
		"unknown phase yields transient": {
			cluster:        &apiv1.Cluster{},
			wantStatus:     StatusUnknown,
			wantStatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
			wantInstances:  map[string]*clustersv1.InstanceStatus{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := BuildClusterStatus(tt.cluster)
			require.Equal(t, tt.wantStatus, got.Status)
			require.Equal(t, tt.wantStatusType, got.StatusType)
			require.Equal(t, tt.wantInstances, got.Instances)
		})
	}
}

package ipfiltering

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewFilter_NoConfigMap(t *testing.T) {
	client := fake.NewClientset()
	ctx := t.Context()

	f, err := NewFilter(ctx, client, "test-ns", "ipfiltering")
	require.NoError(t, err)

	// No rules → all IPs allowed
	require.True(t, f.IsAllowed("some-branch", "1.2.3.4:5432"))
}

func TestNewFilter_WithRules(t *testing.T) {
	rules := map[string]IPFilteringConfig{
		"branch-1": {
			Enabled: true,
			Allowed: []string{"10.0.0.0/8", "192.168.1.1"},
		},
		"branch-2": {
			Enabled: false,
			Allowed: []string{"172.16.0.0/12"},
		},
		"branch-3": {
			Enabled: true,
			Allowed: []string{"not-a-cidr", "also-invalid"},
		},
		"branch-4": {
			Enabled: true,
			Allowed: []string{},
		},
	}
	rulesJSON, err := json.Marshal(rules)
	require.NoError(t, err)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ipfiltering",
			Namespace: "test-ns",
		},
		Data: map[string]string{
			ConfigMapKey: string(rulesJSON),
		},
	}

	client := fake.NewClientset(cm)
	ctx := t.Context()

	f, err := NewFilter(ctx, client, "test-ns", "ipfiltering")
	require.NoError(t, err)

	tests := map[string]struct {
		branchID string
		ip       string
		want     bool
	}{
		"allowed IP in CIDR range": {
			branchID: "branch-1",
			ip:       "10.1.2.3",
			want:     true,
		},
		"allowed exact IP": {
			branchID: "branch-1",
			ip:       "192.168.1.1",
			want:     true,
		},
		"denied IP not in range": {
			branchID: "branch-1",
			ip:       "172.16.0.1",
			want:     false,
		},
		"disabled branch allows all": {
			branchID: "branch-2",
			ip:       "1.2.3.4",
			want:     true,
		},
		"unknown branch allows all": {
			branchID: "unknown",
			ip:       "1.2.3.4",
			want:     true,
		},
		"enabled with all invalid CIDRs denies all": {
			branchID: "branch-3",
			ip:       "1.2.3.4",
			want:     false,
		},
		"enabled with empty CIDR list denies all": {
			branchID: "branch-4",
			ip:       "1.2.3.4",
			want:     false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := f.IsAllowed(tc.branchID, tc.ip)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestFilter_ConfigMapUpdate(t *testing.T) {
	rules := map[string]IPFilteringConfig{
		"branch-1": {
			Enabled: true,
			Allowed: []string{"10.0.0.0/8"},
		},
	}
	rulesJSON, err := json.Marshal(rules)
	require.NoError(t, err)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ipfiltering",
			Namespace: "test-ns",
		},
		Data: map[string]string{
			ConfigMapKey: string(rulesJSON),
		},
	}

	client := fake.NewClientset(cm)
	ctx := t.Context()

	f, err := NewFilter(ctx, client, "test-ns", "ipfiltering")
	require.NoError(t, err)

	// Verify initial rules
	require.True(t, f.IsAllowed("branch-1", "10.1.2.3:5432"))
	require.False(t, f.IsAllowed("branch-1", "192.168.1.1:5432"))

	// Update ConfigMap with new rules
	updatedRules := map[string]IPFilteringConfig{
		"branch-1": {
			Enabled: true,
			Allowed: []string{"192.168.0.0/16"},
		},
	}
	updatedJSON, err := json.Marshal(updatedRules)
	require.NoError(t, err)

	cm.Data[ConfigMapKey] = string(updatedJSON)
	_, err = client.CoreV1().ConfigMaps("test-ns").Update(ctx, cm, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Wait for informer to process update
	require.Eventually(t, func() bool {
		return f.IsAllowed("branch-1", "192.168.1.1:5432")
	}, 5*time.Second, 50*time.Millisecond)

	// Old range should now be denied
	require.Eventually(t, func() bool {
		return !f.IsAllowed("branch-1", "10.1.2.3:5432")
	}, 5*time.Second, 50*time.Millisecond)
}

func TestFilter_ConfigMapDeleted(t *testing.T) {
	rules := map[string]IPFilteringConfig{
		"branch-1": {
			Enabled: true,
			Allowed: []string{"10.0.0.0/8"},
		},
	}
	rulesJSON, err := json.Marshal(rules)
	require.NoError(t, err)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ipfiltering",
			Namespace: "test-ns",
		},
		Data: map[string]string{
			ConfigMapKey: string(rulesJSON),
		},
	}

	client := fake.NewClientset(cm)
	ctx := t.Context()

	f, err := NewFilter(ctx, client, "test-ns", "ipfiltering")
	require.NoError(t, err)

	// IP outside allowed range should be denied
	require.False(t, f.IsAllowed("branch-1", "192.168.1.1:5432"))

	// Delete ConfigMap
	err = client.CoreV1().ConfigMaps("test-ns").Delete(ctx, "ipfiltering", metav1.DeleteOptions{})
	require.NoError(t, err)

	// After deletion, all IPs should be allowed
	require.Eventually(t, func() bool {
		return f.IsAllowed("branch-1", "192.168.1.1:5432")
	}, 5*time.Second, 50*time.Millisecond)
}

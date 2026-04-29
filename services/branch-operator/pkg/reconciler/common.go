package reconciler

import (
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"xata/services/branch-operator/api/v1alpha1"
)

const (
	kubeTrue  = "true"
	kubeFalse = "false"
)

// ensureLabels adds the canonical set of labels for resources managed by the
// operator to the given object, merging in any user-provided labels from
// InheritedMetadata.
func ensureLabels(obj metav1.Object, inheritedMetadata *v1alpha1.InheritedMetadata) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	// Apply user labels
	if inheritedMetadata != nil && inheritedMetadata.Labels != nil {
		maps.Copy(labels, inheritedMetadata.Labels)
	}

	// Apply operator labels
	labels["app.kubernetes.io/managed-by"] = OperatorName

	obj.SetLabels(labels)
}

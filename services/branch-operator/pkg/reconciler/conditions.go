package reconciler

import (
	"context"
	"errors"
	"fmt"

	"xata/services/branch-operator/api/v1alpha1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionError indicates that a specific condition on a Branch should be set
// in response to the error.
type ConditionError struct {
	ConditionType   string
	ConditionReason string
	Err             error
}

func (e *ConditionError) Error() string {
	return fmt.Sprintf("%s - %s: %s", e.ConditionType, e.ConditionReason, e.Err.Error())
}

func (e *ConditionError) Unwrap() error {
	return e.Err
}

// ensureStatusConditions initializes all status conditions for a Branch to
// their default state if they are not already set.
func (r *BranchReconciler) ensureStatusConditions(ctx context.Context, br *v1alpha1.Branch) error {
	if len(br.Status.Conditions) != 0 {
		return nil
	}

	br.Status.Conditions = make([]metav1.Condition, 0)
	readyCondition := metav1.Condition{
		Type:               v1alpha1.BranchReadyConditionType,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: br.Generation,
		Reason:             v1alpha1.AwaitingReconciliationReason,
		Message:            v1alpha1.BranchConditionMessages[v1alpha1.AwaitingReconciliationReason],
	}

	meta.SetStatusCondition(&br.Status.Conditions, readyCondition)

	return r.Status().Update(ctx, br)
}

// setStatusConditionFromError sets a status condition to False on the Branch
// based on the provided error. If the error is a ConditionError, the specified
// condition type and reason are used. Otherwise, the Branch Ready condition is
// set to False with a generic reconciliation failure reason.
func (r *BranchReconciler) setStatusConditionFromError(ctx context.Context, br *v1alpha1.Branch, err error) error {
	if err == nil {
		return nil
	}

	var condErr *ConditionError
	ok := errors.As(err, &condErr)
	if ok {
		return r.setStatusCondition(ctx, br, condErr.ConditionType, metav1.ConditionFalse, condErr.ConditionReason)
	}

	return r.setStatusCondition(ctx, br, v1alpha1.BranchReadyConditionType, metav1.ConditionFalse, v1alpha1.ReconciliationFailedReason)
}

// setStatusCondition sets the specified condition on the Branch status
func (r *BranchReconciler) setStatusCondition(ctx context.Context,
	br *v1alpha1.Branch,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
) error {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            v1alpha1.BranchConditionMessages[reason],
		ObservedGeneration: br.Generation,
	}

	meta.SetStatusCondition(&br.Status.Conditions, condition)

	return r.Status().Update(ctx, br)
}

// setReadyConditionTrue sets the Branch Ready condition to True with the given
// reason
func (r *BranchReconciler) setReadyConditionTrue(ctx context.Context, br *v1alpha1.Branch, reason string) error {
	return r.setStatusCondition(ctx, br, v1alpha1.BranchReadyConditionType, metav1.ConditionTrue, reason)
}

// setReadyConditionUnknown sets the Branch Ready condition to Unknown with the given
// reason
func (r *BranchReconciler) setReadyConditionUnknown(ctx context.Context, br *v1alpha1.Branch, reason string) error {
	return r.setStatusCondition(ctx, br, v1alpha1.BranchReadyConditionType, metav1.ConditionUnknown, reason)
}

// setLastErrorStatus sets the LastError field in the Branch status
func (r *BranchReconciler) setLastErrorStatus(ctx context.Context, br *v1alpha1.Branch, err error) error {
	var msg string
	if err != nil {
		msg = err.Error()
	}

	br.Status.LastError = msg
	return r.Status().Update(ctx, br)
}

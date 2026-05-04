package wakeup

import (
	"context"
	"fmt"
	"time"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
)

const (
	PoolClusterOwnerKey = ".metadata.ownerReferences[controller=true,kind=ClusterPool].name"
	ReconcilerName      = "wakeup-reconciler"
)

// +kubebuilder:rbac:groups=xata.io,resources=wakeuprequests,verbs=get;list;watch;delete,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=wakeuprequests/status,verbs=get;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=branches,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=xata.io,resources=clusterpools,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=xvols,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch,namespace=xatastor

// WakeupReconciler reconciles a WakeupRequest object
type WakeupReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	CSINodeNamespace        string
	CSINodePort             int
	WakeupRequestTTL        time.Duration
	MaxConcurrentReconciles int
}

// Reconcile handles reconciliation for WakeupRequest resources
func (r *WakeupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.Log.WithName(ReconcilerName)

	log.Info("reconciling WakeupRequest", "namespacedName", req.NamespacedName)

	// Fetch the WakeupRequest resource
	wr := &v1alpha1.WakeupRequest{}
	if err := r.Get(ctx, req.NamespacedName, wr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Don't reconcile if the WakeupRequest is being deleted
	if !wr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Ensure status conditions are initialized
	if err := r.ensureStatusConditions(ctx, wr); err != nil {
		log.Error(err, "ensuring status conditions")
		return ctrl.Result{}, err
	}

	// Skip reconciliation if the WakeupRequest has succeeded and delete it if
	// its TTL has expired
	if r.isWakeupSucceeded(wr) {
		return r.deleteAfterTTL(ctx, wr)
	}

	// Skip reconciliation if the WakeupRequest has terminally failed
	if r.isWakeupFailed(wr) {
		return ctrl.Result{}, nil
	}

	// Set ObservedGeneration to the WakeupRequest's current Generation
	if wr.Status.ObservedGeneration != wr.Generation {
		wr.Status.ObservedGeneration = wr.Generation
		if err := r.Status().Update(ctx, wr); err != nil {
			log.Error(err, "updating WakeupRequest status")
			return ctrl.Result{}, err
		}
	}

	// Defer setting status based on errors that occur during reconciliation
	var err error
	defer func() {
		r.setStatusConditionFromError(ctx, wr, err)
		r.setLastErrorStatus(ctx, wr, err)
	}()

	// Set the WakeupRequest status to InProgress. This ensures that we get an
	// optimistic concurrency error here if the WakeupRequest read from the cache
	// is stale and has actually already succeeded or failed
	err = r.setSucceededCondition(ctx, wr, metav1.ConditionUnknown, v1alpha1.WakeupInProgressReason)
	if err != nil {
		log.Error(err, "setting InProgress status")
		return ctrl.Result{}, err
	}

	// Fetch the branch to be woken up
	branch, err := r.getBranch(ctx, wr.Spec.BranchName)
	if err != nil {
		log.Error(err, "fetching Branch", "branchName", wr.Spec.BranchName)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// If the branch doesn't need to be woken up (ie it already has a cluster
	// assigned) then just set the Succeeded condition to True and skip further
	// reconciliation.
	if branch.Spec.ClusterSpec.Name != nil {
		err = r.setSucceededCondition(ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)
		if err != nil {
			return ctrl.Result{}, ignoreTerminal(err)
		}
		return ctrl.Result{RequeueAfter: r.WakeupRequestTTL}, nil
	}

	// Get the XVol name from the Branch status
	xvolName, err := getXVolName(branch)
	if err != nil {
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Validate that the XVol is in a state that can be used as the target of a
	// wakeup operation
	err = r.validateXVolStatus(ctx, xvolName)
	if err != nil {
		log.Error(err, "validating XVol status", "xvolName", xvolName)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Get the pool name from the Branch annotation
	poolName, err := r.wakeupPoolName(branch)
	if err != nil {
		log.Error(err, "getting pool name", "branchName", branch.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Remove a cluster from the wakeup pool
	cluster, err := r.takeClusterFromPool(ctx, wr.Namespace, poolName)
	if err != nil {
		log.Error(err, "taking cluster from pool", "poolName", poolName)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// At this point we have taken a Cluster from the pool; if any subsequent
	// step fails we would have an orphaned Cluster. Ensure that if any error
	// occurs from this point forward, we attempt to clean up by deleting the
	// Cluster that was taken from the pool.
	defer func() {
		if err != nil {
			r.Delete(ctx, cluster)
		}
	}()

	// Get the PV backing the Cluster's primary instance
	pv, err := r.getPersistentVolume(ctx, cluster)
	if err != nil {
		log.Error(err, "getting PV for cluster", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Get the slot ID from the Cluster's primary PV
	slotID, err := getSlotID(pv)
	if err != nil {
		log.Error(err, "getting slot ID for cluster", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Find the CSI node plugin pod on the same node as the primary
	csiNodePod, err := r.getCSINodePod(ctx, cluster)
	if err != nil {
		log.Error(err, "getting CSI node pod for cluster", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Call the WakeUp RPC on the CSI node pod
	err = r.wakeUp(ctx, csiNodePod, slotID, xvolName, pv.Name)
	if err != nil {
		log.Error(err, "calling WakeUp RPC", "clusterName", cluster.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Assign the cluster to the branch
	err = r.assignClusterToBranch(ctx, branch, cluster.Name)
	if err != nil {
		log.Error(err, "assigning cluster to Branch", "branchName", branch.Name)
		return ctrl.Result{}, ignoreTerminal(err)
	}

	// Set the WakeupRequest Succeeded condition to True
	err = r.setSucceededCondition(ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)
	if err != nil {
		log.Error(err, "setting Succeeded condition to True")
		return ctrl.Result{}, ignoreTerminal(err)
	}

	return ctrl.Result{RequeueAfter: r.WakeupRequestTTL}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *WakeupReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	if err := setupIndexers(ctx, mgr); err != nil {
		return fmt.Errorf("setup indexers: %w", err)
	}

	onGenerationChanged := builder.WithPredicates(predicate.GenerationChangedPredicate{})

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WakeupRequest{}, onGenerationChanged).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.MaxConcurrentReconciles}).
		Complete(r)
}

// setupIndexers registers a field indexer on CNPG Cluster objects to ensure
// that Clusters owned by a ClusterPool can be efficiently looked up by pool
// name. Without this, claiming a Cluster from a pool would require listing all
// clusters in the namespace and filtering in memory
func setupIndexers(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &apiv1.Cluster{}, PoolClusterOwnerKey,
		func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != poolv1alpha1.GroupVersion.String() || owner.Kind != poolv1alpha1.ClusterPoolKind {
				return nil
			}
			return []string{owner.Name}
		},
	)
}

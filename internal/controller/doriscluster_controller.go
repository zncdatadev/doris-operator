/*
Copyright 2024 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"

	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	"github.com/zncdatadev/doris-operator/internal/controller/scale"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	opgpconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// DorisClusterReconciler reconciles a DorisCluster object
type DorisClusterReconciler struct {
	ctrlclient.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=secrets.kubedoop.dev,resources=secretclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=authentication.kubedoop.dev,resources=authenticationclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

var logger = ctrl.Log.WithName("doriscluster-controller")

// Reconcile is part of the main kubernetes reconciliation loop
func (r *DorisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.V(1).Info("Reconciling DorisCluster")

	instance := &dorisv1alpha1.DorisCluster{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if ctrlclient.IgnoreNotFound(err) == nil {
			logger.V(1).Info("DorisCluster not found, may have been deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	logger.V(1).Info("DorisCluster found", "namespace", instance.Namespace, "name", instance.Name)

	// Phase 0: Gate BE replicas if there are in-progress decommissions.
	// By modifying the spec replicas in-memory before Phase 1, operator-go's STS
	// reconciler will see the gated value and won't scale down prematurely.
	gateApplied, restoreFn := r.gateBESpecReplicas(ctx, instance)
	if gateApplied {
		defer restoreFn()
	}

	resourceClient := &client.Client{
		Client:         r.Client,
		OwnerReference: instance,
	}

	gvk := instance.GetObjectKind().GroupVersionKind()

	clusterReconciler := NewClusterReconciler(
		resourceClient,
		reconciler.ClusterInfo{
			GVK: &metav1.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    gvk.Kind,
			},
			ClusterName: instance.Name,
		},
		&instance.Spec,
	)

	if err := clusterReconciler.RegisterResources(ctx); err != nil {
		return ctrl.Result{}, err
	}

	if result, err := clusterReconciler.Reconcile(ctx); err != nil {
		return ctrl.Result{}, err
	} else if !result.IsZero() {
		return result, nil
	}

	logger.Info("Cluster resource reconciled, checking if ready.", "cluster", instance.Name, "namespace", instance.Namespace)

	if result, err := clusterReconciler.Ready(ctx); err != nil {
		return ctrl.Result{}, err
	} else if !result.IsZero() {
		return result, nil
	}

	// Phase 2: Scale management (after resources are ready)
	scaleResult, authInitialized, err := r.reconcileScale(ctx, instance)
	if err != nil {
		logger.Error(err, "Scale reconciliation failed", "cluster", instance.Name)
		return ctrl.Result{}, nil
	}

	// Update CR status with node information (single status patch)
	if err := r.updateStatus(ctx, instance, scaleResult, authInitialized); err != nil {
		logger.Error(err, "Failed to update cluster status", "cluster", instance.Name)
		return ctrl.Result{}, err
	}

	if scaleResult != nil && scaleResult.NeedRequeue {
		logger.Info("Scale operation in progress, requeuing", "cluster", instance.Name, "after", scaleResult.RequeueAfter)
		return ctrl.Result{RequeueAfter: scaleResult.RequeueAfter}, nil
	}

	logger.V(1).Info("Reconcile finished.", "cluster", instance.Name, "namespace", instance.Namespace)

	return ctrl.Result{}, nil
}

// clusterScaleDownPolicy implements scale.ScaleDownPolicy using the CR spec.
type clusterScaleDownPolicy struct {
	spec *dorisv1alpha1.DorisClusterSpec
}

func (p *clusterScaleDownPolicy) GetDecommissionTimeout() time.Duration {
	return scale.GetDecommissionTimeout(p.spec)
}

// decommissionTracker implements scale.DecommissionTracker by managing annotations
// on the DorisCluster CR to track BE decommission lifecycle.
type decommissionTracker struct {
	instance *dorisv1alpha1.DorisCluster
	client   ctrlclient.Client
	// pending stores annotation mutations to be persisted: key → value (empty = delete)
	pending map[string]string
	dirty   bool
}

func newDecommissionTracker(
	instance *dorisv1alpha1.DorisCluster,
	client ctrlclient.Client,
) *decommissionTracker {
	return &decommissionTracker{
		instance: instance,
		client:   client,
		pending:  make(map[string]string),
	}
}

// decommissionAnnoKey returns the annotation key for a pod's decommission start time.
func decommissionAnnoKey(podName string) string {
	return fmt.Sprintf("%s/%s", scale.AnnotationDecommissionStart, podName)
}

// GetStart returns the decommission start time for a pod.
func (t *decommissionTracker) GetStart(podName string) (string, bool) {
	key := decommissionAnnoKey(podName)
	// Check pending writes first
	if val, ok := t.pending[key]; ok {
		return val, val != ""
	}
	if t.instance.Annotations == nil {
		return "", false
	}
	val, ok := t.instance.Annotations[key]
	return val, ok
}

// RecordStart records the decommission start time for a pod.
func (t *decommissionTracker) RecordStart(podName string, timestamp string) {
	t.pending[decommissionAnnoKey(podName)] = timestamp
	t.dirty = true
}

// ClearStart removes the decommission start time for a pod.
func (t *decommissionTracker) ClearStart(podName string) {
	key := decommissionAnnoKey(podName)
	t.pending[key] = ""
	t.dirty = true
}

// Persist writes all pending annotation changes to the CR.
// Empty values in pending map are treated as deletions.
func (t *decommissionTracker) Persist(ctx context.Context) error {
	if !t.dirty {
		return nil
	}

	latest := &dorisv1alpha1.DorisCluster{}
	if err := t.client.Get(ctx, types.NamespacedName{
		Name:      t.instance.Name,
		Namespace: t.instance.Namespace,
	}, latest); err != nil {
		return fmt.Errorf("failed to fetch latest CR for annotation update: %w", err)
	}

	patch := ctrlclient.MergeFrom(latest.DeepCopy())
	if latest.Annotations == nil {
		latest.Annotations = make(map[string]string)
	}

	var persisted, deleted int
	for k, v := range t.pending {
		if v == "" {
			delete(latest.Annotations, k)
			deleted++
		} else {
			latest.Annotations[k] = v
			persisted++
		}
	}

	if err := t.client.Patch(ctx, latest, patch); err != nil {
		return fmt.Errorf("failed to persist decommission annotations: %w", err)
	}

	t.instance = latest
	t.dirty = false
	t.pending = make(map[string]string)
	logger.Info("Persisted decommission annotations", "recorded", persisted, "cleared", deleted)
	return nil
}

// PendingPods returns pod names that have active (non-cleared) decommission tracking.
// These pods are in the middle of decommission and their STS replicas should be gated.
// Results are sorted for deterministic behavior.
func (t *decommissionTracker) PendingPods() []string {
	if t.instance.Annotations == nil {
		return nil
	}

	prefix := scale.AnnotationDecommissionStart + "/"
	var pods []string
	for key := range t.instance.Annotations {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			podName := key[len(prefix):]
			// Check if this pod was cleared in pending
			if val, ok := t.pending[decommissionAnnoKey(podName)]; ok && val == "" {
				continue
			}
			pods = append(pods, podName)
		}
	}
	sort.Strings(pods)
	return pods
}

// reconcileScale performs scale reconciliation by connecting to Doris FE
// and checking if any scale-down operations are needed.
// It returns the scale result and whether auth bootstrap was performed.
func (r *DorisClusterReconciler) reconcileScale(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
) (*scale.ScaleResult, bool, error) {
	// Build FE service DNS name for MySQL connection
	clusterDomain := "cluster.local"
	if instance.Spec.ClusterConfig != nil && instance.Spec.ClusterConfig.ClusterDomain != "" {
		clusterDomain = instance.Spec.ClusterConfig.ClusterDomain
	}
	feHost := fmt.Sprintf("%s-fe-internal.%s.svc.%s", instance.Name, instance.Namespace, clusterDomain)

	// Resolve management credentials
	var mgmtUser, mgmtPass string
	needBootstrap := instance.Spec.AuthSecret != nil && !instance.Status.AuthInitialized

	if instance.Spec.AuthSecret != nil {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      instance.Spec.AuthSecret.SecretName,
			Namespace: instance.Namespace,
		}, secret); err != nil {
			if ctrlclient.IgnoreNotFound(err) == nil {
				logger.Info("AuthSecret not found yet, skipping scale reconciliation",
					"secret", instance.Spec.AuthSecret.SecretName)
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("failed to get authSecret: %w", err)
		}
		mgmtUser, mgmtPass = doris_client.GetClusterAuthCredentials(secret.Data)
	} else {
		mgmtUser = doris_client.DefaultAdminUser
		mgmtPass = ""
	}

	// Bootstrap the admin user with root credentials if needed.
	if needBootstrap {
		rootClient, err := doris_client.NewDorisClient(feHost, constants.FEQueryPort, doris_client.DefaultAdminUser, "")
		if err != nil {
			logger.Info("Failed to connect to Doris FE for auth bootstrap",
				"host", feHost, "error", err)
			return nil, false, nil
		}

		exists, err := rootClient.CheckUserExists(ctx, mgmtUser)
		if err != nil {
			_ = rootClient.Close()
			logger.Error(err, "Failed to check if admin user exists", "user", mgmtUser)
			return nil, false, nil
		}
		if !exists {
			if err := rootClient.InitializeAdminUser(ctx, mgmtUser, mgmtPass); err != nil {
				_ = rootClient.Close()
				logger.Error(err, "Failed to initialize admin user", "user", mgmtUser)
				return nil, false, nil
			}
		}
		_ = rootClient.Close()
	}

	// Connect with management credentials for scale operations
	mgmtClient, err := doris_client.NewDorisClient(feHost, constants.FEQueryPort, mgmtUser, mgmtPass)
	if err != nil {
		logger.Info("Failed to connect to Doris FE with management credentials",
			"host", feHost, "user", mgmtUser, "error", err)
		return nil, false, nil
	}
	defer func() { _ = mgmtClient.Close() }()

	scaleMgr := scale.NewScaleManager(mgmtClient)
	defer scaleMgr.Close()

	// Fetch current StatefulSets
	replicaStates, err := r.fetchReplicaStates(ctx, instance)
	if err != nil {
		return nil, false, fmt.Errorf("failed to fetch replica states: %w", err)
	}

	// Create policy and tracker for decommission lifecycle management
	policy := &clusterScaleDownPolicy{spec: &instance.Spec}
	tracker := newDecommissionTracker(instance, r.Client)

	result, err := scaleMgr.ReconcileScale(ctx, &instance.Spec, replicaStates, policy, tracker)
	if err != nil {
		return nil, false, err
	}

	// Persist decommission annotation changes (records + clears)
	if err := tracker.Persist(ctx); err != nil {
		logger.Error(err, "Failed to persist decommission annotations", "cluster", instance.Name)
		// Non-fatal: decommission timeout tracking will be approximate until next persistence
	}

	return result, needBootstrap, nil
}

// gateBESpecReplicas checks for in-progress BE decommissions (via CR annotations)
// and gates the BE spec replicas to the current STS replica count. This prevents
// operator-go's STS reconciler from scaling down pods during active decommission.
//
// It returns whether a gate was applied and a restore function that must be called
// (typically via defer) to restore the original spec values.
//
// This is the "pre-reconcile interception" approach: by modifying the spec in-memory
// before Phase 1, the STS builder sees the gated replicas and won't create an update
// that would trigger premature pod deletion.
func (r *DorisClusterReconciler) gateBESpecReplicas(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
) (bool, func()) {
	if instance.Spec.Backend == nil {
		return false, func() {}
	}

	// Gate if there are in-progress decommissions (from annotations)
	// OR if STS replicas exceed the spec desired count (first-reconcile case
	// where annotations haven't been written yet).
	tracker := newDecommissionTracker(instance, r.Client)
	hasPending := len(tracker.PendingPods()) > 0

	// Fetch current BE StatefulSets to detect scale-down intent
	stsList := &appsv1.StatefulSetList{}
	labelSelector := ctrlclient.MatchingLabels{
		opgpconstants.LabelKubernetesInstance:  instance.Name,
		opgpconstants.LabelKubernetesComponent: string(constants.ComponentTypeBE),
	}
	if err := r.List(ctx, stsList, labelSelector, ctrlclient.InNamespace(instance.Namespace)); err != nil {
		logger.Error(err, "Failed to list BE StatefulSets for gating", "cluster", instance.Name)
		return false, func() {}
	}

	if len(stsList.Items) == 0 {
		return false, func() {}
	}

	// Sum up current running replicas across all BE STSs
	var currentReplicas int32
	for _, sts := range stsList.Items {
		currentReplicas += sts.Status.Replicas
	}

	// Check if gating is actually needed (current > desired)
	desired := scale.GetEffectiveReplicas(instance.Spec.Backend)
	if currentReplicas <= desired {
		return false, func() {}
	}

	if !hasPending {
		// First reconcile after scale-down intent detected but no annotations yet.
		// This is the critical window: Phase 1 would scale down before Phase 2 records
		// decommission-start annotations. Gate now to prevent premature deletion.
		logger.Info("Gating BE spec replicas on first scale-down detection",
			"cluster", instance.Name,
			"desired", desired,
			"current", currentReplicas)
	} else {
		logger.Info("Gating BE spec replicas for in-progress decommission",
			"cluster", instance.Name,
			"desired", desired,
			"current", currentReplicas,
			"pendingPods", tracker.PendingPods())
	}

	// For single roleGroup: override replicas directly
	if len(instance.Spec.Backend.RoleGroups) == 1 {
		for name := range instance.Spec.Backend.RoleGroups {
			rg := instance.Spec.Backend.RoleGroups[name]
			original := rg.Replicas

			// Allocate a stable variable for the gated value (not a loop-local)
			gated := currentReplicas
			rg.Replicas = &gated
			instance.Spec.Backend.RoleGroups[name] = rg

			return true, func() {
				logger.V(1).Info("Restoring BE spec replicas after gate",
					"cluster", instance.Name, "roleGroup", name,
					"restored", original, "gated", gated)
				rg := instance.Spec.Backend.RoleGroups[name]
				rg.Replicas = original
				instance.Spec.Backend.RoleGroups[name] = rg
			}
		}
	}

	// Multi-roleGroup: gate proportionally by distributing the current total
	// across groups weighted by their desired replicas.
	totalDesired := desired
	if totalDesired == 0 {
		return false, func() {}
	}

	originals := make(map[string]int32)
	remaining := currentReplicas
	groupNames := make([]string, 0, len(instance.Spec.Backend.RoleGroups))
	for name := range instance.Spec.Backend.RoleGroups {
		groupNames = append(groupNames, name)
	}

	for i, name := range groupNames {
		rg := instance.Spec.Backend.RoleGroups[name]
		original := int32(0)
		if rg.Replicas != nil {
			original = *rg.Replicas
		}
		originals[name] = original

		groupDesired := original

		var gated int32
		if i == len(groupNames)-1 {
			gated = remaining
		} else {
			gated = currentReplicas * groupDesired / totalDesired
			if gated < 1 {
				gated = 1
			}
		}
		remaining -= gated

		// Allocate stable per-group variable for gated value
		gatedCopy := gated
		rg.Replicas = &gatedCopy
		instance.Spec.Backend.RoleGroups[name] = rg
	}

	return true, func() {
		for name, original := range originals {
			rg := instance.Spec.Backend.RoleGroups[name]
			restoreCopy := original
			rg.Replicas = &restoreCopy
			instance.Spec.Backend.RoleGroups[name] = rg
		}
	}
}

// fetchReplicaStates builds replica states for all cluster components
// by listing StatefulSets via label selector (supports multiple roleGroups).
func (r *DorisClusterReconciler) fetchReplicaStates(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
) (map[constants.ComponentType]*scale.ReplicaState, error) {
	states := make(map[constants.ComponentType]*scale.ReplicaState)

	for _, ct := range []constants.ComponentType{constants.ComponentTypeFE, constants.ComponentTypeBE, constants.ComponentTypeBroker} {
		stsList := &appsv1.StatefulSetList{}
		labelSelector := ctrlclient.MatchingLabels{
			opgpconstants.LabelKubernetesInstance:  instance.Name,
			opgpconstants.LabelKubernetesComponent: string(ct),
		}
		if err := r.List(ctx, stsList, labelSelector, ctrlclient.InNamespace(instance.Namespace)); err != nil {
			return nil, fmt.Errorf("failed to list StatefulSets for %s: %w", ct, err)
		}

		if len(stsList.Items) == 0 {
			continue
		}

		rs := &scale.ReplicaState{Component: ct}
		for _, sts := range stsList.Items {
			rs.SpecReplicas += scale.GetStatefulSetReplicas(&sts)
			rs.CurrentReplicas += sts.Status.Replicas
			rs.ReadyReplicas += sts.Status.ReadyReplicas
			rs.PodNames = append(rs.PodNames, scale.GetStatefulSetPodNames(&sts)...)
			rs.StatefulSetNames = append(rs.StatefulSetNames, sts.Name)
		}
		states[ct] = rs
	}

	return states, nil
}

// updateStatus patches the DorisCluster status in a single patch operation.
// When scaleResult is nil (e.g., BE not yet alive), it falls back to pod-list-based status.
func (r *DorisClusterReconciler) updateStatus(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
	result *scale.ScaleResult,
	authBootstrap bool,
) error {
	latest := &dorisv1alpha1.DorisCluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latest); err != nil {
		return err
	}

	patch := ctrlclient.MergeFrom(latest.DeepCopy())

	// Mark auth initialization as complete
	if authBootstrap {
		latest.Status.AuthInitialized = true
	}

	// buildPodNodeList creates a sorted list of NodeStatus from pod listings.
	buildPodNodeList := func(ct constants.ComponentType) ([]dorisv1alpha1.NodeStatus, error) {
		podList := &corev1.PodList{}
		labelSelector := ctrlclient.MatchingLabels{
			opgpconstants.LabelKubernetesInstance:  instance.Name,
			opgpconstants.LabelKubernetesComponent: string(ct),
		}
		if err := r.List(ctx, podList, labelSelector, ctrlclient.InNamespace(instance.Namespace)); err != nil {
			return nil, err
		}

		nodes := make([]dorisv1alpha1.NodeStatus, 0, len(podList.Items))
		for _, pod := range podList.Items {
			nodes = append(nodes, dorisv1alpha1.NodeStatus{Name: pod.Name})
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
		return nodes, nil
	}

	// Update node status from SQL queries if available, falling back to pod listings for missing components
	if result != nil {
		scale.UpdateClusterStatus(&latest.Status, result.BEStatuses, result.FEStatuses, result.BrokerStatuses)

		if result.BEStatuses == nil {
			if nodes, err := buildPodNodeList(constants.ComponentTypeBE); err != nil {
				return err
			} else {
				latest.Status.BackendNodes = nodes
			}
		}
		if result.FEStatuses == nil {
			if nodes, err := buildPodNodeList(constants.ComponentTypeFE); err != nil {
				return err
			} else {
				latest.Status.FrontendNodes = nodes
			}
		}
		if result.BrokerStatuses == nil {
			if nodes, err := buildPodNodeList(constants.ComponentTypeBroker); err != nil {
				return err
			} else {
				latest.Status.BrokerNodes = nodes
			}
		}
	} else {
		for _, ct := range []constants.ComponentType{constants.ComponentTypeFE, constants.ComponentTypeBE, constants.ComponentTypeBroker} {
			nodes, err := buildPodNodeList(ct)
			if err != nil {
				return err
			}
			switch ct {
			case constants.ComponentTypeFE:
				latest.Status.FrontendNodes = nodes
			case constants.ComponentTypeBE:
				latest.Status.BackendNodes = nodes
			case constants.ComponentTypeBroker:
				latest.Status.BrokerNodes = nodes
			}
		}
	}

	return r.Status().Patch(ctx, latest, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DorisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dorisv1alpha1.DorisCluster{}).
		Complete(r)
}

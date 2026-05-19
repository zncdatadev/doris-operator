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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
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

	// Phase 2b: Gate STS replicas — prevent premature scale-down during active decommission.
	// operator-go's STS reconciler may have already set replicas to the desired (lower) value.
	// If scale manager detects in-progress decommission, we patch those STS replicas back
	// to the current count until decommission completes.
	if scaleResult != nil && len(scaleResult.GatedReplicas) > 0 {
		if err := r.gateSTSReplicas(ctx, instance, scaleResult.GatedReplicas); err != nil {
			logger.Error(err, "Failed to gate STS replicas", "cluster", instance.Name)
			return ctrl.Result{}, err
		}
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

// clusterScaleConfig implements scale.ScaleConfig by managing annotations on the DorisCluster CR.
// It tracks BE decommission start times for timeout enforcement.
type clusterScaleConfig struct {
	instance *dorisv1alpha1.DorisCluster
	client   ctrlclient.Client
	spec     *dorisv1alpha1.DorisClusterSpec
	// pendingAnnotations stores annotation updates to be persisted
	pendingAnnotations map[string]string
	dirty              bool
}

func newClusterScaleConfig(
	instance *dorisv1alpha1.DorisCluster,
	client ctrlclient.Client,
) *clusterScaleConfig {
	return &clusterScaleConfig{
		instance:           instance,
		client:             client,
		spec:               &instance.Spec,
		pendingAnnotations: make(map[string]string),
	}
}

// GetDecommissionTimeout returns the configured decommission timeout duration.
func (c *clusterScaleConfig) GetDecommissionTimeout() time.Duration {
	return scale.GetDecommissionTimeout(c.spec)
}

// decommissionAnnoKey returns the annotation key for a pod's decommission start time.
func decommissionAnnoKey(podName string) string {
	return fmt.Sprintf("%s/%s", scale.AnnotationDecommissionStart, podName)
}

// GetDecommissionStartAnnotation returns the recorded decommission start time for a pod.
func (c *clusterScaleConfig) GetDecommissionStartAnnotation(podName string) (string, bool) {
	key := decommissionAnnoKey(podName)
	if val, ok := c.pendingAnnotations[key]; ok {
		return val, ok
	}
	val, ok := c.instance.Annotations[key]
	return val, ok
}

// SetDecommissionStartAnnotation records the decommission start time for a pod.
func (c *clusterScaleConfig) SetDecommissionStartAnnotation(podName string, timestamp string) {
	key := decommissionAnnoKey(podName)
	c.pendingAnnotations[key] = timestamp
	c.dirty = true
}

// PersistAnnotations writes any pending annotation changes to the CR.
func (c *clusterScaleConfig) PersistAnnotations() error {
	if !c.dirty {
		return nil
	}

	latest := &dorisv1alpha1.DorisCluster{}
	if err := c.client.Get(context.Background(), types.NamespacedName{
		Name:      c.instance.Name,
		Namespace: c.instance.Namespace,
	}, latest); err != nil {
		return fmt.Errorf("failed to fetch latest CR for annotation update: %w", err)
	}

	patch := ctrlclient.MergeFrom(latest.DeepCopy())
	if latest.Annotations == nil {
		latest.Annotations = make(map[string]string)
	}
	for k, v := range c.pendingAnnotations {
		latest.Annotations[k] = v
	}

	if err := c.client.Patch(context.Background(), latest, patch); err != nil {
		return fmt.Errorf("failed to persist decommission annotations: %w", err)
	}

	// Update local reference so subsequent reads in this reconcile are consistent
	c.instance = latest
	c.dirty = false
	logger.Info("Persisted decommission start annotations", "count", len(c.pendingAnnotations))
	return nil
}

// reconcileScale performs scale reconciliation by connecting to Doris FE
// and checking if any scale-down operations are needed.
// It returns the scale result and an optional authInitialized flag.
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
	// This is only done once when authSecret is first configured.
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

	// Create scale config for decommission timeout tracking
	scaleConfig := newClusterScaleConfig(instance, r.Client)

	result, err := scaleMgr.ReconcileScale(ctx, &instance.Spec, replicaStates, scaleConfig)
	if err != nil {
		return nil, false, err
	}

	// Persist decommission start time annotations if any were set
	if err := scaleConfig.PersistAnnotations(); err != nil {
		logger.Error(err, "Failed to persist decommission annotations", "cluster", instance.Name)
		// Non-fatal: decommission timeout tracking will be approximate
	}

	return result, needBootstrap, nil
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
			"app.kubernetes.io/instance":  instance.Name,
			"app.kubernetes.io/component": string(ct),
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
			rs.StSNames = append(rs.StSNames, sts.Name)
		}
		states[ct] = rs
	}

	return states, nil
}

// gateSTSReplicas patches StatefulSets to hold their replica count at the gated value,
// preventing operator-go's STS reconciler from scaling down pods while decommission is active.
// This is the "post-reconcile correction" that implements the scale-down safety gate.
func (r *DorisClusterReconciler) gateSTSReplicas(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
	gatedReplicas map[string]int32,
) error {
	for stsName, minReplicas := range gatedReplicas {
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      stsName,
			Namespace: instance.Namespace,
		}, sts); err != nil {
			if ctrlclient.IgnoreNotFound(err) == nil {
				logger.V(1).Info("Gated STS not found, skipping", "sts", stsName)
				continue
			}
			return fmt.Errorf("failed to get gated STS %s: %w", stsName, err)
		}

		currentReplicas := int32(1)
		if sts.Spec.Replicas != nil {
			currentReplicas = *sts.Spec.Replicas
		}

		if currentReplicas < minReplicas {
			logger.Info("Gating STS replicas to prevent premature scale-down",
				"sts", stsName,
				"current", currentReplicas,
				"gated", minReplicas)
			patch := ctrlclient.MergeFrom(sts.DeepCopy())
			sts.Spec.Replicas = ptr.To(minReplicas)
			if err := r.Patch(ctx, sts, patch); err != nil {
				return fmt.Errorf("failed to gate STS %s replicas to %d: %w", stsName, minReplicas, err)
			}
		}
	}
	return nil
}

// updateStatus patches the DorisCluster status in a single patch operation.
// When scaleResult is nil (e.g., BE not yet alive), it falls back to pod-list-based status.
// When authBootstrap is true, it also sets AuthInitialized = true.
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
			"app.kubernetes.io/instance":  instance.Name,
			"app.kubernetes.io/component": string(ct),
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

		// Fallback to pod listing for components where SQL query failed (nil statuses)
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
		// Full fallback: build status from pod listings for all components
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

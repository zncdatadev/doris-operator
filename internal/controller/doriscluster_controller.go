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

// defaultDorisUser is the default MySQL user for Doris FE
const defaultDorisUser = "root"

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
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
	scaleResult, err := r.reconcileScale(ctx, instance)
	if err != nil {
		logger.Error(err, "Scale reconciliation failed", "cluster", instance.Name)
		// Don't block resource reconciliation on scale errors; log and skip
		_ = r.updateStatus(ctx, instance, scaleResult)
		return ctrl.Result{}, nil
	}

	// Update CR status with node information
	if err := r.updateStatus(ctx, instance, scaleResult); err != nil {
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

// reconcileScale performs scale reconciliation by connecting to Doris FE
// and checking if any scale-down operations are needed.
func (r *DorisClusterReconciler) reconcileScale(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
) (*scale.ScaleResult, error) {
	// Build FE service DNS name for MySQL connection
	clusterDomain := "cluster.local"
	if instance.Spec.ClusterConfig != nil && instance.Spec.ClusterConfig.ClusterDomain != "" {
		clusterDomain = instance.Spec.ClusterConfig.ClusterDomain
	}
	feHost := fmt.Sprintf("%s-fe-internal.%s.svc.%s", instance.Name, instance.Namespace, clusterDomain)

	// Step 1: Connect to Doris FE with root credentials to initialize admin user if needed
	rootClient, err := doris_client.NewDorisClient(feHost, constants.FEQueryPort, defaultDorisUser, "")
	if err != nil {
		logger.Info("Failed to connect to Doris FE for scale management",
			"host", feHost, "error", err)
		// FE not ready yet; this is not an error, just skip scale reconciliation
		return nil, nil
	}
	defer func() { _ = rootClient.Close() }()

	// Step 2: Read auth credentials from Secret (if configured)
	var mgmtUser, mgmtPass string
	if instance.Spec.AuthSecret != nil {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      instance.Spec.AuthSecret.SecretName,
			Namespace: instance.Namespace,
		}, secret); err != nil {
			if ctrlclient.IgnoreNotFound(err) == nil {
				logger.Info("AuthSecret not found yet, skipping scale reconciliation",
					"secret", instance.Spec.AuthSecret.SecretName)
				return nil, nil
			}
			return nil, fmt.Errorf("failed to get authSecret: %w", err)
		}
		mgmtUser, mgmtPass = doris_client.GetClusterAuthCredentials(secret.Data)
	} else {
		// No authSecret configured, use root with empty password
		mgmtUser = defaultDorisUser
		mgmtPass = ""
	}

	// Step 3: Bootstrap the admin user if not yet initialized
	if instance.Spec.AuthSecret != nil && !instance.Status.AuthInitialized {
		exists, err := rootClient.CheckUserExists(ctx, mgmtUser)
		if err != nil {
			logger.Error(err, "Failed to check if admin user exists", "user", mgmtUser)
			return nil, nil
		}
		if !exists {
			if err := rootClient.InitializeAdminUser(ctx, mgmtUser, mgmtPass); err != nil {
				logger.Error(err, "Failed to initialize admin user", "user", mgmtUser)
				return nil, nil
			}
		}
		// Mark auth initialization as complete
		latest := &dorisv1alpha1.DorisCluster{}
		if err := r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latest); err == nil {
			patch := ctrlclient.MergeFrom(latest.DeepCopy())
			latest.Status.AuthInitialized = true
			if err := r.Status().Patch(ctx, latest, patch); err != nil {
				logger.Error(err, "Failed to update authInitialized status")
			}
		}
	}

	mgmtClient, err := doris_client.NewDorisClient(feHost, constants.FEQueryPort, mgmtUser, mgmtPass)
	if err != nil {
		logger.Info("Failed to connect to Doris FE with management credentials",
			"host", feHost, "user", mgmtUser, "error", err)
		// Credentials may have been changed by user; skip scale reconciliation
		return nil, nil
	}
	defer func() { _ = mgmtClient.Close() }()

	scaleMgr := scale.NewScaleManager(mgmtClient)
	defer scaleMgr.Close()

	// Fetch current StatefulSets
	replicaStates, err := r.fetchReplicaStates(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch replica states: %w", err)
	}

	return scaleMgr.ReconcileScale(ctx, &instance.Spec, replicaStates)
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
			rs.PodNames = append(rs.PodNames, scale.GetStatefulSetPodNames(&sts, sts.Namespace)...)
		}
		states[ct] = rs
	}

	return states, nil
}

// updateStatus patches the DorisCluster status with node information
func (r *DorisClusterReconciler) updateStatus(
	ctx context.Context,
	instance *dorisv1alpha1.DorisCluster,
	result *scale.ScaleResult,
) error {
	if result == nil {
		return nil
	}

	// Fetch latest instance to avoid conflicts
	latest := &dorisv1alpha1.DorisCluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latest); err != nil {
		return err
	}

	patch := ctrlclient.MergeFrom(latest.DeepCopy())
	scale.UpdateClusterStatus(&latest.Status, result.BEStatuses, result.FEStatuses, result.BrokerStatuses)

	return r.Status().Patch(ctx, latest, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DorisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dorisv1alpha1.DorisCluster{}).
		Complete(r)
}

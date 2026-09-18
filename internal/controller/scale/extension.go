/*
Copyright 2026 zncdatadev.

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

package scale

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	opcommon "github.com/zncdatadev/operator-go/pkg/common"
	opconstant "github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	clusterExtensionName      = "doris-scale-status"
	scaleDownRequeue          = 30 * time.Second
	legacyManagedByValue      = "doris.kubedoop.dev"
	decommissionIntentPending = "pending"
	frontendDropPhasePending  = "pending"
	frontendDropPhaseRemoved  = "removed-awaiting-statefulset"
)

var _ opcommon.ClusterExtension[*dorisv1alpha1.DorisCluster] = (*ScaleExtension)(nil)

// ScaleExtension keeps product-specific scale and node-status handling outside
// the generic resource reconciliation pipeline.
//
// It is intentionally stateless: one extension instance can safely be shared by
// reconciliations for different DorisCluster objects.
type ScaleExtension struct{}

// NewScaleExtension returns the Doris scale/status cluster extension.
func NewScaleExtension() *ScaleExtension {
	return &ScaleExtension{}
}

// Name implements common.Extension.
func (e *ScaleExtension) Name() string {
	return clusterExtensionName
}

// PreReconcile blocks the generic reconciler before it lowers an FE or BE
// StatefulSet replica count. Doris nodes are removed from the product topology
// first; an incomplete decommission is represented as a stable framework wait.
func (e *ScaleExtension) PreReconcile(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	if !cluster.DeletionTimestamp.IsZero() {
		return nil
	}

	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		return err
	}
	if err := tracker.ClearConvergedFrontendDrops(ctx); err != nil {
		return fmt.Errorf("clear converged Doris frontend drop intents: %w", err)
	}
	tracker.ClearEvidenceForRunningGroups(cluster)

	actions, err := collectScaleDownActions(ctx, k8sClient, cluster)
	if err != nil {
		if persistErr := tracker.Persist(ctx); persistErr != nil {
			return fmt.Errorf("collect Doris scale-down actions: %w (also failed to persist scale evidence: %v)", err, persistErr)
		}
		return err
	}
	if err := validatePendingBEDecommissions(actions, tracker.PendingPods()); err != nil {
		if persistErr := tracker.Persist(ctx); persistErr != nil {
			return fmt.Errorf("validate pending Doris decommission: %w (also failed to persist scale evidence: %v)", err, persistErr)
		}
		return err
	}
	if err := validatePendingFEDrops(actions, tracker.PendingFrontendDropPods()); err != nil {
		if persistErr := tracker.Persist(ctx); persistErr != nil {
			return fmt.Errorf("validate pending Doris frontend drop: %w (also failed to persist scale evidence: %v)", err, persistErr)
		}
		return err
	}
	if len(actions) == 0 {
		if err := tracker.Persist(ctx); err != nil {
			return fmt.Errorf("persist Doris scale evidence: %w", err)
		}
		return nil
	}
	// Preserve the existing BE sequence: persist its intent before connecting to
	// Doris. FE authorization happens later, after all non-destructive FE role and
	// strategy checks have passed, so a connection or preflight failure cannot
	// create a false frontend latch.
	recordBackendScaleDownIntents(actions, tracker)
	if err := tracker.Persist(ctx); err != nil {
		return fmt.Errorf("persist Doris backend scale-down intent: %w", err)
	}

	dorisClient, authInitialized, err := connectDoris(ctx, k8sClient, cluster)
	if err != nil {
		return err
	}
	defer func() {
		_ = dorisClient.Close()
	}()

	if authInitialized {
		cluster.Status.AuthInitialized = true
	}

	policy := &clusterScaleDownPolicy{spec: &cluster.Spec}
	beManager := NewBEScaleManager(dorisClient)
	feManager := NewFEScaleManager(dorisClient)
	frontendPlans, err := feManager.PrepareScaleDownActions(ctx, actions)
	if err != nil {
		return err
	}

	waiting := false
	var operationErr error
	for i, action := range actions {
		var ready []string
		switch action.Component {
		case constants.ComponentTypeBE:
			ready, operationErr = beManager.ScaleDown(ctx, action, policy, tracker)
		case constants.ComponentTypeFE:
			if operationErr = feManager.AuthorizeScaleDown(ctx, frontendPlans[i], tracker); operationErr == nil {
				ready, operationErr = feManager.ExecuteScaleDown(ctx, frontendPlans[i], tracker)
			}
		}
		if operationErr != nil {
			break
		}
		if action.Component == constants.ComponentTypeBE {
			for _, podName := range ready {
				// BEScaleManager clears decommission tracking itself, except on
				// the force-drop path. Clearing again is deliberately idempotent.
				tracker.ClearStart(podName)
			}
		}
		if len(ready) == len(action.PodsToRemove) && action.DesiredReplicas == 0 {
			tracker.MarkSafeZero(action.Component, action.RoleGroup)
		} else if len(ready) != len(action.PodsToRemove) {
			waiting = true
			log.FromContext(ctx).Info(
				"Doris scale-down is still in progress",
				"component", action.Component,
				"statefulSet", action.StatefulSetNames[0],
				"readyForRemoval", len(ready),
				"requestedRemoval", len(action.PodsToRemove),
			)
		}
	}

	// Persist even when a later action failed: a successfully started BE
	// decommission must retain its timeout origin across reconciliations.
	if err := tracker.Persist(ctx); err != nil {
		return fmt.Errorf("persist Doris scale state: %w", err)
	}
	if operationErr != nil {
		return operationErr
	}
	if waiting {
		return opcommon.NewRequeueAfterError(
			scaleDownRequeue,
			"WaitingForDorisScaleDown",
			"Doris nodes are still being removed safely",
		)
	}

	return nil
}

func recordBackendScaleDownIntents(
	actions []ScaleAction,
	tracker *annotationDecommissionTracker,
) {
	for _, action := range actions {
		if action.Component != constants.ComponentTypeBE {
			continue
		}
		for _, podName := range action.PodsToRemove {
			tracker.RecordIntent(podName)
		}
	}
}

// PostReconcile updates the Doris-specific status fields in memory. The generic
// reconciler owns the single status write after all extensions have completed.
func (e *ScaleExtension) PostReconcile(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	podNames := make(map[constants.ComponentType][]string, 3)
	for _, component := range []constants.ComponentType{
		constants.ComponentTypeFE,
		constants.ComponentTypeBE,
		constants.ComponentTypeBroker,
	} {
		names, err := listPodNames(ctx, k8sClient, cluster, component)
		if err != nil {
			return fmt.Errorf("list %s pods for status: %w", component, err)
		}
		podNames[component] = names
	}

	// Always refresh the topology, even while Doris SQL is unavailable. This
	// clears nodes for removed role groups and provides useful pod-level fallback.
	cluster.Status.FrontendNodes = fallbackNodeStatuses(podNames[constants.ComponentTypeFE])
	cluster.Status.BackendNodes = fallbackNodeStatuses(podNames[constants.ComponentTypeBE])
	cluster.Status.BrokerNodes = fallbackNodeStatuses(podNames[constants.ComponentTypeBroker])

	if len(podNames[constants.ComponentTypeFE]) == 0 {
		return nil
	}

	dorisClient, authInitialized, err := connectDoris(ctx, k8sClient, cluster)
	if err != nil {
		if opcommon.IsRequeueAfterError(err) {
			log.FromContext(ctx).V(1).Info(
				"Doris management endpoint is not ready; retaining pod-based status",
				"cluster", cluster.Name,
			)
			return nil
		}
		return err
	}
	defer func() {
		_ = dorisClient.Close()
	}()

	if authInitialized {
		cluster.Status.AuthInitialized = true
	}

	var beStatuses []BENodeStatus
	if len(podNames[constants.ComponentTypeBE]) > 0 {
		statuses, statusErr := NewBEScaleManager(dorisClient).GetBENodeStatuses(
			ctx,
			podNames[constants.ComponentTypeBE],
		)
		if statusErr != nil {
			log.FromContext(ctx).Error(statusErr, "Failed to query Doris backend status")
		} else {
			beStatuses = statuses
		}
	}

	var feStatuses []FENodeStatus
	if len(podNames[constants.ComponentTypeFE]) > 0 {
		statuses, statusErr := NewFEScaleManager(dorisClient).GetFENodeStatuses(
			ctx,
			podNames[constants.ComponentTypeFE],
		)
		if statusErr != nil {
			log.FromContext(ctx).Error(statusErr, "Failed to query Doris frontend status")
		} else {
			feStatuses = statuses
		}
	}

	var brokerStatuses []BrokerNodeStatus
	if len(podNames[constants.ComponentTypeBroker]) > 0 {
		brokers, statusErr := dorisClient.ShowBrokers(ctx)
		if statusErr != nil {
			log.FromContext(ctx).Error(statusErr, "Failed to query Doris broker status")
		} else {
			brokerStatuses = buildBrokerNodeStatuses(
				podNames[constants.ComponentTypeBroker],
				brokers,
			)
		}
	}

	// Nil status slices intentionally leave the pod-based fallback untouched.
	UpdateClusterStatus(
		&cluster.Status,
		beStatuses,
		feStatuses,
		brokerStatuses,
	)
	return nil
}

// OnReconcileError implements common.ClusterExtension. This extension has no
// cross-reconcile in-memory state to clean up.
func (e *ScaleExtension) OnReconcileError(
	_ context.Context,
	_ crclient.Client,
	_ *dorisv1alpha1.DorisCluster,
	_ error,
) error {
	return nil
}

type componentRole struct {
	component constants.ComponentType
	role      *dorisv1alpha1.RoleSpec
	strategy  string
}

// collectScaleDownActions compares each live StatefulSet with its own role
// group's desired replicas. The legacy aggregate comparison was ambiguous when
// one role group scaled down while another scaled up.
func collectScaleDownActions(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) ([]ScaleAction, error) {
	safeZeros, err := decodeSafeZeroRoleGroups(cluster.Annotations)
	if err != nil {
		return nil, err
	}
	components := []componentRole{
		{
			component: constants.ComponentTypeFE,
			role:      cluster.Spec.Frontend,
			strategy:  getFEStrategy(&cluster.Spec),
		},
		{
			component: constants.ComponentTypeBE,
			role:      cluster.Spec.Backend,
			strategy:  getBEStrategy(&cluster.Spec),
		},
	}

	var actions []ScaleAction
	for _, item := range components {
		statefulSets, err := listStatefulSets(ctx, k8sClient, cluster, item.component)
		if err != nil {
			return nil, fmt.Errorf("list %s StatefulSets before scale-down: %w", item.component, err)
		}
		for i := range statefulSets {
			statefulSet := &statefulSets[i]
			roleGroupName := statefulSet.Labels[opconstant.LabelKubernetesRoleGroup]
			if roleGroupName == "" {
				return nil, fmt.Errorf(
					"StatefulSet %s/%s is missing required label %q",
					statefulSet.Namespace,
					statefulSet.Name,
					opconstant.LabelKubernetesRoleGroup,
				)
			}

			current := GetStatefulSetReplicas(statefulSet)
			desired, roleGroupExists := desiredRoleGroupReplicas(item.role, roleGroupName)
			if clusterIsStopped(cluster) {
				if roleGroupExists {
					// Stopping is not a Doris topology change. The generic
					// reconciler may lower every StatefulSet to zero without
					// decommissioning its nodes.
					continue
				}
				// A stopped cluster deliberately keeps Doris topology entries while
				// forcing StatefulSets to zero. A zero live replica count therefore
				// cannot prove that product-level decommission already happened.
				return nil, fmt.Errorf(
					"cannot remove %s role group %q while the Doris cluster is stopped: resume the cluster, set replicas to 0, and wait for safe Doris scale-down before removing the role group",
					item.component,
					roleGroupName,
				)
			}
			if !roleGroupExists && current > 0 {
				// A cluster-level RequeueAfterError skips role reconciliation but
				// deliberately does not skip the Gen3 orphan cleaner. Refuse direct
				// removal while this StatefulSet is live so cleanup cannot bypass
				// Doris decommissioning.
				return nil, fmt.Errorf(
					"cannot remove live %s role group %q: set replicas to 0 and wait for safe Doris scale-down before removing the role group",
					item.component,
					roleGroupName,
				)
			}
			if !roleGroupExists {
				if !safeZeros[safeZeroKey(item.component, roleGroupName)] {
					return nil, fmt.Errorf(
						"cannot remove %s role group %q from a zero-replica StatefulSet without safe scale-down evidence: resume the cluster with the role group present, run it with at least one replica, then set replicas to 0 and wait for Doris topology cleanup before removing the role group",
						item.component,
						roleGroupName,
					)
				}
				continue
			}
			if current <= desired {
				continue
			}

			action := ScaleAction{
				Component:        item.component,
				RoleGroup:        roleGroupName,
				CurrentReplicas:  current,
				DesiredReplicas:  desired,
				Strategy:         item.strategy,
				StatefulSetNames: []string{statefulSet.Name},
			}
			action.PodsToRemove = getPodsToRemove(
				GetStatefulSetPodNames(statefulSet),
				current,
				desired,
			)
			actions = append(actions, action)
		}
	}
	if err := validateRemovedRoleGroupsHaveSafeZeroEvidence(cluster, safeZeros); err != nil {
		return nil, err
	}

	sort.Slice(actions, func(i, j int) bool {
		if actions[i].Component != actions[j].Component {
			return actions[i].Component < actions[j].Component
		}
		return actions[i].StatefulSetNames[0] < actions[j].StatefulSetNames[0]
	})
	return actions, nil
}

// validateRemovedRoleGroupsHaveSafeZeroEvidence protects the cleaner even when
// the StatefulSet has already disappeared. The preceding, high-priority legacy
// compatibility extension restores this ledger from any surviving role-group
// ConfigMap or Service, so the safety decision does not depend on one particular
// Kubernetes object still existing.
func validateRemovedRoleGroupsHaveSafeZeroEvidence(
	cluster *dorisv1alpha1.DorisCluster,
	safeZeros map[string]bool,
) error {
	roles := []struct {
		component constants.ComponentType
		role      *dorisv1alpha1.RoleSpec
	}{
		{component: constants.ComponentTypeFE, role: cluster.Spec.Frontend},
		{component: constants.ComponentTypeBE, role: cluster.Spec.Backend},
	}
	for _, item := range roles {
		groups := append([]string(nil), cluster.Status.RoleGroups[string(item.component)]...)
		sort.Strings(groups)
		for _, groupName := range groups {
			if item.role != nil {
				if _, exists := item.role.RoleGroups[groupName]; exists {
					continue
				}
			}
			if safeZeros[safeZeroKey(item.component, groupName)] {
				continue
			}
			return fmt.Errorf(
				"cannot remove %s role group %q without safe scale-down evidence: restore the role group, run it with at least one replica, then set replicas to 0 and wait for Doris topology cleanup before removing it",
				item.component,
				groupName,
			)
		}
	}
	return nil
}

// validatePendingBEDecommissions rejects raising a BE target while nodes from a
// previous, lower target are still being decommissioned. Doris exposes no
// reliable cancellation operation; allowing the StatefulSet to retain one of
// those pods would let Doris remove a node Kubernetes now intends to keep.
func validatePendingBEDecommissions(actions []ScaleAction, pendingPods []string) error {
	if len(pendingPods) == 0 {
		return nil
	}

	stillRequested := make(map[string]struct{})
	for _, action := range actions {
		if action.Component != constants.ComponentTypeBE {
			continue
		}
		for _, podName := range action.PodsToRemove {
			stillRequested[podName] = struct{}{}
		}
	}

	var retained []string
	for _, podName := range pendingPods {
		if _, requested := stillRequested[podName]; !requested {
			retained = append(retained, podName)
		}
	}
	if len(retained) == 0 {
		return nil
	}
	sort.Strings(retained)
	return fmt.Errorf(
		"cannot raise or cancel the BE scale-down target while Doris is decommissioning pods %v: restore the previous lower replica target, wait for decommission and StatefulSet scale-down to finish, then scale up",
		retained,
	)
}

// validatePendingFEDrops rejects raising an FE target while observers from a
// previous, lower target may already have been dropped. Retrying the original
// target remains valid and lets the manager observe a missing node, clear the
// latch, and continue the StatefulSet scale-down.
func validatePendingFEDrops(actions []ScaleAction, pendingPods []string) error {
	if len(pendingPods) == 0 {
		return nil
	}

	stillRequested := make(map[string]struct{})
	for _, action := range actions {
		if action.Component != constants.ComponentTypeFE {
			continue
		}
		for _, podName := range action.PodsToRemove {
			stillRequested[podName] = struct{}{}
		}
	}

	var retained []string
	for _, podName := range pendingPods {
		if _, requested := stillRequested[podName]; !requested {
			retained = append(retained, podName)
		}
	}
	if len(retained) == 0 {
		return nil
	}
	sort.Strings(retained)
	return fmt.Errorf(
		"cannot raise or cancel the FE scale-down target while Doris has pending observer drops for pods %v: restore the previous lower replica target, wait for DROP OBSERVER and StatefulSet scale-down to finish, then scale up",
		retained,
	)
}

func clusterIsStopped(cluster *dorisv1alpha1.DorisCluster) bool {
	return cluster.Spec.ClusterOperationSpec != nil && cluster.Spec.ClusterOperationSpec.Stopped
}

func desiredRoleGroupReplicas(
	role *dorisv1alpha1.RoleSpec,
	roleGroupName string,
) (int32, bool) {
	if role == nil {
		return 0, false
	}
	roleGroup, exists := role.RoleGroups[roleGroupName]
	if !exists {
		return 0, false
	}
	if roleGroup.Replicas == nil {
		return 1, true
	}
	return *roleGroup.Replicas, true
}

func listStatefulSets(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
) ([]appsv1.StatefulSet, error) {
	if cluster.UID == "" {
		return nil, nil
	}
	statefulSetList := &appsv1.StatefulSetList{}
	if err := k8sClient.List(
		ctx,
		statefulSetList,
		crclient.InNamespace(cluster.Namespace),
		crclient.MatchingLabels{
			opconstant.LabelKubernetesInstance:  cluster.Name,
			opconstant.LabelKubernetesComponent: string(component),
		},
	); err != nil {
		return nil, err
	}

	claimed := make([]appsv1.StatefulSet, 0, len(statefulSetList.Items))
	for i := range statefulSetList.Items {
		statefulSet := &statefulSetList.Items[i]
		if !metav1.IsControlledBy(statefulSet, cluster) {
			continue
		}
		labels := statefulSet.GetLabels()
		roleGroupName := labels[opconstant.LabelKubernetesRoleGroup]
		if roleGroupName == "" || labels[opconstant.LabelKubernetesManagedBy] != legacyManagedByValue {
			continue
		}
		if statefulSet.Name != reconciler.RoleGroupResourceName(
			cluster.Name,
			string(component),
			roleGroupName,
		) {
			continue
		}
		claimed = append(claimed, *statefulSet)
	}
	sort.Slice(claimed, func(i, j int) bool {
		return claimed[i].Name < claimed[j].Name
	})
	return claimed, nil
}

func listPodNames(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
) ([]string, error) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(
		ctx,
		podList,
		crclient.InNamespace(cluster.Namespace),
		crclient.MatchingLabels{
			opconstant.LabelKubernetesInstance:  cluster.Name,
			opconstant.LabelKubernetesComponent: string(component),
		},
	); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(podList.Items))
	for i := range podList.Items {
		names = append(names, podList.Items[i].Name)
	}
	sort.Strings(names)
	return names, nil
}

func fallbackNodeStatuses(podNames []string) []dorisv1alpha1.NodeStatus {
	statuses := make([]dorisv1alpha1.NodeStatus, 0, len(podNames))
	for _, podName := range podNames {
		statuses = append(statuses, dorisv1alpha1.NodeStatus{Name: podName})
	}
	return statuses
}

func connectDoris(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) (*doris_client.DorisClient, bool, error) {
	clusterDomain := "cluster.local"
	if cluster.Spec.ClusterConfig != nil && cluster.Spec.ClusterConfig.ClusterDomain != "" {
		clusterDomain = cluster.Spec.ClusterConfig.ClusterDomain
	}
	feHost := fmt.Sprintf(
		"%s-fe-internal.%s.svc.%s",
		cluster.Name,
		cluster.Namespace,
		clusterDomain,
	)

	managementUser := doris_client.DefaultAdminUser
	managementPassword := ""
	needsBootstrap := cluster.Spec.AuthSecret != nil && !cluster.Status.AuthInitialized
	if cluster.Spec.AuthSecret != nil {
		secret := &corev1.Secret{}
		if err := k8sClient.Get(
			ctx,
			types.NamespacedName{
				Name:      cluster.Spec.AuthSecret.SecretName,
				Namespace: cluster.Namespace,
			},
			secret,
		); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, opcommon.NewRequeueAfterError(
					opcommon.DefaultRequeueAfter,
					"WaitingForDorisAuthSecret",
					"the Doris management credentials Secret is not available",
				)
			}
			return nil, false, fmt.Errorf("get Doris auth Secret: %w", err)
		}
		managementUser, managementPassword = doris_client.GetClusterAuthCredentials(secret.Data)
	}

	if needsBootstrap {
		rootClient, err := doris_client.NewDorisClient(
			feHost,
			constants.FEQueryPort,
			doris_client.DefaultAdminUser,
			"",
		)
		if err != nil {
			log.FromContext(ctx).V(1).Info(
				"Doris frontend is not ready for auth bootstrap",
				"host", feHost,
				"error", err,
			)
			return nil, false, waitingForDorisFrontend()
		}

		exists, err := rootClient.CheckUserExists(ctx, managementUser)
		if err != nil {
			_ = rootClient.Close()
			return nil, false, fmt.Errorf("check Doris management user: %w", err)
		}
		if !exists {
			if err := rootClient.InitializeAdminUser(
				ctx,
				managementUser,
				managementPassword,
			); err != nil {
				_ = rootClient.Close()
				return nil, false, fmt.Errorf("initialize Doris management user: %w", err)
			}
		}
		if err := rootClient.Close(); err != nil {
			return nil, false, fmt.Errorf("close Doris bootstrap connection: %w", err)
		}
	}

	managementClient, err := doris_client.NewDorisClient(
		feHost,
		constants.FEQueryPort,
		managementUser,
		managementPassword,
	)
	if err != nil {
		log.FromContext(ctx).V(1).Info(
			"Doris frontend is not ready for management operations",
			"host", feHost,
			"user", managementUser,
			"error", err,
		)
		return nil, false, waitingForDorisFrontend()
	}
	return managementClient, needsBootstrap, nil
}

func waitingForDorisFrontend() error {
	return opcommon.NewRequeueAfterError(
		opcommon.DefaultRequeueAfter,
		"WaitingForDorisFrontend",
		"the Doris frontend is not ready for management operations",
	)
}

// clusterScaleDownPolicy adapts the API policy to the existing scale manager.
type clusterScaleDownPolicy struct {
	spec *dorisv1alpha1.DorisClusterSpec
}

var _ ScaleDownPolicy = (*clusterScaleDownPolicy)(nil)

func (p *clusterScaleDownPolicy) GetDecommissionTimeout() time.Duration {
	return GetDecommissionTimeout(p.spec)
}

// annotationDecommissionTracker stores persistent BE/FE operation fences and
// safe-zero evidence under legal annotation keys. Kubernetes annotation keys
// may contain only one slash, and the suffix is length-limited, so pod and role
// group names belong in JSON values rather than in a second path segment.
type annotationDecommissionTracker struct {
	cluster       *dorisv1alpha1.DorisCluster
	client        crclient.Client
	starts        map[string]string
	frontendDrops map[string]string
	safeZeros     map[string]bool
	dirty         bool
}

var _ DecommissionTracker = (*annotationDecommissionTracker)(nil)

func newAnnotationDecommissionTracker(
	cluster *dorisv1alpha1.DorisCluster,
	k8sClient crclient.Client,
) (*annotationDecommissionTracker, error) {
	tracker := &annotationDecommissionTracker{
		cluster:       cluster,
		client:        k8sClient,
		starts:        make(map[string]string),
		frontendDrops: make(map[string]string),
		safeZeros:     make(map[string]bool),
	}
	if cluster.Annotations == nil {
		return tracker, nil
	}
	if raw := cluster.Annotations[AnnotationDecommissionStart]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &tracker.starts); err != nil {
			return nil, fmt.Errorf(
				"decode %q annotation: %w",
				AnnotationDecommissionStart,
				err,
			)
		}
		if tracker.starts == nil {
			tracker.starts = make(map[string]string)
		}
	}
	if raw := cluster.Annotations[AnnotationFrontendDropIntent]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &tracker.frontendDrops); err != nil {
			return nil, fmt.Errorf(
				"decode %q annotation: %w",
				AnnotationFrontendDropIntent,
				err,
			)
		}
		if tracker.frontendDrops == nil {
			tracker.frontendDrops = make(map[string]string)
		}
		for podName, phase := range tracker.frontendDrops {
			if podName == "" {
				return nil, fmt.Errorf("decode %q annotation: pod name must not be empty", AnnotationFrontendDropIntent)
			}
			switch phase {
			case frontendDropPhasePending, frontendDropPhaseRemoved:
			default:
				return nil, fmt.Errorf(
					"decode %q annotation: unsupported phase %q for pod %q",
					AnnotationFrontendDropIntent,
					phase,
					podName,
				)
			}
		}
	}
	safeZeros, err := decodeSafeZeroRoleGroups(cluster.Annotations)
	if err != nil {
		return nil, err
	}
	tracker.safeZeros = safeZeros
	return tracker, nil
}

func decodeSafeZeroRoleGroups(annotations map[string]string) (map[string]bool, error) {
	result := make(map[string]bool)
	if annotations == nil || annotations[AnnotationSafelyScaledToZero] == "" {
		return result, nil
	}
	if err := json.Unmarshal([]byte(annotations[AnnotationSafelyScaledToZero]), &result); err != nil {
		return nil, fmt.Errorf("decode %q annotation: %w", AnnotationSafelyScaledToZero, err)
	}
	if result == nil {
		result = make(map[string]bool)
	}
	return result, nil
}

func safeZeroKey(component constants.ComponentType, roleGroupName string) string {
	return string(component) + "/" + roleGroupName
}

func (t *annotationDecommissionTracker) MarkSafeZero(
	component constants.ComponentType,
	roleGroupName string,
) {
	key := safeZeroKey(component, roleGroupName)
	if t.safeZeros[key] {
		return
	}
	t.safeZeros[key] = true
	t.dirty = true
}

func (t *annotationDecommissionTracker) ClearSafeZero(
	component constants.ComponentType,
	roleGroupName string,
) {
	key := safeZeroKey(component, roleGroupName)
	if _, exists := t.safeZeros[key]; !exists {
		return
	}
	delete(t.safeZeros, key)
	t.dirty = true
}

// ClearEvidenceForRunningGroups invalidates a previous zero marker as soon as
// the desired spec asks that group to run again. This happens even while the
// cluster is stopped, when the StatefulSet itself remains at zero.
func (t *annotationDecommissionTracker) ClearEvidenceForRunningGroups(
	cluster *dorisv1alpha1.DorisCluster,
) {
	roles := []struct {
		component constants.ComponentType
		role      *dorisv1alpha1.RoleSpec
	}{
		{component: constants.ComponentTypeFE, role: cluster.Spec.Frontend},
		{component: constants.ComponentTypeBE, role: cluster.Spec.Backend},
	}
	for _, item := range roles {
		if item.role == nil {
			continue
		}
		for groupName, group := range item.role.RoleGroups {
			desired := int32(1)
			if group.Replicas != nil {
				desired = *group.Replicas
			}
			if desired > 0 {
				t.ClearSafeZero(item.component, groupName)
			}
		}
	}
}

func (t *annotationDecommissionTracker) GetStart(podName string) (string, bool) {
	value, exists := t.starts[podName]
	return value, exists
}

func (t *annotationDecommissionTracker) RecordStart(podName, timestamp string) {
	if current, exists := t.starts[podName]; exists && current == timestamp {
		return
	}
	t.starts[podName] = timestamp
	t.dirty = true
}

func (t *annotationDecommissionTracker) RecordIntent(podName string) {
	if _, exists := t.starts[podName]; exists {
		return
	}
	t.starts[podName] = decommissionIntentPending
	t.dirty = true
}

func (t *annotationDecommissionTracker) ClearStart(podName string) {
	if _, exists := t.starts[podName]; !exists {
		return
	}
	delete(t.starts, podName)
	t.dirty = true
}

func (t *annotationDecommissionTracker) PendingPods() []string {
	pods := make([]string, 0, len(t.starts))
	for podName := range t.starts {
		pods = append(pods, podName)
	}
	sort.Strings(pods)
	return pods
}

func (t *annotationDecommissionTracker) RecordFrontendDropIntent(podName string) {
	if _, exists := t.frontendDrops[podName]; exists {
		return
	}
	t.frontendDrops[podName] = frontendDropPhasePending
	t.dirty = true
}

func (t *annotationDecommissionTracker) MarkFrontendRemoved(podName string) {
	if t.frontendDrops[podName] == frontendDropPhaseRemoved {
		return
	}
	t.frontendDrops[podName] = frontendDropPhaseRemoved
	t.dirty = true
}

func (t *annotationDecommissionTracker) clearFrontendDropIntent(podName string) {
	if _, exists := t.frontendDrops[podName]; !exists {
		return
	}
	delete(t.frontendDrops, podName)
	t.dirty = true
}

func (t *annotationDecommissionTracker) PendingFrontendDropPods() []string {
	pods := make([]string, 0, len(t.frontendDrops))
	for podName := range t.frontendDrops {
		pods = append(pods, podName)
	}
	sort.Strings(pods)
	return pods
}

// ClearConvergedFrontendDrops releases a removed latch only after the live StatefulSet
// specs no longer retain that exact pod name. Listing every StatefulSet in the
// namespace is deliberately conservative with respect to damaged labels or
// owner references: an object occupying the same StatefulSet name still keeps
// the latch in place.
func (t *annotationDecommissionTracker) ClearConvergedFrontendDrops(ctx context.Context) error {
	if len(t.frontendDrops) == 0 {
		return nil
	}

	statefulSets := &appsv1.StatefulSetList{}
	if err := t.client.List(
		ctx,
		statefulSets,
		crclient.InNamespace(t.cluster.Namespace),
	); err != nil {
		return err
	}
	for podName, phase := range t.frontendDrops {
		if phase != frontendDropPhaseRemoved {
			continue
		}
		if anyStatefulSetRetainsPod(statefulSets.Items, podName) {
			continue
		}
		t.clearFrontendDropIntent(podName)
	}
	return nil
}

func anyStatefulSetRetainsPod(statefulSets []appsv1.StatefulSet, podName string) bool {
	for i := range statefulSets {
		statefulSet := &statefulSets[i]
		prefix := statefulSet.Name + "-"
		if !strings.HasPrefix(podName, prefix) {
			continue
		}
		ordinal, err := strconv.ParseInt(strings.TrimPrefix(podName, prefix), 10, 32)
		if err != nil {
			continue
		}
		start := int64(0)
		if statefulSet.Spec.Ordinals != nil {
			start = int64(statefulSet.Spec.Ordinals.Start)
		}
		replicas := int64(GetStatefulSetReplicas(statefulSet))
		if ordinal >= start && ordinal < start+replicas {
			return true
		}
	}
	return false
}

func (t *annotationDecommissionTracker) Persist(ctx context.Context) error {
	if !t.dirty {
		return nil
	}

	latest := &dorisv1alpha1.DorisCluster{}
	if err := t.client.Get(
		ctx,
		types.NamespacedName{
			Name:      t.cluster.Name,
			Namespace: t.cluster.Namespace,
		},
		latest,
	); err != nil {
		return err
	}
	if latest.UID != t.cluster.UID {
		return fmt.Errorf(
			"DorisCluster %s/%s was replaced while persisting scale state",
			t.cluster.Namespace,
			t.cluster.Name,
		)
	}
	if latest.Generation != t.cluster.Generation {
		return opcommon.NewRequeueAfterError(
			opcommon.DefaultRequeueAfter,
			"DorisScaleSpecChanged",
			"the DorisCluster spec changed while scale state was being persisted; retrying with the current target",
		)
	}

	before := latest.DeepCopy()
	if latest.Annotations == nil {
		latest.Annotations = make(map[string]string)
	}
	if len(t.starts) == 0 {
		delete(latest.Annotations, AnnotationDecommissionStart)
	} else {
		encoded, err := json.Marshal(t.starts)
		if err != nil {
			return fmt.Errorf("encode Doris decommission state: %w", err)
		}
		latest.Annotations[AnnotationDecommissionStart] = string(encoded)
	}
	if len(t.frontendDrops) == 0 {
		delete(latest.Annotations, AnnotationFrontendDropIntent)
	} else {
		encoded, err := json.Marshal(t.frontendDrops)
		if err != nil {
			return fmt.Errorf("encode Doris frontend drop intent: %w", err)
		}
		latest.Annotations[AnnotationFrontendDropIntent] = string(encoded)
	}
	if len(t.safeZeros) == 0 {
		delete(latest.Annotations, AnnotationSafelyScaledToZero)
	} else {
		encoded, err := json.Marshal(t.safeZeros)
		if err != nil {
			return fmt.Errorf("encode Doris safe scale-down evidence: %w", err)
		}
		latest.Annotations[AnnotationSafelyScaledToZero] = string(encoded)
	}

	// This patch is the linearization point for a destructive Doris operation.
	// Optimistic locking ensures a concurrent spec update wins and forces this
	// reconcile to restart before it calls the Doris API.
	if err := t.client.Patch(
		ctx,
		latest,
		crclient.MergeFromWithOptions(before, crclient.MergeFromWithOptimisticLock{}),
	); err != nil {
		return err
	}

	// Keep the object passed to the generic reconciler current, so its later
	// status patch does not use the resourceVersion preceding this metadata patch.
	t.cluster.Annotations = cloneStringMap(latest.Annotations)
	t.cluster.ResourceVersion = latest.ResourceVersion
	t.dirty = false
	return nil
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

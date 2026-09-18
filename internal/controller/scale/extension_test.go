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
	"reflect"
	"strings"
	"testing"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opcommon "github.com/zncdatadev/operator-go/pkg/common"
	opconstant "github.com/zncdatadev/operator-go/pkg/constant"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	extensionTestClusterName       = "doris"
	extensionTestNamespace         = "default"
	extensionTestHotRoleGroup      = "hot"
	extensionTestColdRoleGroup     = "cold"
	extensionTestObserverRoleGroup = "observer"
	extensionTestTrackedPod        = "doris-be-hot-3"
	extensionTestPodTwo            = "doris-be-hot-2"
	extensionTestFEPodOne          = "doris-fe-hot-1"
	extensionTestFEPodTwo          = "doris-fe-hot-2"
	extensionTestFEPodThree        = "doris-fe-hot-3"
	extensionTestStartTime         = "2026-09-14T01:02:03Z"
	extensionTestPreservedValue    = "preserve-me"
	extensionTestClusterUID        = "doris-cluster-test-uid"
)

func TestCollectScaleDownActionsPerRoleGroupStatefulSet(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			extensionTestHotRoleGroup: {
				Replicas: extensionTestReplicas(2),
			},
			extensionTestColdRoleGroup: {
				Replicas: extensionTestReplicas(3),
			},
		},
	}
	cluster.Spec.Frontend = &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			extensionTestObserverRoleGroup: {
				Replicas: extensionTestReplicas(2),
			},
		},
	}

	hot := extensionTestStatefulSet(
		constants.ComponentTypeBE,
		extensionTestHotRoleGroup,
		4,
	)
	// This group scales up. An aggregate component comparison would cancel out
	// the hot group's scale-down and miss its required decommission.
	cold := extensionTestStatefulSet(
		constants.ComponentTypeBE,
		extensionTestColdRoleGroup,
		1,
	)
	observer := extensionTestStatefulSet(
		constants.ComponentTypeFE,
		extensionTestObserverRoleGroup,
		3,
	)
	k8sClient := extensionTestClient(t, hot, cold, observer)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err != nil {
		t.Fatalf("collectScaleDownActions() error = %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("collectScaleDownActions() returned %d actions, want 2: %#v", len(actions), actions)
	}

	actionsByStatefulSet := make(map[string]ScaleAction, len(actions))
	for _, action := range actions {
		actionsByStatefulSet[action.StatefulSetNames[0]] = action
	}

	hotAction, exists := actionsByStatefulSet[hot.Name]
	if !exists {
		t.Fatalf("missing scale-down action for StatefulSet %q", hot.Name)
	}
	if hotAction.Component != constants.ComponentTypeBE {
		t.Errorf("hot action component = %q, want %q", hotAction.Component, constants.ComponentTypeBE)
	}
	if hotAction.CurrentReplicas != 4 || hotAction.DesiredReplicas != 2 {
		t.Errorf(
			"hot action replicas = %d -> %d, want 4 -> 2",
			hotAction.CurrentReplicas,
			hotAction.DesiredReplicas,
		)
	}
	wantHotPods := []string{hot.Name + "-2", hot.Name + "-3"}
	if !reflect.DeepEqual(hotAction.PodsToRemove, wantHotPods) {
		t.Errorf("hot action pods = %#v, want %#v", hotAction.PodsToRemove, wantHotPods)
	}

	observerAction, exists := actionsByStatefulSet[observer.Name]
	if !exists {
		t.Fatalf("missing scale-down action for StatefulSet %q", observer.Name)
	}
	if observerAction.Component != constants.ComponentTypeFE {
		t.Errorf(
			"observer action component = %q, want %q",
			observerAction.Component,
			constants.ComponentTypeFE,
		)
	}
	if observerAction.CurrentReplicas != 3 || observerAction.DesiredReplicas != 2 {
		t.Errorf(
			"observer action replicas = %d -> %d, want 3 -> 2",
			observerAction.CurrentReplicas,
			observerAction.DesiredReplicas,
		)
	}
	wantObserverPods := []string{observer.Name + "-2"}
	if !reflect.DeepEqual(observerAction.PodsToRemove, wantObserverPods) {
		t.Errorf(
			"observer action pods = %#v, want %#v",
			observerAction.PodsToRemove,
			wantObserverPods,
		)
	}

	if _, exists := actionsByStatefulSet[cold.Name]; exists {
		t.Errorf("unexpected scale-down action for scaling-up StatefulSet %q", cold.Name)
	}
}

func TestCollectScaleDownActionsRejectsDirectLiveRoleGroupRemoval(t *testing.T) {
	tests := []struct {
		name      string
		component constants.ComponentType
	}{
		{
			name:      "frontend",
			component: constants.ComponentTypeFE,
		},
		{
			name:      "backend",
			component: constants.ComponentTypeBE,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := extensionTestCluster()
			cluster.Spec.Frontend = extensionTestKeptRole()
			cluster.Spec.Backend = extensionTestKeptRole()
			removed := extensionTestStatefulSet(test.component, "removed", 2)
			k8sClient := extensionTestClient(t, removed)

			actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
			if err == nil {
				t.Fatalf(
					"collectScaleDownActions() actions = %#v, want direct live role-group removal error",
					actions,
				)
			}
			wantFragments := []string{
				"cannot remove live " + string(test.component) + " role group",
				"set replicas to 0",
			}
			for _, fragment := range wantFragments {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("error %q does not contain %q", err, fragment)
				}
			}
		})
	}
}

func TestCollectScaleDownActionsRejectsRoleGroupRemovalWhileStopped(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.ClusterOperationSpec = &commonsv1alpha1.ClusterOperationSpec{Stopped: true}
	cluster.Spec.Frontend = extensionTestKeptRole()
	cluster.Spec.Backend = extensionTestKeptRole()
	removed := extensionTestStatefulSet(constants.ComponentTypeBE, "removed", 0)
	k8sClient := extensionTestClient(t, removed)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err == nil {
		t.Fatalf("collectScaleDownActions() actions = %#v, want stopped-cluster role-group removal error", actions)
	}
	for _, fragment := range []string{"while the Doris cluster is stopped", "resume the cluster", "set replicas to 0"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q does not contain %q", err, fragment)
		}
	}
}

func TestCollectScaleDownActionsRejectsZeroReplicaRemovalWithoutEvidence(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.Frontend = extensionTestKeptRole()
	cluster.Spec.Backend = extensionTestKeptRole()
	removed := extensionTestStatefulSet(constants.ComponentTypeBE, "removed", 0)
	k8sClient := extensionTestClient(t, removed)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err == nil {
		t.Fatalf("collectScaleDownActions() actions = %#v, want missing safe scale-down evidence error", actions)
	}
	for _, fragment := range []string{"without safe scale-down evidence", "run it with at least one replica", "replicas to 0"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("error %q does not contain %q", err, fragment)
		}
	}
}

func TestCollectScaleDownActionsAllowsZeroReplicaRemovalWithEvidence(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.Frontend = extensionTestKeptRole()
	cluster.Spec.Backend = extensionTestKeptRole()
	encoded, err := json.Marshal(map[string]bool{"be/removed": true})
	if err != nil {
		t.Fatalf("json.Marshal(safe zeros) error = %v", err)
	}
	cluster.Annotations = map[string]string{AnnotationSafelyScaledToZero: string(encoded)}
	removed := extensionTestStatefulSet(constants.ComponentTypeBE, "removed", 0)
	k8sClient := extensionTestClient(t, removed)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err != nil {
		t.Fatalf("collectScaleDownActions() error = %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("collectScaleDownActions() actions = %#v, want none before orphan cleanup", actions)
	}
}

func TestCollectScaleDownActionsDoesNotDecommissionConfiguredGroupsWhileStopped(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.ClusterOperationSpec = &commonsv1alpha1.ClusterOperationSpec{Stopped: true}
	cluster.Spec.Frontend = extensionTestKeptRole()
	cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
		extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(0)},
	}}
	statefulSet := extensionTestStatefulSet(constants.ComponentTypeBE, extensionTestHotRoleGroup, 3)
	k8sClient := extensionTestClient(t, statefulSet)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err != nil {
		t.Fatalf("collectScaleDownActions() error = %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("collectScaleDownActions() actions = %#v, want stop to preserve Doris topology", actions)
	}
}

func TestCollectScaleDownActionsIgnoresUnclaimedStatefulSets(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.StatefulSet)
	}{
		{
			name: "foreign controller owner",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.OwnerReferences[0].UID = "foreign-cluster-uid"
			},
		},
		{
			name: "wrong managed-by label",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.Labels[opconstant.LabelKubernetesManagedBy] = "another-operator"
			},
		},
		{
			name: "noncanonical name",
			mutate: func(statefulSet *appsv1.StatefulSet) {
				statefulSet.Name += "-copy"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := extensionTestCluster()
			cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(0)},
				},
			}
			statefulSet := extensionTestStatefulSet(constants.ComponentTypeBE, extensionTestHotRoleGroup, 3)
			test.mutate(statefulSet)

			actions, err := collectScaleDownActions(
				context.Background(),
				extensionTestClient(t, statefulSet),
				cluster,
			)
			if err != nil {
				t.Fatalf("collectScaleDownActions() error = %v", err)
			}
			if len(actions) != 0 {
				t.Fatalf("collectScaleDownActions() actions = %#v, want unclaimed StatefulSet ignored", actions)
			}
		})
	}
}

func TestCollectScaleDownActionsRequiresEvidenceForLedgerOrphanWithoutStatefulSet(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.Backend = extensionTestKeptRole()
	cluster.Status.RoleGroups = map[string][]string{
		string(constants.ComponentTypeBE): {"removed"},
	}

	actions, err := collectScaleDownActions(
		context.Background(),
		extensionTestClient(t),
		cluster,
	)
	if err == nil {
		t.Fatalf("collectScaleDownActions() actions = %#v, want missing safe scale-down evidence error", actions)
	}
	if !strings.Contains(err.Error(), "cannot remove be role group \"removed\" without safe scale-down evidence") {
		t.Fatalf("collectScaleDownActions() error = %q, want ledger-orphan safety error", err)
	}

	encoded, marshalErr := json.Marshal(map[string]bool{"be/removed": true})
	if marshalErr != nil {
		t.Fatalf("json.Marshal(safe zeros) error = %v", marshalErr)
	}
	cluster.Annotations = map[string]string{AnnotationSafelyScaledToZero: string(encoded)}
	actions, err = collectScaleDownActions(
		context.Background(),
		extensionTestClient(t),
		cluster,
	)
	if err != nil {
		t.Fatalf("collectScaleDownActions() with evidence error = %v", err)
	}
	if len(actions) != 0 {
		t.Fatalf("collectScaleDownActions() with evidence actions = %#v, want none", actions)
	}
}

func TestValidatePendingBEDecommissionsRejectsTargetRollback(t *testing.T) {
	previousPods := []string{"doris-be-hot-1", extensionTestPodTwo}
	tests := []struct {
		name    string
		actions []ScaleAction
		wantErr bool
	}{
		{
			name: "original target one remains valid",
			actions: []ScaleAction{{
				Component:    constants.ComponentTypeBE,
				PodsToRemove: previousPods,
			}},
		},
		{
			name: "target raised from one to two",
			actions: []ScaleAction{{
				Component:    constants.ComponentTypeBE,
				PodsToRemove: []string{extensionTestPodTwo},
			}},
			wantErr: true,
		},
		{
			name:    "target raised from one to three",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePendingBEDecommissions(test.actions, previousPods)
			if test.wantErr && err == nil {
				t.Fatal("validatePendingBEDecommissions() error = nil, want target rollback rejection")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validatePendingBEDecommissions() error = %v, want nil", err)
			}
			if err != nil {
				for _, fragment := range []string{"cannot raise or cancel", "restore the previous lower replica target"} {
					if !strings.Contains(err.Error(), fragment) {
						t.Errorf("error %q does not contain %q", err, fragment)
					}
				}
			}
		})
	}
}

func TestScaleExtensionPreRejectsBEDecommissionTargetRollbackBeforeConnect(t *testing.T) {
	encoded, err := json.Marshal(map[string]string{
		"doris-be-hot-1":    extensionTestStartTime,
		extensionTestPodTwo: extensionTestStartTime,
	})
	if err != nil {
		t.Fatalf("json.Marshal(decommission starts) error = %v", err)
	}

	for _, desired := range []int32{2, 3} {
		t.Run(fmt.Sprintf("target_%d", desired), func(t *testing.T) {
			cluster := extensionTestCluster()
			cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(desired)},
				},
			}
			cluster.Annotations = map[string]string{AnnotationDecommissionStart: string(encoded)}
			statefulSet := extensionTestStatefulSet(constants.ComponentTypeBE, extensionTestHotRoleGroup, 3)

			err := NewScaleExtension().PreReconcile(
				context.Background(),
				extensionTestClient(t, cluster, statefulSet),
				cluster,
			)
			if err == nil {
				t.Fatal("PreReconcile() error = nil, want target rollback rejection")
			}
			if !strings.Contains(err.Error(), "cannot raise or cancel the BE scale-down target") {
				t.Fatalf("PreReconcile() error = %q, want rollback error before Doris connection", err)
			}
		})
	}
}

func TestValidatePendingFEDropsRejectsTargetRollback(t *testing.T) {
	previousPods := []string{extensionTestFEPodOne, extensionTestFEPodTwo}
	tests := []struct {
		name    string
		actions []ScaleAction
		wantErr bool
	}{
		{
			name: "original target one remains valid",
			actions: []ScaleAction{{
				Component:    constants.ComponentTypeFE,
				PodsToRemove: previousPods,
			}},
		},
		{
			name: "target raised from one to two",
			actions: []ScaleAction{{
				Component:    constants.ComponentTypeFE,
				PodsToRemove: []string{extensionTestFEPodTwo},
			}},
			wantErr: true,
		},
		{
			name:    "target raised from one to three",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePendingFEDrops(test.actions, previousPods)
			if test.wantErr && err == nil {
				t.Fatal("validatePendingFEDrops() error = nil, want target rollback rejection")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validatePendingFEDrops() error = %v, want nil", err)
			}
			if err != nil {
				for _, fragment := range []string{"cannot raise or cancel", "restore the previous lower replica target"} {
					if !strings.Contains(err.Error(), fragment) {
						t.Errorf("error %q does not contain %q", err, fragment)
					}
				}
			}
		})
	}
}

func TestScaleExtensionPreRejectsFEDropTargetRollbackBeforeConnect(t *testing.T) {
	encoded, err := json.Marshal(map[string]string{
		extensionTestFEPodOne: frontendDropPhasePending,
		extensionTestFEPodTwo: frontendDropPhaseRemoved,
	})
	if err != nil {
		t.Fatalf("json.Marshal(frontend drop intents) error = %v", err)
	}

	for _, desired := range []int32{2, 3} {
		t.Run(fmt.Sprintf("target_%d", desired), func(t *testing.T) {
			cluster := extensionTestCluster()
			cluster.Spec.Frontend = &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(desired)},
				},
			}
			cluster.Annotations = map[string]string{AnnotationFrontendDropIntent: string(encoded)}
			statefulSet := extensionTestStatefulSet(constants.ComponentTypeFE, extensionTestHotRoleGroup, 3)

			err := NewScaleExtension().PreReconcile(
				context.Background(),
				extensionTestClient(t, cluster, statefulSet),
				cluster,
			)
			if err == nil {
				t.Fatal("PreReconcile() error = nil, want FE target rollback rejection")
			}
			if !strings.Contains(err.Error(), "cannot raise or cancel the FE scale-down target") {
				t.Fatalf("PreReconcile() error = %q, want rollback error before Doris connection", err)
			}
		})
	}
}

func TestScaleExtensionPreDoesNotPersistFEDropIntentBeforeConnect(t *testing.T) {
	ctx := context.Background()
	cluster := extensionTestCluster()
	cluster.Spec.Frontend = &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(2)},
		},
	}
	cluster.Spec.AuthSecret = &dorisv1alpha1.AuthSecretSpec{SecretName: "missing"}
	statefulSet := extensionTestStatefulSet(constants.ComponentTypeFE, extensionTestHotRoleGroup, 3)
	k8sClient := extensionTestClient(t, cluster, statefulSet)

	err := NewScaleExtension().PreReconcile(ctx, k8sClient, cluster)
	if err == nil {
		t.Fatal("PreReconcile() error = nil, want missing auth Secret wait")
	}
	if !opcommon.IsRequeueAfterError(err) {
		t.Fatalf("PreReconcile() error = %v, want RequeueAfterError", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get intent-bearing DorisCluster error = %v", err)
	}
	if _, exists := stored.Annotations[AnnotationFrontendDropIntent]; exists {
		t.Fatalf("annotation %q persisted before Doris connection and FE preflight", AnnotationFrontendDropIntent)
	}
}

func TestScaleExtensionPreClearsFEDropLatchAfterStatefulSetConverges(t *testing.T) {
	ctx := context.Background()
	encoded, err := json.Marshal(map[string]string{
		extensionTestFEPodTwo: frontendDropPhaseRemoved,
	})
	if err != nil {
		t.Fatalf("json.Marshal(frontend drop phase) error = %v", err)
	}
	cluster := extensionTestCluster()
	cluster.Spec.Frontend = &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			extensionTestHotRoleGroup: {Replicas: extensionTestReplicas(3)},
		},
	}
	cluster.Annotations = map[string]string{AnnotationFrontendDropIntent: string(encoded)}
	statefulSet := extensionTestStatefulSet(constants.ComponentTypeFE, extensionTestHotRoleGroup, 2)
	k8sClient := extensionTestClient(t, cluster, statefulSet)

	if err := NewScaleExtension().PreReconcile(ctx, k8sClient, cluster); err != nil {
		t.Fatalf("PreReconcile() after StatefulSet convergence error = %v", err)
	}
	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get latch-cleared DorisCluster error = %v", err)
	}
	if _, exists := stored.Annotations[AnnotationFrontendDropIntent]; exists {
		t.Fatalf("annotation %q remains after StatefulSet excluded the dropped pod", AnnotationFrontendDropIntent)
	}
}

func TestCollectScaleDownActionsDefaultsMissingReplicasToOne(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Spec.Frontend = extensionTestKeptRole()
	cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			extensionTestHotRoleGroup: {},
		},
	}
	statefulSet := extensionTestStatefulSet(
		constants.ComponentTypeBE,
		extensionTestHotRoleGroup,
		2,
	)
	k8sClient := extensionTestClient(t, statefulSet)

	actions, err := collectScaleDownActions(context.Background(), k8sClient, cluster)
	if err != nil {
		t.Fatalf("collectScaleDownActions() error = %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("collectScaleDownActions() returned %d actions, want 1: %#v", len(actions), actions)
	}

	action := actions[0]
	if action.CurrentReplicas != 2 || action.DesiredReplicas != 1 {
		t.Errorf(
			"action replicas = %d -> %d, want 2 -> 1",
			action.CurrentReplicas,
			action.DesiredReplicas,
		)
	}
	wantPods := []string{statefulSet.Name + "-1"}
	if !reflect.DeepEqual(action.PodsToRemove, wantPods) {
		t.Errorf("action pods = %#v, want %#v", action.PodsToRemove, wantPods)
	}

	desired, exists := desiredRoleGroupReplicas(
		cluster.Spec.Backend,
		extensionTestHotRoleGroup,
	)
	if !exists {
		t.Fatal("desiredRoleGroupReplicas() reports configured role group as absent")
	}
	if desired != 1 {
		t.Errorf("desiredRoleGroupReplicas() = %d, want default 1", desired)
	}
}

func TestAnnotationDecommissionTrackerJSONLifecycle(t *testing.T) {
	ctx := context.Background()
	initialStarts := map[string]string{
		extensionTestPodTwo:     extensionTestStartTime,
		extensionTestTrackedPod: "2026-09-14T02:03:04Z",
	}
	encodedInitial, err := json.Marshal(initialStarts)
	if err != nil {
		t.Fatalf("json.Marshal(initialStarts) error = %v", err)
	}
	cluster := extensionTestCluster()
	cluster.Annotations = map[string]string{
		AnnotationDecommissionStart: string(encodedInitial),
		"example.com/unrelated":     extensionTestPreservedValue,
	}
	k8sClient := extensionTestClient(t, cluster)

	if slashCount := strings.Count(AnnotationDecommissionStart, "/"); slashCount != 1 {
		t.Fatalf(
			"AnnotationDecommissionStart contains %d slashes, want exactly 1: %q",
			slashCount,
			AnnotationDecommissionStart,
		)
	}

	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}
	if got, exists := tracker.GetStart(extensionTestPodTwo); !exists || got != initialStarts[extensionTestPodTwo] {
		t.Errorf(
			"GetStart(doris-be-hot-2) = %q, %t, want %q, true",
			got,
			exists,
			initialStarts[extensionTestPodTwo],
		)
	}
	wantInitialPods := []string{extensionTestPodTwo, extensionTestTrackedPod}
	if got := tracker.PendingPods(); !reflect.DeepEqual(got, wantInitialPods) {
		t.Errorf("PendingPods() = %#v, want %#v", got, wantInitialPods)
	}

	tracker.ClearStart(extensionTestPodTwo)
	tracker.RecordStart("doris-be-hot-4", "2026-09-14T03:04:05Z")
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(
		ctx,
		crclient.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace},
		stored,
	); err != nil {
		t.Fatalf("get persisted DorisCluster error = %v", err)
	}
	if got := stored.Annotations["example.com/unrelated"]; got != extensionTestPreservedValue {
		t.Errorf("unrelated annotation = %q, want preserve-me", got)
	}
	var persistedStarts map[string]string
	if err := json.Unmarshal(
		[]byte(stored.Annotations[AnnotationDecommissionStart]),
		&persistedStarts,
	); err != nil {
		t.Fatalf("persisted annotation is not valid JSON: %v", err)
	}
	wantPersistedStarts := map[string]string{
		extensionTestTrackedPod: "2026-09-14T02:03:04Z",
		"doris-be-hot-4":        "2026-09-14T03:04:05Z",
	}
	if !reflect.DeepEqual(persistedStarts, wantPersistedStarts) {
		t.Errorf("persisted starts = %#v, want %#v", persistedStarts, wantPersistedStarts)
	}

	tracker.ClearStart(extensionTestTrackedPod)
	tracker.ClearStart("doris-be-hot-4")
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() after clearing all starts error = %v", err)
	}
	if got := tracker.PendingPods(); len(got) != 0 {
		t.Errorf("PendingPods() after clearing = %#v, want empty", got)
	}

	cleared := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(
		ctx,
		crclient.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace},
		cleared,
	); err != nil {
		t.Fatalf("get cleared DorisCluster error = %v", err)
	}
	if _, exists := cleared.Annotations[AnnotationDecommissionStart]; exists {
		t.Errorf(
			"annotation %q still exists after clearing all starts",
			AnnotationDecommissionStart,
		)
	}
	if got := cleared.Annotations["example.com/unrelated"]; got != extensionTestPreservedValue {
		t.Errorf("unrelated annotation after clearing = %q, want preserve-me", got)
	}
}

func TestAnnotationDecommissionTrackerPersistsPreMutationIntent(t *testing.T) {
	ctx := context.Background()
	cluster := extensionTestCluster()
	k8sClient := extensionTestClient(t, cluster)
	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}

	const podName = extensionTestPodTwo
	tracker.RecordIntent(podName)
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() intent error = %v", err)
	}
	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get intent-bearing DorisCluster error = %v", err)
	}
	var starts map[string]string
	if err := json.Unmarshal([]byte(stored.Annotations[AnnotationDecommissionStart]), &starts); err != nil {
		t.Fatalf("decode persisted intent error = %v", err)
	}
	if got := starts[podName]; got != decommissionIntentPending {
		t.Fatalf("persisted intent = %q, want %q", got, decommissionIntentPending)
	}

	const startedAt = "2026-09-14T03:04:05Z"
	tracker.RecordStart(podName, startedAt)
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() timestamp error = %v", err)
	}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get timestamp-bearing DorisCluster error = %v", err)
	}
	if err := json.Unmarshal([]byte(stored.Annotations[AnnotationDecommissionStart]), &starts); err != nil {
		t.Fatalf("decode persisted timestamp error = %v", err)
	}
	if got := starts[podName]; got != startedAt {
		t.Fatalf("persisted timestamp = %q, want %q", got, startedAt)
	}
}

func TestAnnotationDecommissionTrackerRejectsStaleSpecBeforePersistingIntent(t *testing.T) {
	ctx := context.Background()
	staleCluster := extensionTestCluster()
	staleCluster.Generation = 1
	currentCluster := staleCluster.DeepCopy()
	currentCluster.Generation = 2
	k8sClient := extensionTestClient(t, currentCluster)

	tracker, err := newAnnotationDecommissionTracker(staleCluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}
	tracker.RecordIntent(extensionTestPodTwo)
	err = tracker.Persist(ctx)
	if err == nil {
		t.Fatal("Persist() error = nil, want stale-generation retry")
	}
	if !opcommon.IsRequeueAfterError(err) {
		t.Fatalf("Persist() error = %v, want RequeueAfterError", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(currentCluster), stored); err != nil {
		t.Fatalf("get current DorisCluster error = %v", err)
	}
	if _, exists := stored.Annotations[AnnotationDecommissionStart]; exists {
		t.Errorf("stale reconcile persisted %q on the current spec", AnnotationDecommissionStart)
	}
}

func TestAnnotationDecommissionTrackerFrontendDropIntentJSONLifecycle(t *testing.T) {
	ctx := context.Background()
	initial, err := json.Marshal(map[string]string{extensionTestFEPodOne: frontendDropPhasePending})
	if err != nil {
		t.Fatalf("json.Marshal(frontend drop intents) error = %v", err)
	}
	cluster := extensionTestCluster()
	cluster.Annotations = map[string]string{
		AnnotationFrontendDropIntent: string(initial),
		"example.com/unrelated":      extensionTestPreservedValue,
	}
	k8sClient := extensionTestClient(t, cluster)

	if slashCount := strings.Count(AnnotationFrontendDropIntent, "/"); slashCount != 1 {
		t.Fatalf(
			"AnnotationFrontendDropIntent contains %d slashes, want exactly 1: %q",
			slashCount,
			AnnotationFrontendDropIntent,
		)
	}
	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}
	if got := tracker.PendingFrontendDropPods(); !reflect.DeepEqual(got, []string{extensionTestFEPodOne}) {
		t.Fatalf("PendingFrontendDropPods() = %#v, want initial pod", got)
	}

	tracker.RecordFrontendDropIntent(extensionTestFEPodTwo)
	tracker.MarkFrontendRemoved(extensionTestFEPodOne)
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() FE intents error = %v", err)
	}
	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get intent-bearing DorisCluster error = %v", err)
	}
	var persisted map[string]string
	if err := json.Unmarshal([]byte(stored.Annotations[AnnotationFrontendDropIntent]), &persisted); err != nil {
		t.Fatalf("decode persisted FE drop intent error = %v", err)
	}
	want := map[string]string{
		extensionTestFEPodOne: frontendDropPhaseRemoved,
		extensionTestFEPodTwo: frontendDropPhasePending,
	}
	if !reflect.DeepEqual(persisted, want) {
		t.Errorf("persisted FE drop intents = %#v, want %#v", persisted, want)
	}
	if got := stored.Annotations["example.com/unrelated"]; got != extensionTestPreservedValue {
		t.Errorf("unrelated annotation = %q, want %q", got, extensionTestPreservedValue)
	}

	tracker.clearFrontendDropIntent(extensionTestFEPodOne)
	tracker.clearFrontendDropIntent(extensionTestFEPodTwo)
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() after clearing FE intents error = %v", err)
	}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get cleared DorisCluster error = %v", err)
	}
	if _, exists := stored.Annotations[AnnotationFrontendDropIntent]; exists {
		t.Errorf("annotation %q still exists after clearing all intents", AnnotationFrontendDropIntent)
	}
}

func TestAnnotationDecommissionTrackerClearsFrontendIntentOnlyAfterStatefulSetConverges(t *testing.T) {
	ctx := context.Background()
	encoded, err := json.Marshal(map[string]string{
		extensionTestFEPodOne:   frontendDropPhasePending,
		extensionTestFEPodTwo:   frontendDropPhaseRemoved,
		extensionTestFEPodThree: frontendDropPhasePending,
	})
	if err != nil {
		t.Fatalf("json.Marshal(frontend drop phases) error = %v", err)
	}
	cluster := extensionTestCluster()
	cluster.Annotations = map[string]string{AnnotationFrontendDropIntent: string(encoded)}
	statefulSet := extensionTestStatefulSet(constants.ComponentTypeFE, extensionTestHotRoleGroup, 3)
	k8sClient := extensionTestClient(t, cluster, statefulSet)
	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}

	if err := tracker.ClearConvergedFrontendDrops(ctx); err != nil {
		t.Fatalf("ClearConvergedFrontendDrops() before scale-down error = %v", err)
	}
	if got := tracker.PendingFrontendDropPods(); !reflect.DeepEqual(
		got,
		[]string{extensionTestFEPodOne, extensionTestFEPodTwo, extensionTestFEPodThree},
	) {
		t.Fatalf("frontend latches before StatefulSet convergence = %#v, want all pods", got)
	}

	statefulSet.Spec.Replicas = extensionTestReplicas(2)
	if err := k8sClient.Update(ctx, statefulSet); err != nil {
		t.Fatalf("update converged StatefulSet error = %v", err)
	}
	if err := tracker.ClearConvergedFrontendDrops(ctx); err != nil {
		t.Fatalf("ClearConvergedFrontendDrops() after scale-down error = %v", err)
	}
	if got := tracker.PendingFrontendDropPods(); !reflect.DeepEqual(
		got,
		[]string{extensionTestFEPodOne, extensionTestFEPodThree},
	) {
		t.Fatalf("frontend latches after StatefulSet convergence = %#v, want retained ordinal and unresolved pending phase", got)
	}
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() converged frontend latches error = %v", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get converged DorisCluster error = %v", err)
	}
	var phases map[string]string
	if err := json.Unmarshal([]byte(stored.Annotations[AnnotationFrontendDropIntent]), &phases); err != nil {
		t.Fatalf("decode converged frontend phases error = %v", err)
	}
	want := map[string]string{
		extensionTestFEPodOne:   frontendDropPhasePending,
		extensionTestFEPodThree: frontendDropPhasePending,
	}
	if !reflect.DeepEqual(phases, want) {
		t.Errorf("persisted frontend phases after convergence = %#v, want %#v", phases, want)
	}
}

func TestAnnotationDecommissionTrackerRejectsUnknownFrontendDropPhase(t *testing.T) {
	cluster := extensionTestCluster()
	cluster.Annotations = map[string]string{
		AnnotationFrontendDropIntent: `{"doris-fe-hot-2":"unknown"}`,
	}

	if _, err := newAnnotationDecommissionTracker(cluster, extensionTestClient(t, cluster)); err == nil {
		t.Fatal("newAnnotationDecommissionTracker() error = nil, want unknown phase rejection")
	}
}

func TestAnnotationDecommissionTrackerRejectsStaleSpecBeforePersistingFEDropIntent(t *testing.T) {
	ctx := context.Background()
	staleCluster := extensionTestCluster()
	staleCluster.Generation = 1
	currentCluster := staleCluster.DeepCopy()
	currentCluster.Generation = 2
	k8sClient := extensionTestClient(t, currentCluster)

	tracker, err := newAnnotationDecommissionTracker(staleCluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}
	tracker.RecordFrontendDropIntent(extensionTestFEPodTwo)
	err = tracker.Persist(ctx)
	if err == nil {
		t.Fatal("Persist() error = nil, want stale-generation retry")
	}
	if !opcommon.IsRequeueAfterError(err) {
		t.Fatalf("Persist() error = %v, want RequeueAfterError", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(currentCluster), stored); err != nil {
		t.Fatalf("get current DorisCluster error = %v", err)
	}
	if _, exists := stored.Annotations[AnnotationFrontendDropIntent]; exists {
		t.Errorf("stale reconcile persisted %q on the current spec", AnnotationFrontendDropIntent)
	}
}

func TestSafeZeroEvidenceLifecycleAndInvalidation(t *testing.T) {
	ctx := context.Background()
	encoded, err := json.Marshal(map[string]bool{
		"be/hot": true,
		"fe/old": true,
	})
	if err != nil {
		t.Fatalf("json.Marshal(safe zeros) error = %v", err)
	}
	cluster := extensionTestCluster()
	cluster.Spec.Backend = &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
		"hot": {Replicas: extensionTestReplicas(1)},
	}}
	cluster.Annotations = map[string]string{AnnotationSafelyScaledToZero: string(encoded)}
	k8sClient := extensionTestClient(t, cluster)

	tracker, err := newAnnotationDecommissionTracker(cluster, k8sClient)
	if err != nil {
		t.Fatalf("newAnnotationDecommissionTracker() error = %v", err)
	}
	tracker.ClearEvidenceForRunningGroups(cluster)
	tracker.MarkSafeZero(constants.ComponentTypeBE, "cold")
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() error = %v", err)
	}

	stored := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), stored); err != nil {
		t.Fatalf("get persisted DorisCluster error = %v", err)
	}
	got, err := decodeSafeZeroRoleGroups(stored.Annotations)
	if err != nil {
		t.Fatalf("decodeSafeZeroRoleGroups() error = %v", err)
	}
	want := map[string]bool{"fe/old": true, "be/cold": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("safe zero evidence = %#v, want %#v", got, want)
	}

	tracker.ClearSafeZero(constants.ComponentTypeFE, "old")
	tracker.ClearSafeZero(constants.ComponentTypeBE, "cold")
	if err := tracker.Persist(ctx); err != nil {
		t.Fatalf("Persist() after clearing error = %v", err)
	}
	cleared := &dorisv1alpha1.DorisCluster{}
	if err := k8sClient.Get(ctx, crclient.ObjectKeyFromObject(cluster), cleared); err != nil {
		t.Fatalf("get cleared DorisCluster error = %v", err)
	}
	if _, exists := cleared.Annotations[AnnotationSafelyScaledToZero]; exists {
		t.Errorf("annotation %q still exists after clearing", AnnotationSafelyScaledToZero)
	}
}

func extensionTestCluster() *dorisv1alpha1.DorisCluster {
	return &dorisv1alpha1.DorisCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: dorisv1alpha1.GroupVersion.String(),
			Kind:       "DorisCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      extensionTestClusterName,
			Namespace: extensionTestNamespace,
			UID:       extensionTestClusterUID,
		},
	}
}

func extensionTestKeptRole() *dorisv1alpha1.RoleSpec {
	return &dorisv1alpha1.RoleSpec{
		RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
			"kept": {
				Replicas: extensionTestReplicas(1),
			},
		},
	}
}

func extensionTestReplicas(replicas int32) *int32 {
	return &replicas
}

func extensionTestStatefulSet(
	component constants.ComponentType,
	roleGroupName string,
	replicas int32,
) *appsv1.StatefulSet {
	controller := true
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: strings.Join(
				[]string{extensionTestClusterName, string(component), roleGroupName},
				"-",
			),
			Namespace: extensionTestNamespace,
			Labels: map[string]string{
				opconstant.LabelKubernetesInstance:  extensionTestClusterName,
				opconstant.LabelKubernetesComponent: string(component),
				opconstant.LabelKubernetesRoleGroup: roleGroupName,
				opconstant.LabelKubernetesManagedBy: legacyManagedByValue,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: dorisv1alpha1.GroupVersion.String(),
				Kind:       "DorisCluster",
				Name:       extensionTestClusterName,
				UID:        extensionTestClusterUID,
				Controller: &controller,
			}},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: extensionTestReplicas(replicas),
		},
	}
}

func extensionTestClient(t *testing.T, objects ...crclient.Object) crclient.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps/v1 to test scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core/v1 to test scheme: %v", err)
	}
	if err := dorisv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Doris API to test scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
}

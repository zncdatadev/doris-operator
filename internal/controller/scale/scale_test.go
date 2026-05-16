/*
Copyright 2025 zncdatadev.

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
	"testing"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
)

func intPtr(v int32) *int32 { return &v }

const (
	testRoleGroupDefault = "default"
	testFEPod0           = "fe-0"
	testFEPod1           = "fe-1"
	testFEPod2           = "fe-2"
	testBEPod0           = "be-0"
	testBEPod1           = "be-1"
	testBEPod2           = "be-2"
	testPod0             = "pod-0"
	testPod1             = "pod-1"
	testPod2             = "pod-2"
	testPod3             = "pod-3"
	testBrokerPod0       = "broker-0"
)

func TestGetEffectiveReplicas(t *testing.T) {
	tests := []struct {
		name     string
		roleSpec *dorisv1alpha1.RoleSpec
		want     int32
	}{
		{
			name:     "nil role spec",
			roleSpec: nil,
			want:     0,
		},
		{
			name: "single role group with replicas",
			roleSpec: &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					testRoleGroupDefault: {Replicas: intPtr(3)},
				},
			},
			want: 3,
		},
		{
			name: "multiple role groups",
			roleSpec: &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					testRoleGroupDefault: {Replicas: intPtr(2)},
					"extra":              {Replicas: intPtr(1)},
				},
			},
			want: 3,
		},
		{
			name: "nil replicas defaults to 0",
			roleSpec: &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					testRoleGroupDefault: {},
				},
			},
			want: 0,
		},
		{
			name:     "empty role groups",
			roleSpec: &dorisv1alpha1.RoleSpec{},
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetEffectiveReplicas(tt.roleSpec)
			if got != tt.want {
				t.Errorf("GetEffectiveReplicas() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestComputeScaleActions(t *testing.T) {
	tests := []struct {
		name          string
		spec          *dorisv1alpha1.DorisClusterSpec
		replicaStates map[constants.ComponentType]*ReplicaState
		wantLen       int
		wantUps       int
		wantDowns     int
	}{
		{
			name: "no scale needed - current equals desired",
			spec: &dorisv1alpha1.DorisClusterSpec{
				Frontend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(3)},
					},
				},
				Backend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(3)},
					},
				},
			},
			replicaStates: map[constants.ComponentType]*ReplicaState{
				constants.ComponentTypeFE: {
					Component:       constants.ComponentTypeFE,
					SpecReplicas:    3,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testFEPod0, testFEPod1, testFEPod2},
				},
				constants.ComponentTypeBE: {
					Component:       constants.ComponentTypeBE,
					SpecReplicas:    3,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testBEPod0, testBEPod1, testBEPod2},
				},
			},
			wantLen:   2, // both components present => 2 actions (no-op but included)
			wantUps:   0,
			wantDowns: 0,
		},
		{
			name: "scale up FE and BE",
			spec: &dorisv1alpha1.DorisClusterSpec{
				Frontend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(5)},
					},
				},
				Backend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(4)},
					},
				},
			},
			replicaStates: map[constants.ComponentType]*ReplicaState{
				constants.ComponentTypeFE: {
					Component:       constants.ComponentTypeFE,
					SpecReplicas:    5,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testFEPod0, testFEPod1, testFEPod2},
				},
				constants.ComponentTypeBE: {
					Component:       constants.ComponentTypeBE,
					SpecReplicas:    4,
					CurrentReplicas: 2,
					ReadyReplicas:   2,
					PodNames:        []string{testBEPod0, testBEPod1},
				},
			},
			wantLen:   2,
			wantUps:   2,
			wantDowns: 0,
		},
		{
			name: "scale down BE",
			spec: &dorisv1alpha1.DorisClusterSpec{
				Frontend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(3)},
					},
				},
				Backend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(1)},
					},
				},
			},
			replicaStates: map[constants.ComponentType]*ReplicaState{
				constants.ComponentTypeFE: {
					Component:       constants.ComponentTypeFE,
					SpecReplicas:    3,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testFEPod0, testFEPod1, testFEPod2},
				},
				constants.ComponentTypeBE: {
					Component:       constants.ComponentTypeBE,
					SpecReplicas:    1,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testBEPod0, testBEPod1, testBEPod2},
				},
			},
			wantLen:   2, // FE (no-op) + BE (scale-down)
			wantUps:   0,
			wantDowns: 1,
		},
		{
			name: "scale down to zero replicas",
			spec: &dorisv1alpha1.DorisClusterSpec{
				Frontend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(0)},
					},
				},
				Backend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(3)},
					},
				},
			},
			replicaStates: map[constants.ComponentType]*ReplicaState{
				constants.ComponentTypeFE: {
					Component:       constants.ComponentTypeFE,
					SpecReplicas:    0,
					CurrentReplicas: 1,
					ReadyReplicas:   1,
					PodNames:        []string{testFEPod0},
				},
				constants.ComponentTypeBE: {
					Component:       constants.ComponentTypeBE,
					SpecReplicas:    3,
					CurrentReplicas: 3,
					ReadyReplicas:   3,
					PodNames:        []string{testBEPod0, testBEPod1, testBEPod2},
				},
			},
			wantLen:   2, // FE (no-op) + BE (scale-down)
			wantUps:   0,
			wantDowns: 1,
		},
		{
			name: "component not in replicaStates",
			spec: &dorisv1alpha1.DorisClusterSpec{
				Frontend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(1)},
					},
				},
				Backend: &dorisv1alpha1.RoleSpec{
					RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
						testRoleGroupDefault: {Replicas: intPtr(1)},
					},
				},
			},
			replicaStates: map[constants.ComponentType]*ReplicaState{
				// Only FE, no BE
				constants.ComponentTypeFE: {
					Component:       constants.ComponentTypeFE,
					SpecReplicas:    1,
					CurrentReplicas: 1,
					ReadyReplicas:   1,
					PodNames:        []string{testFEPod0},
				},
			},
			wantLen:   1, // FE present in states (no-op), BE skipped (not in states)
			wantUps:   0,
			wantDowns: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actions := ComputeScaleActions(tt.spec, tt.replicaStates)
			if len(actions) != tt.wantLen {
				t.Errorf("ComputeScaleActions() returned %d actions, want %d", len(actions), tt.wantLen)
				return
			}

			ups, downs := 0, 0
			for _, a := range actions {
				if a.IsScaleUp() {
					ups++
				}
				if a.IsScaleDown() {
					downs++
				}
			}
			if ups != tt.wantUps {
				t.Errorf("ComputeScaleActions() scale-ups = %d, want %d", ups, tt.wantUps)
			}
			if downs != tt.wantDowns {
				t.Errorf("ComputeScaleActions() scale-downs = %d, want %d", downs, tt.wantDowns)
			}
		})
	}
}

func TestGetPodsToRemove(t *testing.T) {
	tests := []struct {
		name          string
		podNames      []string
		current       int32
		desired       int32
		wantRemoveLen int
		wantRemoved   []string
	}{
		{
			name:          "scale down by 1",
			podNames:      []string{testPod0, testPod1, testPod2},
			current:       3,
			desired:       2,
			wantRemoveLen: 1,
			wantRemoved:   []string{testPod2},
		},
		{
			name:          "scale down by 2",
			podNames:      []string{testPod0, testPod1, testPod2, testPod3},
			current:       4,
			desired:       2,
			wantRemoveLen: 2,
			wantRemoved:   []string{testPod2, testPod3},
		},
		{
			name:          "scale down to zero",
			podNames:      []string{testPod0},
			current:       1,
			desired:       0,
			wantRemoveLen: 1,
			wantRemoved:   []string{testPod0},
		},
		{
			name:          "no scale down",
			podNames:      []string{testPod0, testPod1},
			current:       2,
			desired:       2,
			wantRemoveLen: 0,
			wantRemoved:   nil,
		},
		{
			name:          "scale up",
			podNames:      []string{testPod0},
			current:       1,
			desired:       3,
			wantRemoveLen: 0,
			wantRemoved:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getPodsToRemove(tt.podNames, tt.current, tt.desired)
			if len(got) != tt.wantRemoveLen {
				t.Errorf("getPodsToRemove() len = %d, want %d", len(got), tt.wantRemoveLen)
				return
			}
			for i, pod := range got {
				if i < len(tt.wantRemoved) && pod != tt.wantRemoved[i] {
					t.Errorf("getPodsToRemove()[%d] = %q, want %q", i, pod, tt.wantRemoved[i])
				}
			}
		})
	}
}

func TestGetBEStrategy(t *testing.T) {
	tests := []struct {
		name string
		spec *dorisv1alpha1.DorisClusterSpec
		want string
	}{
		{
			name: "nil cluster config returns default",
			spec: &dorisv1alpha1.DorisClusterSpec{},
			want: StrategyDecommission,
		},
		{
			name: "explicit decommission",
			spec: &dorisv1alpha1.DorisClusterSpec{
				ClusterConfig: &dorisv1alpha1.ClusterConfigSpec{
					ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{
						BackendStrategy: StrategyDecommission,
					},
				},
			},
			want: StrategyDecommission,
		},
		{
			name: "explicit force-drop",
			spec: &dorisv1alpha1.DorisClusterSpec{
				ClusterConfig: &dorisv1alpha1.ClusterConfigSpec{
					ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{
						BackendStrategy: StrategyForceDrop,
					},
				},
			},
			want: StrategyForceDrop,
		},
		{
			name: "empty strategy returns default",
			spec: &dorisv1alpha1.DorisClusterSpec{
				ClusterConfig: &dorisv1alpha1.ClusterConfigSpec{
					ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{},
				},
			},
			want: StrategyDecommission,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getBEStrategy(tt.spec)
			if got != tt.want {
				t.Errorf("getBEStrategy() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetFEStrategy(t *testing.T) {
	tests := []struct {
		name string
		spec *dorisv1alpha1.DorisClusterSpec
		want string
	}{
		{
			name: "nil cluster config returns default",
			spec: &dorisv1alpha1.DorisClusterSpec{},
			want: StrategyDropObserver,
		},
		{
			name: "explicit drop-observer",
			spec: &dorisv1alpha1.DorisClusterSpec{
				ClusterConfig: &dorisv1alpha1.ClusterConfigSpec{
					ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{
						FrontendStrategy: StrategyDropObserver,
					},
				},
			},
			want: StrategyDropObserver,
		},
		{
			name: "empty strategy returns default",
			spec: &dorisv1alpha1.DorisClusterSpec{
				ClusterConfig: &dorisv1alpha1.ClusterConfigSpec{
					ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{},
				},
			},
			want: StrategyDropObserver,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getFEStrategy(tt.spec)
			if got != tt.want {
				t.Errorf("getFEStrategy() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUpdateClusterStatus(t *testing.T) {
	tests := []struct {
		name            string
		beStatuses      []BENodeStatus
		feStatuses      []FENodeStatus
		brokerStatuses  []BrokerNodeStatus
		wantBENodes     int
		wantFENodes     int
		wantBrokerNodes int
	}{
		{
			name:            "nil statuses do not overwrite",
			beStatuses:      nil,
			feStatuses:      nil,
			brokerStatuses:  nil,
			wantBENodes:     0,
			wantFENodes:     0,
			wantBrokerNodes: 0,
		},
		{
			name: "populates BE nodes",
			beStatuses: []BENodeStatus{
				{PodName: testBEPod0, Host: testBEPod0, Alive: true, Decommission: false},
				{PodName: testBEPod1, Host: testBEPod1, Alive: true, Decommission: true, TabletNum: 5},
			},
			feStatuses:      nil,
			brokerStatuses:  nil,
			wantBENodes:     2,
			wantFENodes:     0,
			wantBrokerNodes: 0,
		},
		{
			name:       "populates FE nodes",
			beStatuses: nil,
			feStatuses: []FENodeStatus{
				{PodName: testFEPod0, Host: testFEPod0, Role: "FOLLOWER", Alive: true},
			},
			brokerStatuses:  nil,
			wantBENodes:     0,
			wantFENodes:     1,
			wantBrokerNodes: 0,
		},
		{
			name:       "populates broker nodes",
			beStatuses: nil,
			feStatuses: nil,
			brokerStatuses: []BrokerNodeStatus{
				{PodName: testBrokerPod0, Host: testBrokerPod0, Alive: true},
			},
			wantBENodes:     0,
			wantFENodes:     0,
			wantBrokerNodes: 1,
		},
		{
			name: "populates all",
			beStatuses: []BENodeStatus{
				{PodName: testBEPod0, Host: testBEPod0, Alive: true},
			},
			feStatuses: []FENodeStatus{
				{PodName: testFEPod0, Host: testFEPod0, Role: "MASTER", Alive: true},
			},
			brokerStatuses: []BrokerNodeStatus{
				{PodName: testBrokerPod0, Host: testBrokerPod0, Alive: true},
			},
			wantBENodes:     1,
			wantFENodes:     1,
			wantBrokerNodes: 1,
		},
		{
			name: "decommission BE gets Decommissioning phase",
			beStatuses: []BENodeStatus{
				{PodName: testBEPod0, Host: testBEPod0, Alive: true, Decommission: true},
			},
			feStatuses:      nil,
			brokerStatuses:  nil,
			wantBENodes:     1,
			wantFENodes:     0,
			wantBrokerNodes: 0,
		},
		{
			name: "nil cluster status does not panic",
			beStatuses: []BENodeStatus{
				{PodName: testBEPod0, Alive: true},
			},
			feStatuses:      nil,
			brokerStatuses:  nil,
			wantBENodes:     0, // nil clusterStatus returns early
			wantFENodes:     0,
			wantBrokerNodes: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var status dorisv1alpha1.DorisClusterStatus
			if tt.name == "nil cluster status does not panic" {
				UpdateClusterStatus(nil, tt.beStatuses, tt.feStatuses, tt.brokerStatuses)
				return
			}

			UpdateClusterStatus(&status, tt.beStatuses, tt.feStatuses, tt.brokerStatuses)

			if len(status.BackendNodes) != tt.wantBENodes {
				t.Errorf("BackendNodes len = %d, want %d", len(status.BackendNodes), tt.wantBENodes)
			}
			if len(status.FrontendNodes) != tt.wantFENodes {
				t.Errorf("FrontendNodes len = %d, want %d", len(status.FrontendNodes), tt.wantFENodes)
			}
			if len(status.BrokerNodes) != tt.wantBrokerNodes {
				t.Errorf("BrokerNodes len = %d, want %d", len(status.BrokerNodes), tt.wantBrokerNodes)
			}
		})
	}
}

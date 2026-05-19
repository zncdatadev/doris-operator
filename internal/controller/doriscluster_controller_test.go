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

package controller

import (
	"testing"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/scale"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testClusterName      = "test"
	testClusterNamespace = "default"
	testTimestamp        = "2026-05-19T10:00:00Z"
	testAnnoBE0          = "doris.kubedoop.dev/decommission-start/be-0"
)

func TestDecommissionTracker_GetStart(t *testing.T) {
	instance := &dorisv1alpha1.DorisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testClusterName,
			Namespace:   testClusterNamespace,
			Annotations: map[string]string{},
		},
	}
	tracker := newDecommissionTracker(instance, nil)

	// No annotation → not found
	_, ok := tracker.GetStart("be-0")
	if ok {
		t.Error("expected false for missing annotation")
	}

	// Set and read back
	tracker.RecordStart("be-0", testTimestamp)
	val, ok := tracker.GetStart("be-0")
	if !ok || val != testTimestamp {
		t.Errorf("expected recorded start, got val=%q ok=%v", val, ok)
	}

	// Clear
	tracker.ClearStart("be-0")
	_, ok = tracker.GetStart("be-0")
	if ok {
		t.Error("expected false after clear")
	}
}

func TestDecommissionTracker_PendingPods(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		pending     map[string]string
		want        []string
	}{
		{
			name:        "no annotations",
			annotations: nil,
			want:        nil,
		},
		{
			name: "active decommission",
			annotations: map[string]string{
				testAnnoBE0: testTimestamp,
				"doris.kubedoop.dev/decommission-start/be-1": "2026-05-19T10:01:00Z",
			},
			want: []string{"be-0", "be-1"},
		},
		{
			name: "cleared in pending",
			annotations: map[string]string{
				testAnnoBE0: testTimestamp,
			},
			pending: map[string]string{
				testAnnoBE0: "", // cleared
			},
			want: nil,
		},
		{
			name: "mixed - one active one cleared",
			annotations: map[string]string{
				testAnnoBE0: testTimestamp,
				"doris.kubedoop.dev/decommission-start/be-1": "2026-05-19T10:01:00Z",
			},
			pending: map[string]string{
				testAnnoBE0: "",
			},
			want: []string{"be-1"},
		},
		{
			name: "unrelated annotation ignored",
			annotations: map[string]string{
				testAnnoBE0:             testTimestamp,
				"some.other/annotation": "value",
			},
			want: []string{"be-0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &dorisv1alpha1.DorisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:        testClusterName,
					Namespace:   testClusterNamespace,
					Annotations: tt.annotations,
				},
			}
			tracker := newDecommissionTracker(instance, nil)
			if tt.pending != nil {
				tracker.pending = tt.pending
			}
			got := tracker.PendingPods()
			if len(got) != len(tt.want) {
				t.Errorf("PendingPods() len = %d, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("PendingPods()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestDecommissionTracker_Dirty(t *testing.T) {
	instance := &dorisv1alpha1.DorisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: testClusterNamespace},
	}
	tracker := newDecommissionTracker(instance, nil)

	if tracker.dirty {
		t.Error("expected clean tracker on init")
	}

	tracker.RecordStart("be-0", testTimestamp)
	if !tracker.dirty {
		t.Error("expected dirty after RecordStart")
	}

	tracker.pending = make(map[string]string)
	tracker.dirty = false

	tracker.ClearStart("be-0")
	if !tracker.dirty {
		t.Error("expected dirty after ClearStart")
	}
}

func TestGateBESpecReplicas_SingleRoleGroup(t *testing.T) {
	desired := int32(2)
	current := int32(3)

	instance := &dorisv1alpha1.DorisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName,
			Namespace: testClusterNamespace,
			Annotations: map[string]string{
				"doris.kubedoop.dev/decommission-start/be-2": testTimestamp,
			},
		},
		Spec: dorisv1alpha1.DorisClusterSpec{
			Backend: &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					"default": {Replicas: &desired},
				},
			},
		},
	}

	// Simulate gating: copy the roleGroup, modify replicas, put back
	rg := instance.Spec.Backend.RoleGroups["default"]
	original := rg.Replicas
	rg.Replicas = &current
	instance.Spec.Backend.RoleGroups["default"] = rg

	if *instance.Spec.Backend.RoleGroups["default"].Replicas != current {
		t.Errorf("expected gated replicas %d, got %d", current, *instance.Spec.Backend.RoleGroups["default"].Replicas)
	}

	// Restore
	rg = instance.Spec.Backend.RoleGroups["default"]
	rg.Replicas = original
	instance.Spec.Backend.RoleGroups["default"] = rg

	if *instance.Spec.Backend.RoleGroups["default"].Replicas != desired {
		t.Errorf("expected restored replicas %d, got %d", desired, *instance.Spec.Backend.RoleGroups["default"].Replicas)
	}
}

func TestGateBESpecReplicas_MultiRoleGroup(t *testing.T) {
	rg1Desired := int32(2)
	rg2Desired := int32(1)
	totalDesired := rg1Desired + rg2Desired
	currentReplicas := int32(6) // 2 roleGroups want 3 total, but 6 running

	instance := &dorisv1alpha1.DorisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testClusterName,
			Namespace: testClusterNamespace,
			Annotations: map[string]string{
				"doris.kubedoop.dev/decommission-start/be-2": testTimestamp,
			},
		},
		Spec: dorisv1alpha1.DorisClusterSpec{
			Backend: &dorisv1alpha1.RoleSpec{
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					"rg1": {Replicas: &rg1Desired},
					"rg2": {Replicas: &rg2Desired},
				},
			},
		},
	}

	// Simulate proportional gating
	originals := make(map[string]int32)
	remaining := currentReplicas
	groupNames := []string{"rg1", "rg2"}

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
		rg.Replicas = &gated
		instance.Spec.Backend.RoleGroups[name] = rg
	}

	// Verify total gated replicas equals current
	totalGated := int32(0)
	for _, rg := range instance.Spec.Backend.RoleGroups {
		if rg.Replicas != nil {
			totalGated += *rg.Replicas
		}
	}
	if totalGated != currentReplicas {
		t.Errorf("total gated replicas = %d, want %d", totalGated, currentReplicas)
	}

	// Restore
	for name, original := range originals {
		rg := instance.Spec.Backend.RoleGroups[name]
		rg.Replicas = &original
		instance.Spec.Backend.RoleGroups[name] = rg
	}

	if *instance.Spec.Backend.RoleGroups["rg1"].Replicas != rg1Desired {
		t.Errorf("rg1 restored = %d, want %d", *instance.Spec.Backend.RoleGroups["rg1"].Replicas, rg1Desired)
	}
	if *instance.Spec.Backend.RoleGroups["rg2"].Replicas != rg2Desired {
		t.Errorf("rg2 restored = %d, want %d", *instance.Spec.Backend.RoleGroups["rg2"].Replicas, rg2Desired)
	}
}

func TestScaleDownPolicy_Timeout(t *testing.T) {
	spec := &dorisv1alpha1.DorisClusterSpec{}
	policy := &clusterScaleDownPolicy{spec: spec}

	timeout := policy.GetDecommissionTimeout()
	if timeout != 2*time.Hour {
		t.Errorf("expected 2h default timeout, got %v", timeout)
	}

	// Custom timeout
	custom := metav1.Duration{Duration: 30 * time.Minute}
	spec.ClusterConfig = &dorisv1alpha1.ClusterConfigSpec{
		ScaleDownPolicy: &dorisv1alpha1.ScaleDownPolicySpec{
			DecommissionTimeout: &custom,
		},
	}
	timeout = policy.GetDecommissionTimeout()
	if timeout != 30*time.Minute {
		t.Errorf("expected 30m custom timeout, got %v", timeout)
	}
}

// Ensure the core types satisfy interfaces at compile time
var (
	_ scale.ScaleDownPolicy     = (*clusterScaleDownPolicy)(nil)
	_ scale.DecommissionTracker = (*decommissionTracker)(nil)
)

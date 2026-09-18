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

package v1alpha1

import (
	"encoding/json"
	"reflect"
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const testDefaultRoleGroup = "default"

var (
	_ common.ClusterInterface               = (*DorisCluster)(nil)
	_ common.ClusterResource[*DorisCluster] = (*DorisCluster)(nil)
	_ reconciler.VectorAggregatorProvider   = (*DorisCluster)(nil)
)

func TestToGenericSpecMapsTypedRoles(t *testing.T) {
	roleTimeout := "45s"
	groupTimeout := "90s"
	replicas := int32(3)
	clusterOperation := &commonsv1alpha1.ClusterOperationSpec{}
	roleConfig := &commonsv1alpha1.RoleConfigSpec{}
	roleConfigSpec := &ConfigSpec{GracefulShutdownTimeout: &roleTimeout}
	groupConfigSpec := &ConfigSpec{GracefulShutdownTimeout: &groupTimeout}
	rolePodOverrides := &runtime.RawExtension{Raw: []byte(`{"spec":{"priorityClassName":"critical"}}`)}
	groupPodOverrides := &runtime.RawExtension{Raw: []byte(`{"spec":{"nodeName":"worker-1"}}`)}

	cluster := &DorisCluster{Spec: DorisClusterSpec{
		ClusterOperationSpec: clusterOperation,
		Frontend: &RoleSpec{
			Config:     roleConfigSpec,
			RoleConfig: roleConfig,
			OverridesSpec: &commonsv1alpha1.OverridesSpec{
				ConfigOverrides: map[string]map[string]string{"fe.conf": {"priority_networks": "10.0.0.0/8"}},
				EnvOverrides:    map[string]string{"JAVA_HOME": "/opt/java"},
				CliOverrides:    []string{"--console"},
				PodOverrides:    rolePodOverrides,
			},
			RoleGroups: map[string]RoleGroupSpec{
				testDefaultRoleGroup: {
					Replicas: &replicas,
					Config:   groupConfigSpec,
					OverridesSpec: &commonsv1alpha1.OverridesSpec{
						ConfigOverrides: map[string]map[string]string{"fe.conf": {"query_port": "9030"}},
						EnvOverrides:    map[string]string{"GROUP": testDefaultRoleGroup},
						CliOverrides:    []string{"--group"},
						PodOverrides:    groupPodOverrides,
					},
				},
			},
		},
		Backend: &RoleSpec{},
		Broker:  &RoleSpec{},
	}}

	got := cluster.GetSpec()
	if got.ClusterOperation != clusterOperation {
		t.Fatalf("ClusterOperation = %p, want original pointer %p", got.ClusterOperation, clusterOperation)
	}

	if len(got.Roles) != 3 {
		t.Fatalf("len(Roles) = %d, want 3: %#v", len(got.Roles), got.Roles)
	}
	for _, roleName := range []string{"fe", "be", "broker"} {
		if _, found := got.Roles[roleName]; !found {
			t.Errorf("Roles does not contain %q", roleName)
		}
	}

	frontend := got.Roles["fe"]
	if frontend.RoleConfig != roleConfig {
		t.Errorf("frontend RoleConfig = %p, want original pointer %p", frontend.RoleConfig, roleConfig)
	}
	if frontend.Config == nil || frontend.Config.GracefulShutdownTimeout == nil ||
		*frontend.Config.GracefulShutdownTimeout != roleTimeout {
		t.Errorf("frontend Config = %#v, want gracefulShutdownTimeout %q", frontend.Config, roleTimeout)
	}
	if !reflect.DeepEqual(frontend.ConfigOverrides, cluster.Spec.Frontend.ConfigOverrides) {
		t.Errorf("frontend ConfigOverrides = %#v, want %#v", frontend.ConfigOverrides, cluster.Spec.Frontend.ConfigOverrides)
	}
	if !reflect.DeepEqual(frontend.EnvOverrides, cluster.Spec.Frontend.EnvOverrides) {
		t.Errorf("frontend EnvOverrides = %#v, want %#v", frontend.EnvOverrides, cluster.Spec.Frontend.EnvOverrides)
	}
	if !reflect.DeepEqual(frontend.CliOverrides, cluster.Spec.Frontend.CliOverrides) {
		t.Errorf("frontend CliOverrides = %#v, want %#v", frontend.CliOverrides, cluster.Spec.Frontend.CliOverrides)
	}
	if frontend.PodOverrides != rolePodOverrides {
		t.Errorf("frontend PodOverrides = %p, want original pointer %p", frontend.PodOverrides, rolePodOverrides)
	}

	group, found := frontend.RoleGroups[testDefaultRoleGroup]
	if !found {
		t.Fatal("frontend RoleGroups does not contain default")
	}
	if group.Replicas != &replicas || *group.Replicas != replicas {
		t.Errorf("default replicas = %v, want original pointer to %d", group.Replicas, replicas)
	}
	if group.Config == nil || group.Config.GracefulShutdownTimeout == nil ||
		*group.Config.GracefulShutdownTimeout != groupTimeout {
		t.Errorf("default Config = %#v, want gracefulShutdownTimeout %q", group.Config, groupTimeout)
	}
	wantGroup := cluster.Spec.Frontend.RoleGroups[testDefaultRoleGroup]
	if !reflect.DeepEqual(group.ConfigOverrides, wantGroup.ConfigOverrides) {
		t.Errorf("default ConfigOverrides = %#v, want %#v", group.ConfigOverrides, wantGroup.ConfigOverrides)
	}
	if !reflect.DeepEqual(group.EnvOverrides, wantGroup.EnvOverrides) {
		t.Errorf("default EnvOverrides = %#v, want %#v", group.EnvOverrides, wantGroup.EnvOverrides)
	}
	if !reflect.DeepEqual(group.CliOverrides, wantGroup.CliOverrides) {
		t.Errorf("default CliOverrides = %#v, want %#v", group.CliOverrides, wantGroup.CliOverrides)
	}
	if group.PodOverrides != groupPodOverrides {
		t.Errorf("default PodOverrides = %p, want original pointer %p", group.PodOverrides, groupPodOverrides)
	}
}

func TestToGenericSpecOmitsUnsetRoles(t *testing.T) {
	got := (&DorisClusterSpec{Frontend: &RoleSpec{}}).ToGenericSpec()

	if len(got.Roles) != 1 {
		t.Fatalf("len(Roles) = %d, want 1: %#v", len(got.Roles), got.Roles)
	}
	if _, found := got.Roles["fe"]; !found {
		t.Error("Roles does not contain fe")
	}
	for _, roleName := range []string{"be", "broker"} {
		if _, found := got.Roles[roleName]; found {
			t.Errorf("Roles unexpectedly contains unset role %q", roleName)
		}
	}

	var nilSpec *DorisClusterSpec
	got = nilSpec.ToGenericSpec()
	if got == nil {
		t.Fatal("nil receiver returned nil GenericClusterSpec")
	}
	if got.Roles == nil || len(got.Roles) != 0 {
		t.Errorf("nil receiver Roles = %#v, want a non-nil empty map", got.Roles)
	}
}

func TestToGenericSpecKeepsLegacyEmptyStorageClassSemantics(t *testing.T) {
	emptyStorageClass := ""
	roleConfig := &ConfigSpec{Resources: &commonsv1alpha1.ResourcesSpec{
		Storage: &commonsv1alpha1.StorageResource{StorageClass: &emptyStorageClass},
	}}
	groupConfig := &ConfigSpec{Resources: roleConfig.Resources.DeepCopy()}
	spec := &DorisClusterSpec{Frontend: &RoleSpec{
		Config: roleConfig,
		RoleGroups: map[string]RoleGroupSpec{
			testDefaultRoleGroup: {Config: groupConfig},
		},
	}}

	frontend := spec.ToGenericSpec().Roles["fe"]
	if frontend.Config.Resources.Storage.StorageClass != nil {
		t.Errorf("role storageClass = %#v, want nil for the legacy empty-string contract", frontend.Config.Resources.Storage.StorageClass)
	}
	group := frontend.RoleGroups[testDefaultRoleGroup]
	if group.Config.Resources.Storage.StorageClass != nil {
		t.Errorf("role-group storageClass = %#v, want nil for the legacy empty-string contract", group.Config.Resources.Storage.StorageClass)
	}
	if roleConfig.Resources.Storage.StorageClass == nil || *roleConfig.Resources.Storage.StorageClass != "" {
		t.Error("ToGenericSpec mutated the typed Doris role config")
	}
	if groupConfig.Resources.Storage.StorageClass == nil || *groupConfig.Resources.Storage.StorageClass != "" {
		t.Error("ToGenericSpec mutated the typed Doris role-group config")
	}
}

func TestToGenericSpecKeepsLegacyGracefulShutdownBoundaryValues(t *testing.T) {
	emptyTimeout := ""
	zeroTimeout := "0s"
	spec := &DorisClusterSpec{
		Frontend: &RoleSpec{
			Config: &ConfigSpec{GracefulShutdownTimeout: &emptyTimeout},
			RoleGroups: map[string]RoleGroupSpec{
				testDefaultRoleGroup: {Config: &ConfigSpec{GracefulShutdownTimeout: &zeroTimeout}},
			},
		},
	}

	frontend := spec.ToGenericSpec().Roles["fe"]
	if frontend.Config.GracefulShutdownTimeout != nil {
		t.Errorf("role gracefulShutdownTimeout = %#v, want nil for the legacy empty-string contract", frontend.Config.GracefulShutdownTimeout)
	}
	groupTimeout := frontend.RoleGroups[testDefaultRoleGroup].Config.GracefulShutdownTimeout
	if groupTimeout == nil || *groupTimeout != zeroTimeout {
		t.Errorf("role-group gracefulShutdownTimeout = %#v, want %q", groupTimeout, zeroTimeout)
	}
	if spec.Frontend.Config.GracefulShutdownTimeout == nil || *spec.Frontend.Config.GracefulShutdownTimeout != "" {
		t.Error("ToGenericSpec mutated the typed Doris role timeout")
	}
}

func TestToGenericSpecPreservesRoleDeclaredImageResolution(t *testing.T) {
	pullPolicy := corev1.PullAlways
	spec := &DorisClusterSpec{Image: &ImageSpec{
		Repo:            "registry.example/doris",
		ProductVersion:  "3.0.5",
		KubedoopVersion: "1.2.3",
		PullPolicy:      &pullPolicy,
		PullSecretName:  "registry-credentials",
	}}

	got := spec.ToGenericSpec().Image
	if got == nil {
		t.Fatal("Image = nil, want projected transport settings")
	}
	if got.Repo != "" || got.ProductVersion != "" || got.KubedoopVersion != "" {
		t.Fatalf(
			"structured generic image fields = repo %q, productVersion %q, kubedoopVersion %q; want all empty",
			got.Repo,
			got.ProductVersion,
			got.KubedoopVersion,
		)
	}
	if got.PullPolicy != pullPolicy {
		t.Errorf("PullPolicy = %q, want %q", got.PullPolicy, pullPolicy)
	}
	if got.PullSecretName != "registry-credentials" {
		t.Errorf("PullSecretName = %q, want registry-credentials", got.PullSecretName)
	}

	const roleDeclaredImage = "apache/doris:fe-3.0.5"
	resolved, err := got.ResolveImage("", commonsv1alpha1.ImageSpec{Custom: roleDeclaredImage})
	if err != nil {
		t.Fatalf("ResolveImage() error = %v", err)
	}
	if resolved != roleDeclaredImage {
		t.Errorf("ResolveImage() = %q, want handler-declared %q", resolved, roleDeclaredImage)
	}

	spec.Image.Custom = "private.example/doris:custom"
	got = spec.ToGenericSpec().Image
	resolved, err = got.ResolveImage("", commonsv1alpha1.ImageSpec{Custom: roleDeclaredImage})
	if err != nil {
		t.Fatalf("ResolveImage() with custom image error = %v", err)
	}
	if resolved != spec.Image.Custom {
		t.Errorf("ResolveImage() with custom image = %q, want %q", resolved, spec.Image.Custom)
	}

	if (&DorisClusterSpec{}).ToGenericSpec().Image != nil {
		t.Error("nil Doris image projected to a non-nil generic image")
	}
}

func TestGetStatusReturnsEmbeddedStatus(t *testing.T) {
	cluster := &DorisCluster{Status: DorisClusterStatus{
		AuthInitialized: true,
		FrontendNodes:   []NodeStatus{{Name: "fe-0"}},
	}}

	got := cluster.GetStatus()
	if got != &cluster.Status.GenericClusterStatus {
		t.Fatalf("GetStatus() = %p, want embedded status pointer %p", got, &cluster.Status.GenericClusterStatus)
	}

	got.ObservedGeneration = 7
	got.RoleGroups = map[string][]string{"fe": {testDefaultRoleGroup}}
	if cluster.Status.ObservedGeneration != 7 {
		t.Errorf("ObservedGeneration = %d, want 7", cluster.Status.ObservedGeneration)
	}
	if !reflect.DeepEqual(cluster.Status.RoleGroups, map[string][]string{"fe": {testDefaultRoleGroup}}) {
		t.Errorf("RoleGroups = %#v, want framework mutation to be visible on the CR", cluster.Status.RoleGroups)
	}
	if !cluster.Status.AuthInitialized || len(cluster.Status.FrontendNodes) != 1 {
		t.Errorf("Doris-specific status changed: %#v", cluster.Status)
	}
}

func TestDorisClusterStatusRetainsLegacyJSONFields(t *testing.T) {
	status := DorisClusterStatus{
		GenericClusterStatus: commonsv1alpha1.GenericClusterStatus{
			Conditions:         []metav1.Condition{{Type: "Available", Status: metav1.ConditionTrue}},
			RoleGroups:         map[string][]string{"fe": {testDefaultRoleGroup}},
			ObservedGeneration: 12,
		},
		URLs:            []StatusURL{{Name: "Doris UI", URL: "https://doris.example"}},
		Generation:      11,
		Name:            "example",
		Type:            "DorisCluster",
		AuthInitialized: true,
		FrontendNodes:   []NodeStatus{{Name: "fe-0", Alive: true}},
		BackendNodes:    []NodeStatus{{Name: "be-0", Alive: true}},
		BrokerNodes:     []NodeStatus{{Name: "broker-0", Alive: true}},
	}

	payload, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("json.Unmarshal() fields error = %v", err)
	}
	for _, field := range []string{
		"conditions",
		"roleGroups",
		"observedGeneration",
		"urls",
		"generation",
		"name",
		"type",
		"authInitialized",
		"frontendNodes",
		"backendNodes",
		"brokerNodes",
	} {
		if _, found := fields[field]; !found {
			t.Errorf("marshaled status does not contain %q: %s", field, payload)
		}
	}
	if _, found := fields["genericClusterStatus"]; found {
		t.Errorf("embedded generic status was nested instead of flattened: %s", payload)
	}

	var roundTripped DorisClusterStatus
	if err := json.Unmarshal(payload, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal() status error = %v", err)
	}
	if roundTripped.Generation != status.Generation || roundTripped.Name != status.Name || roundTripped.Type != status.Type {
		t.Errorf("legacy identity fields after round trip = (%d, %q, %q), want (%d, %q, %q)",
			roundTripped.Generation,
			roundTripped.Name,
			roundTripped.Type,
			status.Generation,
			status.Name,
			status.Type,
		)
	}
	if !reflect.DeepEqual(roundTripped.URLs, status.URLs) {
		t.Errorf("URLs after round trip = %#v, want %#v", roundTripped.URLs, status.URLs)
	}
	if roundTripped.ObservedGeneration != status.ObservedGeneration || !reflect.DeepEqual(roundTripped.RoleGroups, status.RoleGroups) {
		t.Errorf("generic status after round trip = %#v, want %#v", roundTripped.GenericClusterStatus, status.GenericClusterStatus)
	}
}

func TestVectorAggregatorConfigMapName(t *testing.T) {
	configuredName := "vector-aggregator-discovery"
	tests := []struct {
		name    string
		cluster *DorisCluster
		want    string
	}{
		{name: "nil cluster"},
		{name: "nil cluster config", cluster: &DorisCluster{}},
		{
			name:    "nil config map name",
			cluster: &DorisCluster{Spec: DorisClusterSpec{ClusterConfig: &ClusterConfigSpec{}}},
		},
		{
			name: "configured",
			cluster: &DorisCluster{Spec: DorisClusterSpec{ClusterConfig: &ClusterConfigSpec{
				VectorAggregatorConfigMapName: &configuredName,
			}}},
			want: configuredName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cluster.VectorAggregatorConfigMapName(); got != tt.want {
				t.Errorf("VectorAggregatorConfigMapName() = %q, want %q", got, tt.want)
			}
		})
	}
}

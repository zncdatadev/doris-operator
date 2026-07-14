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

package fe

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

func newTestRoleGroupInfo() *reconciler.RoleGroupInfo {
	return &reconciler.RoleGroupInfo{
		RoleInfo: reconciler.RoleInfo{
			ClusterInfo: reconciler.ClusterInfo{
				GVK: &metav1.GroupVersionKind{
					Group:   dorisv1alpha1.GroupVersion.Group,
					Version: dorisv1alpha1.GroupVersion.Version,
					Kind:    "DorisCluster",
				},
				ClusterName: "test",
			},
			RoleName: string(constants.ComponentTypeFE),
		},
		RoleGroupName: "default",
	}
}

// Regression test: spec.clusterConfig is optional in the CRD, so a minimal
// DorisCluster without it must not panic when building the FE ConfigMap.
func TestNewFEConfigMapReconciler_OptionalClusterConfig(t *testing.T) {
	tests := []struct {
		name          string
		clusterConfig *dorisv1alpha1.ClusterConfigSpec
	}{
		{
			name:          "nil clusterConfig",
			clusterConfig: nil,
		},
		{
			name:          "clusterConfig without authentication",
			clusterConfig: &dorisv1alpha1.ClusterConfigSpec{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dorisCluster := &dorisv1alpha1.DorisCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
				Spec: dorisv1alpha1.DorisClusterSpec{
					ClusterConfig: tt.clusterConfig,
				},
			}
			cli := client.NewClient(nil, dorisCluster)

			rec := NewFEConfigMapReconciler(
				context.Background(),
				cli,
				newTestRoleGroupInfo(),
				nil,
				nil,
				dorisCluster,
			)

			obj, err := rec.GetBuilder().Build(context.Background())
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			cm, ok := obj.(*corev1.ConfigMap)
			if !ok {
				t.Fatalf("Build() returned %T, want *corev1.ConfigMap", obj)
			}

			feConf, ok := cm.Data[string(constants.FEConfigFilename)]
			if !ok {
				t.Fatalf("ConfigMap missing %s", constants.FEConfigFilename)
			}
			if !strings.Contains(feConf, "enable_fqdn_mode=true") {
				t.Errorf("fe.conf missing default config, got:\n%s", feConf)
			}
			if _, ok := cm.Data[constants.LDAPConfigFilename]; ok {
				t.Errorf("ConfigMap should not contain %s without authentication config", constants.LDAPConfigFilename)
			}
		})
	}
}

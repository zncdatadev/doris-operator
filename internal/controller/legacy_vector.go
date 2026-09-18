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

package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opvector "github.com/zncdatadev/operator-go/pkg/vector"
	corev1 "k8s.io/api/core/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// preserveLegacyVectorConfig retains the Gen 2 ConfigMap contract without opting Doris into the
// v0.13 managed sidecar. Existing users can keep supplying their own Vector container through
// podOverrides and consume the same vector.yaml key.
func preserveLegacyVectorConfig(
	ctx context.Context,
	k8sClient crclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
	configMap *corev1.ConfigMap,
) error {
	config := buildCtx.RoleGroupSpec.GetConfig()
	if config == nil || !opvector.IsAgentEnabled(config.Logging) {
		return nil
	}
	aggregatorConfigMap := cluster.VectorAggregatorConfigMapName()
	if aggregatorConfigMap == "" {
		return fmt.Errorf(
			"build Doris %s/%s vector config: vectorAggregatorConfigMapName is required when the Vector agent is enabled",
			buildCtx.RoleName,
			buildCtx.RoleGroupName,
		)
	}
	aggregatorAddress, err := opvector.DiscoverAggregatorAddress(
		ctx,
		k8sClient,
		buildCtx.ClusterNamespace,
		aggregatorConfigMap,
	)
	if err != nil {
		return fmt.Errorf("build Doris %s/%s vector config: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}
	content, err := renderLegacyVectorConfig(map[string]any{
		"LogDir":                  strings.TrimRight(dorisLogDirectory, "/") + "/",
		"VectorAggregatorAddress": aggregatorAddress,
		"Namespace":               buildCtx.ClusterNamespace,
		"Cluster":                 buildCtx.ClusterName,
		"RoleName":                buildCtx.RoleName,
		"RoleGroupName":           buildCtx.RoleGroupName,
	})
	if err != nil {
		return fmt.Errorf("build Doris %s/%s vector config: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}
	if configMap.Data == nil {
		configMap.Data = make(map[string]string)
	}
	configMap.Data[opvector.VectorConfigFileName] = content
	return nil
}

func renderLegacyVectorConfig(data map[string]any) (string, error) {
	tmpl, err := template.New("legacy-vector.yaml").Parse(legacyVectorConfigTemplate)
	if err != nil {
		return "", fmt.Errorf("parse Gen 2 Vector template: %w", err)
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, data); err != nil {
		return "", fmt.Errorf("execute Gen 2 Vector template: %w", err)
	}
	// The v0.12.6 helper converted the template's indentation tabs to two spaces.
	return strings.ReplaceAll(output.String(), "\t", "  "), nil
}

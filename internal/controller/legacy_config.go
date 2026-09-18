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
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// buildLegacyConfigMapData preserves the Gen 2 ConfigMap contract. Product defaults are rendered
// as properties, while user configOverrides are recognized only through the historical
// whole-file shape configOverrides[filename][filename]. Other per-key entries were dormant in
// Gen 2 and must not suddenly alter ports or startup behavior during a framework upgrade.
func buildLegacyConfigMapData(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (map[string]string, error) {
	productConfig, err := resolveDorisProductConfig(ctx, k8sClient, cr, buildCtx.RoleName)
	if err != nil {
		return nil, err
	}

	data := make(map[string]string, len(productConfig))
	for filename, values := range productConfig {
		data[filename] = renderLegacyConfigFile(filename, values)
	}
	for filename, overrides := range legacyConfigOverrides(buildCtx) {
		if raw, wholeFile := overrides[filename]; wholeFile {
			data[filename] = raw
		}
	}
	return data, nil
}

func legacyConfigOverrides(buildCtx *reconciler.RoleGroupBuildContext) map[string]map[string]string {
	result := make(map[string]map[string]string)
	merge := func(overrides map[string]map[string]string) {
		for filename, values := range overrides {
			merged := result[filename]
			if merged == nil {
				merged = make(map[string]string, len(values))
				result[filename] = merged
			}
			for key, value := range values {
				merged[key] = value
			}
		}
	}

	if buildCtx.RoleSpec == nil {
		return result
	}
	merge(buildCtx.RoleSpec.ConfigOverrides)
	if group, exists := buildCtx.RoleSpec.RoleGroups[buildCtx.RoleGroupName]; exists {
		merge(group.ConfigOverrides)
	}
	return result
}

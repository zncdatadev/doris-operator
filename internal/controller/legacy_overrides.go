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
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opconfig "github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

// legacyCompatibleBuildContext restores the Gen 2 layer fold at the boundary where
// operator-go v0.13 consumes it. Gen 2 recursively merged objects and appended every JSON array;
// v0.13 deliberately replaces several higher-level fields. Existing Doris CRs retain the old
// contract while the generic reconciler still owns all resource construction and application.
func legacyCompatibleBuildContext(
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupBuildContext, *corev1.PodTemplateSpec, error) {
	legacy := *buildCtx
	mergedConfig := opconfig.NewMergedConfig()
	if buildCtx.MergedConfig != nil {
		// A shallow struct copy is intentional: maps and logging are read-only during BuildResources,
		// while keeping Logging avoids the v0.13 Clone method's deliberately narrow copy surface.
		copyOfMerged := *buildCtx.MergedConfig
		mergedConfig = &copyOfMerged
	}

	userPodOverrides, err := legacyUserPodOverrides(buildCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("merge role and role-group podOverrides: %w", err)
	}
	// Base receives product-owned defaults only. User podOverrides are applied to the finished
	// Gen 2-compatible PodTemplate below, after the framework's mount-invariant validation.
	mergedConfig.PodOverrides = dorisProductPodOverrides(buildCtx.RoleName)
	mergedConfig.CliArgs = nil
	legacy.MergedConfig = mergedConfig

	affinity, configured, err := legacyMergedAffinity(buildCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("merge role and role-group affinity: %w", err)
	}
	if configured {
		group := buildCtx.RoleGroupSpec.DeepCopy()
		if group.Config == nil {
			group.Config = &commonsv1alpha1.RoleGroupConfigSpec{}
		}
		group.Config.Affinity = affinity
		legacy.RoleGroupSpec = *group
	}

	return &legacy, userPodOverrides, nil
}

func legacyUserPodOverrides(
	buildCtx *reconciler.RoleGroupBuildContext,
) (*corev1.PodTemplateSpec, error) {
	if buildCtx.RoleSpec == nil {
		return nil, nil
	}
	var groupOverrides *k8sruntime.RawExtension
	if group, exists := buildCtx.RoleSpec.RoleGroups[buildCtx.RoleGroupName]; exists {
		groupOverrides = group.PodOverrides
	}
	merged, configured, err := mergeLegacyRawObjects(buildCtx.RoleSpec.PodOverrides, groupOverrides)
	if err != nil || !configured {
		return nil, err
	}
	var podTemplate corev1.PodTemplateSpec
	if err := json.Unmarshal(merged, &podTemplate); err != nil {
		return nil, fmt.Errorf("decode merged pod template: %w", err)
	}
	return &podTemplate, nil
}

func legacyMergedAffinity(
	buildCtx *reconciler.RoleGroupBuildContext,
) (*k8sruntime.RawExtension, bool, error) {
	if buildCtx.RoleSpec == nil {
		return nil, false, nil
	}
	var roleAffinity *k8sruntime.RawExtension
	if buildCtx.RoleSpec.Config != nil {
		roleAffinity = buildCtx.RoleSpec.Config.Affinity
	}
	var groupAffinity *k8sruntime.RawExtension
	if group, exists := buildCtx.RoleSpec.RoleGroups[buildCtx.RoleGroupName]; exists && group.Config != nil {
		groupAffinity = group.Config.Affinity
	}
	merged, configured, err := mergeLegacyRawObjects(roleAffinity, groupAffinity)
	if err != nil || !configured {
		return nil, configured, err
	}
	return &k8sruntime.RawExtension{Raw: merged}, true, nil
}

func mergeLegacyRawObjects(
	original *k8sruntime.RawExtension,
	override *k8sruntime.RawExtension,
) ([]byte, bool, error) {
	if original == nil && override == nil {
		return nil, false, nil
	}
	originalObject, err := decodeRawObject(original)
	if err != nil {
		return nil, true, fmt.Errorf("decode role layer: %w", err)
	}
	overrideObject, err := decodeRawObject(override)
	if err != nil {
		return nil, true, fmt.Errorf("decode role-group layer: %w", err)
	}
	merged := mergeLegacyJSONMaps(originalObject, overrideObject)
	encoded, err := json.Marshal(merged)
	if err != nil {
		return nil, true, err
	}
	return encoded, true, nil
}

func decodeRawObject(extension *k8sruntime.RawExtension) (map[string]any, error) {
	if extension == nil {
		return map[string]any{}, nil
	}
	encoded, err := json.Marshal(extension)
	if err != nil {
		return nil, err
	}
	if string(encoded) == "null" {
		return map[string]any{}, nil
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		return nil, err
	}
	if object == nil {
		object = map[string]any{}
	}
	return object, nil
}

func mergeLegacyJSONMaps(original, override map[string]any) map[string]any {
	for key, overrideValue := range override {
		originalValue, exists := original[key]
		if !exists {
			original[key] = overrideValue
			continue
		}
		switch typedOriginal := originalValue.(type) {
		case map[string]any:
			if typedOverride, ok := overrideValue.(map[string]any); ok {
				original[key] = mergeLegacyJSONMaps(typedOriginal, typedOverride)
			} else {
				original[key] = overrideValue
			}
		case []any:
			if typedOverride, ok := overrideValue.([]any); ok {
				original[key] = append(typedOriginal, typedOverride...)
			} else {
				original[key] = overrideValue
			}
		default:
			original[key] = overrideValue
		}
	}
	return original
}

// applyLegacyPodOverrides reproduces operator-go v0.12.6 MergeObjectWithStrategic exactly,
// including its second JSON merge-patch pass. That detail makes explicit empty slices behave as
// "not specified", as they did for existing Doris CRs.
func applyLegacyPodOverrides(
	base *corev1.PodTemplateSpec,
	override *corev1.PodTemplateSpec,
) (*corev1.PodTemplateSpec, error) {
	if override == nil {
		return base.DeepCopy(), nil
	}
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	overrideJSON, err := json.Marshal(override)
	if err != nil {
		return nil, err
	}
	strategicResult, err := strategicpatch.StrategicMergePatch(baseJSON, overrideJSON, corev1.PodTemplateSpec{})
	if err != nil {
		return nil, err
	}
	mergedJSON, err := jsonpatch.MergePatch(baseJSON, strategicResult)
	if err != nil {
		return nil, err
	}
	var merged corev1.PodTemplateSpec
	if err := json.Unmarshal(mergedJSON, &merged); err != nil {
		return nil, err
	}
	return &merged, nil
}

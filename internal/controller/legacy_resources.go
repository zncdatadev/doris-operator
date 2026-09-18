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
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// v0.12 treated a present CPU or memory object as a complete value and used
// an empty object to remove the corresponding product default. operator-go
// v0.13 deliberately folds empty structural leaves as "inherit". Doris keeps
// its existing v1alpha1 wire contract here so upgrades do not unexpectedly add
// multi-core requests and roll every StatefulSet.
func preserveLegacyContainerResources(
	cluster *dorisv1alpha1.DorisCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
	container *corev1.Container,
) {
	resources, configured := legacyUserResources(cluster, buildCtx.RoleName, buildCtx.RoleGroupName)
	if !configured {
		return
	}
	if container.Resources.Requests == nil {
		container.Resources.Requests = corev1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}

	if resources.CPU == nil && resources.Memory == nil {
		delete(container.Resources.Requests, corev1.ResourceCPU)
		delete(container.Resources.Limits, corev1.ResourceCPU)
		delete(container.Resources.Requests, corev1.ResourceMemory)
		delete(container.Resources.Limits, corev1.ResourceMemory)
		return
	}

	if resources.CPU != nil {
		minIsZero := resources.CPU.Min == nil || resources.CPU.Min.IsZero()
		maxIsZero := resources.CPU.Max == nil || resources.CPU.Max.IsZero()
		if minIsZero && maxIsZero {
			delete(container.Resources.Requests, corev1.ResourceCPU)
			delete(container.Resources.Limits, corev1.ResourceCPU)
		} else {
			zero := resource.MustParse("0")
			if resources.CPU.Min == nil {
				container.Resources.Requests[corev1.ResourceCPU] = zero
			} else {
				container.Resources.Requests[corev1.ResourceCPU] = *resources.CPU.Min
			}
			if resources.CPU.Max == nil {
				container.Resources.Limits[corev1.ResourceCPU] = zero
			} else {
				container.Resources.Limits[corev1.ResourceCPU] = *resources.CPU.Max
			}
		}
	}

	if resources.Memory != nil {
		if resources.Memory.Limit == nil || resources.Memory.Limit.IsZero() {
			delete(container.Resources.Requests, corev1.ResourceMemory)
			delete(container.Resources.Limits, corev1.ResourceMemory)
		} else {
			container.Resources.Requests[corev1.ResourceMemory] = *resources.Memory.Limit
			container.Resources.Limits[corev1.ResourceMemory] = *resources.Memory.Limit
		}
	}
}

// legacyUserResources recreates the old JSON-object merge only for the
// container-resource branches whose value-to-pointer migration changed their
// meaning. An explicitly present group CPU or memory block replaces that
// complete block; an empty group resources object leaves the role layer intact.
func legacyUserResources(
	cluster *dorisv1alpha1.DorisCluster,
	roleName string,
	roleGroupName string,
) (*commonsv1alpha1.ResourcesSpec, bool) {
	role := dorisRole(cluster, roleName)
	if role == nil {
		return nil, false
	}

	roleResources := configResources(role.Config)
	roleGroup, exists := role.RoleGroups[roleGroupName]
	if !exists {
		if roleResources == nil {
			return nil, false
		}
		return roleResources.DeepCopy(), true
	}
	groupResources := configResources(roleGroup.Config)

	switch {
	case roleResources == nil && groupResources == nil:
		return nil, false
	case roleResources == nil:
		return groupResources.DeepCopy(), true
	case groupResources == nil:
		return roleResources.DeepCopy(), true
	}

	merged := roleResources.DeepCopy()
	if groupResources.CPU != nil {
		merged.CPU = groupResources.CPU.DeepCopy()
	}
	if groupResources.Memory != nil {
		merged.Memory = groupResources.Memory.DeepCopy()
	}
	if groupResources.Storage != nil {
		merged.Storage = groupResources.Storage.DeepCopy()
	}
	return merged, true
}

func dorisRole(cluster *dorisv1alpha1.DorisCluster, roleName string) *dorisv1alpha1.RoleSpec {
	if cluster == nil {
		return nil
	}
	switch roleName {
	case "fe":
		return cluster.Spec.Frontend
	case "be":
		return cluster.Spec.Backend
	case "broker":
		return cluster.Spec.Broker
	default:
		return nil
	}
}

func configResources(config *dorisv1alpha1.ConfigSpec) *commonsv1alpha1.ResourcesSpec {
	if config == nil {
		return nil
	}
	return config.Resources
}

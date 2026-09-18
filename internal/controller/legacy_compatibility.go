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
	"fmt"
	"sort"
	"strings"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opconstant "github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// The pre-Gen3 controller put managed-by=doris.kubedoop.dev in the immutable
// StatefulSet selector. That value cannot be changed in place to operator-go,
// which is the value the v0.13 live orphan detector and pod-failure scan use.
// Restore the ledger from strictly owned legacy slots so the normal cleaner can
// still reclaim them by deterministic name when status was lost or pruned.
func restoreLegacyRoleGroupLedger(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	if cluster.UID == "" {
		return nil
	}

	listOptions := []ctrlclient.ListOption{
		ctrlclient.InNamespace(cluster.Namespace),
		ctrlclient.MatchingLabels{
			opconstant.LabelKubernetesName:      legacyAppName,
			opconstant.LabelKubernetesInstance:  cluster.Name,
			opconstant.LabelKubernetesManagedBy: legacyManagedBy,
		},
	}

	statefulSets := &appsv1.StatefulSetList{}
	if err := k8sClient.List(ctx, statefulSets, listOptions...); err != nil {
		return fmt.Errorf("restore legacy Doris role-group ledger from StatefulSets: %w", err)
	}
	configMaps := &corev1.ConfigMapList{}
	if err := k8sClient.List(ctx, configMaps, listOptions...); err != nil {
		return fmt.Errorf("restore legacy Doris role-group ledger from ConfigMaps: %w", err)
	}
	services := &corev1.ServiceList{}
	if err := k8sClient.List(ctx, services, listOptions...); err != nil {
		return fmt.Errorf("restore legacy Doris role-group ledger from Services: %w", err)
	}

	type coordinate struct {
		role  string
		group string
	}
	coordinates := make(map[coordinate]struct{})
	for i := range statefulSets.Items {
		role, group, ok, err := ownedLegacyRoleGroupSlot(&statefulSets.Items[i], cluster)
		if err != nil {
			return err
		}
		if ok {
			coordinates[coordinate{role: role, group: group}] = struct{}{}
		}
	}
	for i := range configMaps.Items {
		role, group, ok, err := ownedLegacyRoleGroupSlot(&configMaps.Items[i], cluster)
		if err != nil {
			return err
		}
		if ok {
			coordinates[coordinate{role: role, group: group}] = struct{}{}
		}
	}
	for i := range services.Items {
		role, group, ok, err := ownedLegacyRoleGroupSlot(&services.Items[i], cluster)
		if err != nil {
			return err
		}
		if ok {
			coordinates[coordinate{role: role, group: group}] = struct{}{}
		}
	}

	ordered := make([]coordinate, 0, len(coordinates))
	for item := range coordinates {
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].role != ordered[j].role {
			return ordered[i].role < ordered[j].role
		}
		return ordered[i].group < ordered[j].group
	})
	for _, item := range ordered {
		cluster.Status.SetRoleGroup(item.role, item.group)
	}

	return nil
}

func ownedLegacyRoleGroupSlot(
	object metav1.Object,
	cluster *dorisv1alpha1.DorisCluster,
) (string, string, bool, error) {
	if !metav1.IsControlledBy(object, cluster) {
		return "", "", false, nil
	}
	labels := object.GetLabels()
	role := labels[opconstant.LabelKubernetesComponent]
	group := labels[opconstant.LabelKubernetesRoleGroup]
	if !isDorisRole(role) || group == "" {
		return "", "", false, nil
	}

	canonicalName := reconciler.RoleGroupResourceName(cluster.Name, role, group)
	legacyName := cluster.Name + "-" + role + "-" + group
	if canonicalName != legacyName &&
		(object.GetName() == legacyName || object.GetName() == legacyName+"-metrics") {
		return "", "", false, fmt.Errorf(
			"cannot migrate legacy Doris role group %s/%s: existing resource %q uses the pre-v0.13 name %q, but operator-go v0.13 addresses that slot as %q; shorten the cluster or role-group name before upgrading",
			role,
			group,
			object.GetName(),
			legacyName,
			canonicalName,
		)
	}
	if object.GetName() != canonicalName && object.GetName() != canonicalName+"-metrics" {
		return "", "", false, nil
	}
	return role, group, true, nil
}

func isDorisRole(role string) bool {
	switch role {
	case "fe", "be", "broker":
		return true
	default:
		return false
	}
}

const (
	legacyReasonCrashLoopBackOff = "CrashLoopBackOff"
	legacyReasonImagePullBackOff = "ImagePullBackOff"
)

var legacyStuckContainerReasons = map[string]struct{}{
	legacyReasonCrashLoopBackOff: {},
	legacyReasonImagePullBackOff: {},
	"ErrImagePull":               {},
	"InvalidImageName":           {},
	"CreateContainerConfigError": {},
	"CreateContainerError":       {},
	"RunContainerError":          {},
}

// HealthManager checks only managed-by=operator-go pods. Run the same failure
// classification for legacy-labelled Doris pods after the generic health pass,
// overriding Degraded only when a concrete pod failure exists. A later healthy
// pass is cleared by HealthManager before this extension executes again.
func reportLegacyPodFailures(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	pods := &corev1.PodList{}
	if err := k8sClient.List(ctx, pods,
		ctrlclient.InNamespace(cluster.Namespace),
		ctrlclient.MatchingLabels{
			opconstant.LabelKubernetesName:      legacyAppName,
			opconstant.LabelKubernetesInstance:  cluster.Name,
			opconstant.LabelKubernetesManagedBy: legacyManagedBy,
		},
	); err != nil {
		return fmt.Errorf("check legacy-labelled Doris pods: %w", err)
	}

	failures := make([]string, 0)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if reason := legacyPodFailureReason(pod); reason != "" {
			failures = append(failures, fmt.Sprintf("%s (%s)", pod.Name, reason))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	if cluster.Status.IsDegraded() {
		// Workload-unreadable, service-health, and reconcile errors carry more
		// specific diagnostics than this compatibility scan. Never replace them.
		return nil
	}

	sort.Strings(failures)
	cluster.Status.SetDegraded(
		true,
		commonsv1alpha1.ReasonPodFailure,
		"Failing pods: "+strings.Join(failures, ", "),
	)
	return nil
}

func legacyPodFailureReason(pod *corev1.Pod) string {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == corev1.PodReasonUnschedulable {
			return corev1.PodReasonUnschedulable
		}
	}

	for _, statuses := range [][]corev1.ContainerStatus{
		pod.Status.InitContainerStatuses,
		pod.Status.ContainerStatuses,
	} {
		for i := range statuses {
			waiting := statuses[i].State.Waiting
			if waiting == nil {
				continue
			}
			if _, stuck := legacyStuckContainerReasons[waiting.Reason]; stuck {
				return waiting.Reason
			}
		}
	}

	return ""
}

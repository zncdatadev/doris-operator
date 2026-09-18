package scale

import (
	"fmt"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	appsv1 "k8s.io/api/apps/v1"
)

const (
	// defaultDecommissionTimeout is the default timeout for BE decommission
	defaultDecommissionTimeout = 2 * time.Hour

	// StrategyDecommission is the default BE scale-down strategy
	StrategyDecommission = "decommission"
	// StrategyForceDrop is the force-drop BE scale-down strategy
	StrategyForceDrop = "force-drop"
	// StrategyDropObserver is the default FE scale-down strategy
	StrategyDropObserver = "drop-observer"

	// AnnotationDecommissionStart is the single DorisCluster annotation used to track
	// BE decommission state. Its value is a JSON object mapping pod names to either
	// the pre-mutation "pending" intent marker or an RFC3339 start timestamp.
	// Keeping the pod name in the value makes the annotation key a valid qualified
	// name with exactly one slash.
	AnnotationDecommissionStart = "doris.kubedoop.dev/decommission-start"
	// AnnotationFrontendDropIntent records FE observer pods whose DROP OBSERVER
	// operation has been authorized by an optimistic-lock patch. Its JSON object
	// maps pod names to an explicit lifecycle phase. The durable safety latch stays
	// in place after Doris removes the node and is cleared only after the live
	// StatefulSet replica target no longer retains that pod ordinal.
	AnnotationFrontendDropIntent = "doris.kubedoop.dev/frontend-drop-intent"
	// AnnotationSafelyScaledToZero records FE/BE role groups whose Doris topology
	// entries were removed before their StatefulSet reached zero replicas. This
	// distinguishes a safe scale-down from the identical StatefulSet state caused
	// by spec.clusterOperation.stopped.
	AnnotationSafelyScaledToZero = "doris.kubedoop.dev/safely-scaled-to-zero"
)

// ScaleAction represents a scale operation to perform
type ScaleAction struct {
	// Component is the component type (fe, be, broker)
	Component constants.ComponentType
	// RoleGroup is the specific role group represented by StatefulSetNames.
	RoleGroup string
	// CurrentReplicas is the current number of replicas (from StatefulSet)
	CurrentReplicas int32
	// DesiredReplicas is the target number of replicas (from CR spec)
	DesiredReplicas int32
	// PodsToRemove lists pods to be safely removed (empty = scale up or no action)
	PodsToRemove []string
	// Strategy is the scale-down strategy for this component
	Strategy string
	// StatefulSetNames lists the StatefulSet names involved in this scale action.
	// Used for STS replica gating during decommission.
	StatefulSetNames []string
}

// IsScaleDown returns true if this is a scale-down action
func (a *ScaleAction) IsScaleDown() bool {
	return a.DesiredReplicas < a.CurrentReplicas
}

// getPodsToRemove returns pod names for the pods that should be removed during scale-down.
// StatefulSet scale-down removes highest ordinals first.
func getPodsToRemove(podNames []string, currentReplicas, desiredReplicas int32) []string {
	removeCount := currentReplicas - desiredReplicas
	if removeCount <= 0 || len(podNames) == 0 {
		return nil
	}

	// PodNames are assumed sorted by ordinal in ascending order (e.g., fe-default-0, fe-default-1, fe-default-2).
	// We remove from the highest ordinal
	startIdx := len(podNames) - int(removeCount)
	if startIdx < 0 {
		startIdx = 0
	}
	return podNames[startIdx:]
}

// getBEStrategy returns the scale-down strategy for BE
func getBEStrategy(spec *dorisv1alpha1.DorisClusterSpec) string {
	if spec.ClusterConfig != nil && spec.ClusterConfig.ScaleDownPolicy != nil {
		strategy := spec.ClusterConfig.ScaleDownPolicy.BackendStrategy
		if strategy != "" {
			return strategy
		}
	}
	return StrategyDecommission
}

// getFEStrategy returns the scale-down strategy for FE
func getFEStrategy(spec *dorisv1alpha1.DorisClusterSpec) string {
	if spec.ClusterConfig != nil && spec.ClusterConfig.ScaleDownPolicy != nil {
		strategy := spec.ClusterConfig.ScaleDownPolicy.FrontendStrategy
		if strategy != "" {
			return strategy
		}
	}
	return StrategyDropObserver
}

// GetDecommissionTimeout returns the decommission timeout duration.
func GetDecommissionTimeout(spec *dorisv1alpha1.DorisClusterSpec) time.Duration {
	if spec.ClusterConfig != nil && spec.ClusterConfig.ScaleDownPolicy != nil &&
		spec.ClusterConfig.ScaleDownPolicy.DecommissionTimeout != nil {
		return spec.ClusterConfig.ScaleDownPolicy.DecommissionTimeout.Duration
	}
	return defaultDecommissionTimeout
}

// GetStatefulSetReplicas extracts replica count from a StatefulSet
func GetStatefulSetReplicas(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas != nil {
		return *sts.Spec.Replicas
	}
	return 1 // StatefulSet default
}

// GetStatefulSetPodNames returns sorted pod names from a StatefulSet based on spec.replicas.
func GetStatefulSetPodNames(sts *appsv1.StatefulSet) []string {
	replicas := GetStatefulSetReplicas(sts)
	names := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, i))
	}
	return names
}

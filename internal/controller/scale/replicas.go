package scale

import (
	"fmt"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	// defaultDecommissionTimeout is the default timeout for BE decommission
	defaultDecommissionTimeout = "2h"

	// StrategyDecommission is the default BE scale-down strategy
	StrategyDecommission = "decommission"
	// StrategyForceDrop is the force-drop BE scale-down strategy
	StrategyForceDrop = "force-drop"
	// StrategyDropObserver is the default FE scale-down strategy
	StrategyDropObserver = "drop-observer"
)

// ScaleAction represents a scale operation to perform
type ScaleAction struct {
	// Component is the component type (fe, be, broker)
	Component constants.ComponentType
	// CurrentReplicas is the current number of replicas (from StatefulSet)
	CurrentReplicas int32
	// DesiredReplicas is the target number of replicas (from CR spec)
	DesiredReplicas int32
	// PodsToRemove lists pods to be safely removed (empty = scale up or no action)
	PodsToRemove []string
	// Strategy is the scale-down strategy for this component
	Strategy string
}

// IsScaleDown returns true if this is a scale-down action
func (a *ScaleAction) IsScaleDown() bool {
	return a.DesiredReplicas < a.CurrentReplicas
}

// IsScaleUp returns true if this is a scale-up action
func (a *ScaleAction) IsScaleUp() bool {
	return a.DesiredReplicas > a.CurrentReplicas
}

// ReplicaState holds the current replica information for a component
type ReplicaState struct {
	// Component type
	Component constants.ComponentType
	// DesiredReplicas from StatefulSet spec (what the STS is targeting)
	SpecReplicas int32
	// CurrentReplicas from StatefulSet status (actual number of pods currently running)
	CurrentReplicas int32
	// Ready replicas from StatefulSet status
	ReadyReplicas int32
	// Pod names currently running (sorted by ordinal)
	PodNames []string
}

// GetEffectiveReplicas resolves the effective replica count for a component.
// It sums replicas across all role groups.
func GetEffectiveReplicas(roleSpec *dorisv1alpha1.RoleSpec) int32 {
	if roleSpec == nil {
		return 0
	}

	var total int32
	for _, rg := range roleSpec.RoleGroups {
		if rg.Replicas != nil {
			total += *rg.Replicas
		}
	}

	return total
}

// ComputeScaleActions compares desired replicas against current StatefulSet state
// and returns scale actions for each component.
//
// Limitation: When multiple roleGroups exist for a component, scale-down pod selection
// may not correctly target the roleGroup being scaled. Scale-down is only safely supported
// for single-roleGroup deployments. Multi-roleGroup scale support requires per-StatefulSet
// action computation which is not yet implemented.
func ComputeScaleActions(
	spec *dorisv1alpha1.DorisClusterSpec,
	replicaStates map[constants.ComponentType]*ReplicaState,
) []ScaleAction {
	var actions []ScaleAction

	components := []struct {
		ct       constants.ComponentType
		roleSpec *dorisv1alpha1.RoleSpec
		strategy string
	}{
		{constants.ComponentTypeFE, spec.Frontend, getFEStrategy(spec)},
		{constants.ComponentTypeBE, spec.Backend, getBEStrategy(spec)},
	}

	for _, comp := range components {
		state, ok := replicaStates[comp.ct]
		if !ok || state == nil {
			continue
		}

		desired := GetEffectiveReplicas(comp.roleSpec)

		action := ScaleAction{
			Component:       comp.ct,
			CurrentReplicas: state.CurrentReplicas,
			DesiredReplicas: desired,
			Strategy:        comp.strategy,
		}

		if action.IsScaleDown() {
			// Determine which pods to remove (highest ordinals first)
			action.PodsToRemove = getPodsToRemove(state.PodNames, state.CurrentReplicas, desired)
		}

		actions = append(actions, action)
	}

	return actions
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
// TODO: Implement timeout tracking in BE scale-down to automatically fallback to force-drop.
func GetDecommissionTimeout(spec *dorisv1alpha1.DorisClusterSpec) string {
	if spec.ClusterConfig != nil && spec.ClusterConfig.ScaleDownPolicy != nil &&
		spec.ClusterConfig.ScaleDownPolicy.DecommissionTimeout != nil {
		return spec.ClusterConfig.ScaleDownPolicy.DecommissionTimeout.Duration.String()
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

// GetStatefulSetPodNames returns sorted pod names from a StatefulSet based on actual running replicas.
func GetStatefulSetPodNames(sts *appsv1.StatefulSet) []string {
	replicas := sts.Status.Replicas // Use actual running count, not spec
	names := make([]string, 0, int(replicas))
	for i := int32(0); i < replicas; i++ {
		names = append(names, fmt.Sprintf("%s-%d", sts.Name, i))
	}
	return names
}

// ComponentRole returns the Doris role name for a component
func ComponentRole(ct constants.ComponentType) string {
	switch ct {
	case constants.ComponentTypeFE:
		return "fe"
	case constants.ComponentTypeBE:
		return "be"
	case constants.ComponentTypeBroker:
		return "broker"
	default:
		return string(ct)
	}
}

// IsPodOwnerRef checks if a pod belongs to a given StatefulSet
func IsPodOwnerRef(pod corev1.Pod, stsName string) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "StatefulSet" && ref.Name == stsName {
			return true
		}
	}
	return false
}

package scale

import (
	"context"
	"fmt"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var scaleManagerLogger = ctrl.Log.WithName("scale-manager")

// ScaleManager orchestrates scale operations for Doris cluster components.
// It coordinates BE and FE scale-down through Doris MySQL protocol.
type ScaleManager struct {
	dorisClient *doris_client.DorisClient
	beManager   *BEScaleManager
	feManager   *FEScaleManager
}

// NewScaleManager creates a new ScaleManager
func NewScaleManager(dorisClient *doris_client.DorisClient) *ScaleManager {
	return &ScaleManager{
		dorisClient: dorisClient,
		beManager:   NewBEScaleManager(dorisClient),
		feManager:   NewFEScaleManager(dorisClient),
	}
}

// ScaleResult contains the result of a scale operation
type ScaleResult struct {
	// NeedRequeue is true if there's an in-progress operation that needs time
	NeedRequeue bool
	// RequeueAfter is the duration to wait before requeuing
	RequeueAfter time.Duration
	// CompletedRemovals lists pods that are ready for StatefulSet scale-down
	CompletedRemovals map[constants.ComponentType][]string
	// BEStatuses contains current BE node statuses
	BEStatuses []BENodeStatus
	// FEStatuses contains current FE node statuses
	FEStatuses []FENodeStatus
}

// ReconcileScale performs scale reconciliation for all components.
// It checks if scale-down is needed and coordinates safe decommission/drop operations.
func (m *ScaleManager) ReconcileScale(
	ctx context.Context,
	spec *dorisv1alpha1.DorisClusterSpec,
	stsMap map[constants.ComponentType]*appsv1.StatefulSet,
) (*ScaleResult, error) {
	result := &ScaleResult{
		CompletedRemovals: make(map[constants.ComponentType][]string),
	}

	// Build replica states from StatefulSets
	replicaStates := buildReplicaStates(stsMap)

	// Compute scale actions
	actions := ComputeScaleActions(spec, replicaStates)
	if len(actions) == 0 {
		scaleManagerLogger.V(1).Info("No scale actions needed")
		return result, nil
	}

	for _, action := range actions {
		scaleManagerLogger.Info("Processing scale action",
			"component", action.Component,
			"current", action.CurrentReplicas,
			"desired", action.DesiredReplicas,
			"strategy", action.Strategy)

		if action.IsScaleUp() {
			// Scale-up is handled by operator-go's StatefulSet reconciler
			// Nothing to do here
			continue
		}

		if action.IsScaleDown() {
			switch action.Component {
			case constants.ComponentTypeBE:
				beResult, err := m.beManager.ScaleDown(ctx, action)
				if err != nil {
					return nil, fmt.Errorf("BE scale-down failed: %w", err)
				}
				if len(beResult) > 0 {
					result.CompletedRemovals[constants.ComponentTypeBE] = beResult
				} else {
					// Decommission in progress, need to wait
					result.NeedRequeue = true
					result.RequeueAfter = 30 * time.Second
				}

			case constants.ComponentTypeFE:
				feResult, err := m.feManager.ScaleDown(ctx, action)
				if err != nil {
					return nil, fmt.Errorf("FE scale-down failed: %w", err)
				}
				if len(feResult) > 0 {
					result.CompletedRemovals[constants.ComponentTypeFE] = feResult
				}

			default:
				scaleManagerLogger.V(1).Info("No scale-down handler for component",
					"component", action.Component)
			}
		}
	}

	// Collect node statuses
	if _, ok := replicaStates[constants.ComponentTypeBE]; ok {
		beStatuses, err := m.beManager.GetBENodeStatuses(ctx, replicaStates[constants.ComponentTypeBE].PodNames)
		if err != nil {
			scaleManagerLogger.Error(err, "Failed to get BE node statuses")
		} else {
			result.BEStatuses = beStatuses
		}
	}

	if _, ok := replicaStates[constants.ComponentTypeFE]; ok {
		feStatuses, err := m.feManager.GetFENodeStatuses(ctx, replicaStates[constants.ComponentTypeFE].PodNames)
		if err != nil {
			scaleManagerLogger.Error(err, "Failed to get FE node statuses")
		} else {
			result.FEStatuses = feStatuses
		}
	}

	return result, nil
}

// Close closes the underlying Doris client connection
func (m *ScaleManager) Close() {
	if m.dorisClient != nil {
		_ = m.dorisClient.Close()
	}
}

// buildReplicaStates extracts replica states from StatefulSet map
func buildReplicaStates(stsMap map[constants.ComponentType]*appsv1.StatefulSet) map[constants.ComponentType]*ReplicaState {
	states := make(map[constants.ComponentType]*ReplicaState)
	for ct, sts := range stsMap {
		states[ct] = &ReplicaState{
			Component:     ct,
			SpecReplicas:  GetStatefulSetReplicas(sts),
			ReadyReplicas: sts.Status.ReadyReplicas,
			PodNames:      GetStatefulSetPodNames(sts, sts.Namespace),
		}
	}
	return states
}

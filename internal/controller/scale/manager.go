package scale

import (
	"context"
	"fmt"
	"strings"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
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
	// BrokerStatuses contains current Broker node statuses
	BrokerStatuses []BrokerNodeStatus
	// GatedReplicas maps STS names to their minimum replica count.
	// When a component has an in-progress scale-down (e.g., BE decommission not yet complete),
	// the corresponding STS replicas are gated to prevent premature pod deletion.
	// The controller should patch these STS to the gated replica count after reconciliation.
	GatedReplicas map[string]int32
}

// ReconcileScale performs scale reconciliation for all components.
// It checks if scale-down is needed and coordinates safe decommission/drop operations.
// Node statuses are collected every reconciliation regardless of scale actions.
func (m *ScaleManager) ReconcileScale(
	ctx context.Context,
	spec *dorisv1alpha1.DorisClusterSpec,
	replicaStates map[constants.ComponentType]*ReplicaState,
	scaleConfig ScaleConfig,
) (*ScaleResult, error) {
	result := &ScaleResult{
		CompletedRemovals: make(map[constants.ComponentType][]string),
		GatedReplicas:     make(map[string]int32),
	}

	// Compute and execute scale actions
	actions := ComputeScaleActions(spec, replicaStates)
	if len(actions) > 0 {
		for _, action := range actions {
			scaleManagerLogger.Info("Processing scale action",
				"component", action.Component,
				"current", action.CurrentReplicas,
				"desired", action.DesiredReplicas,
				"strategy", action.Strategy)

			if action.IsScaleUp() {
				// Scale-up is handled by operator-go's StatefulSet reconciler
				continue
			}

			if action.IsScaleDown() {
				switch action.Component {
				case constants.ComponentTypeBE:
					beResult, err := m.beManager.ScaleDown(ctx, action, scaleConfig)
					if err != nil {
						return nil, fmt.Errorf("BE scale-down failed: %w", err)
					}
					if len(beResult) > 0 {
						result.CompletedRemovals[constants.ComponentTypeBE] = beResult
					}
					// Requeue if not all pods are ready for removal
					if len(beResult) < len(action.PodsToRemove) {
						result.NeedRequeue = true
						result.RequeueAfter = 30 * time.Second
						// Gate: prevent STS from scaling down until decommission completes.
						// Set gated replicas to the current count so pods are not deleted prematurely.
						if state, ok := replicaStates[constants.ComponentTypeBE]; ok {
							for _, stsName := range action.StSNames {
								result.GatedReplicas[stsName] = state.CurrentReplicas
							}
						}
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
	} else {
		scaleManagerLogger.V(1).Info("No scale actions needed")
	}

	// Collect node statuses for all deployed components
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

	if _, ok := replicaStates[constants.ComponentTypeBroker]; ok {
		brokerPods := replicaStates[constants.ComponentTypeBroker].PodNames
		brokers, err := m.dorisClient.ShowBrokers(ctx)
		if err != nil {
			scaleManagerLogger.Error(err, "Failed to get Broker node statuses")
		} else {
			result.BrokerStatuses = buildBrokerNodeStatuses(brokerPods, brokers)
		}
	}

	return result, nil
}

// buildBrokerNodeStatuses maps K8s pod names to Doris broker info.
func buildBrokerNodeStatuses(podNames []string, brokers []doris_client.BrokerInfo) []BrokerNodeStatus {
	statuses := make([]BrokerNodeStatus, 0, len(podNames))
	for _, podName := range podNames {
		s := BrokerNodeStatus{PodName: podName}
		for _, bi := range brokers {
			// Match by substring or exact hostname
			if len(bi.Host) > 0 && (strings.Contains(bi.Host, podName) || bi.Host == podName) {
				s.Host = bi.Host
				s.Alive = bi.Alive
				break
			}
		}
		statuses = append(statuses, s)
	}
	return statuses
}

// BrokerNodeStatus represents the scale-relevant status of a Broker pod
type BrokerNodeStatus struct {
	PodName string
	Host    string
	Alive   bool
}

// Close closes the underlying Doris client connection
func (m *ScaleManager) Close() {
	if m.dorisClient != nil {
		_ = m.dorisClient.Close()
	}
}

package scale

import (
	"context"
	"fmt"

	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	ctrl "sigs.k8s.io/controller-runtime"
)

var beScaleLogger = ctrl.Log.WithName("scale-be")

// BEScaleManager handles BE scale-down operations
type BEScaleManager struct {
	client *doris_client.DorisClient
}

// NewBEScaleManager creates a new BE scale manager
func NewBEScaleManager(client *doris_client.DorisClient) *BEScaleManager {
	return &BEScaleManager{client: client}
}

// ScaleDown performs scale-down for BE nodes
// It returns the number of pods that have been decommissioned (ready for removal).
func (m *BEScaleManager) ScaleDown(ctx context.Context, action ScaleAction) ([]string, error) {
	if !action.IsScaleDown() {
		return nil, nil
	}

	if len(action.PodsToRemove) == 0 {
		return nil, fmt.Errorf("no pods to remove in scale-down action")
	}

	backends, err := m.client.ShowBackends(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query BE nodes: %w", err)
	}

	var readyForRemoval []string

	for _, podName := range action.PodsToRemove {
		be := doris_client.MatchPodToBackend(podName, backends)
		if be == nil {
			beScaleLogger.Info("BE node not found in Doris cluster, safe to remove",
				"pod", podName)
			readyForRemoval = append(readyForRemoval, podName)
			continue
		}

		switch action.Strategy {
		case "decommission":
			if be.Decommission {
				if doris_client.IsDecommissionComplete(*be) {
					beScaleLogger.Info("BE decommission complete, ready for removal",
						"pod", podName, "host", be.Host)
					readyForRemoval = append(readyForRemoval, podName)
				} else {
					beScaleLogger.Info("BE decommission in progress, waiting",
						"pod", podName, "host", be.Host, "tabletNum", be.TabletNum)
				}
			} else {
				// Start decommission
				beScaleLogger.Info("Starting BE decommission",
					"pod", podName, "host", be.Host, "port", be.Port)
				if err := m.client.DecommissionBackend(ctx, be.Host, be.Port); err != nil {
					return nil, fmt.Errorf("failed to decommission BE %s: %w", podName, err)
				}
			}

		case "force-drop":
			beScaleLogger.Info("Force dropping BE node",
				"pod", podName, "host", be.Host, "port", be.Port)
			if err := m.client.DropBackend(ctx, be.Host, be.Port); err != nil {
				return nil, fmt.Errorf("failed to drop BE %s: %w", podName, err)
			}
			readyForRemoval = append(readyForRemoval, podName)

		default:
			return nil, fmt.Errorf("unknown BE scale-down strategy: %s", action.Strategy)
		}
	}

	return readyForRemoval, nil
}

// IsDecommissioning checks if any BE node is currently being decommissioned
func (m *BEScaleManager) IsDecommissioning(ctx context.Context) (bool, error) {
	backends, err := m.client.ShowBackends(ctx)
	if err != nil {
		return false, err
	}

	for _, be := range backends {
		if be.Decommission && !doris_client.IsDecommissionComplete(be) {
			return true, nil
		}
	}
	return false, nil
}

// GetBENodeStatuses converts Doris BE node info to NodeStatus slice
func (m *BEScaleManager) GetBENodeStatuses(ctx context.Context, podNames []string) ([]BENodeStatus, error) {
	backends, err := m.client.ShowBackends(ctx)
	if err != nil {
		return nil, err
	}

	var statuses []BENodeStatus
	for _, podName := range podNames {
		be := doris_client.MatchPodToBackend(podName, backends)
		status := BENodeStatus{PodName: podName}
		if be != nil {
			status.Host = be.Host
			status.Alive = be.Alive
			status.Decommission = be.Decommission
			status.TabletNum = be.TabletNum
		} else {
			status.Alive = false
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

// BENodeStatus represents the scale-relevant status of a BE pod
type BENodeStatus struct {
	PodName      string
	Host         string
	Alive        bool
	Decommission bool
	TabletNum    int
}

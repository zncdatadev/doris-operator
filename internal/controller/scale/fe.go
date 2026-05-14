package scale

import (
	"context"
	"fmt"

	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	ctrl "sigs.k8s.io/controller-runtime"
)

var feScaleLogger = ctrl.Log.WithName("scale-fe")

// FEScaleManager handles FE scale-down operations
type FEScaleManager struct {
	client *doris_client.DorisClient
}

// NewFEScaleManager creates a new FE scale manager
func NewFEScaleManager(client *doris_client.DorisClient) *FEScaleManager {
	return &FEScaleManager{client: client}
}

// ScaleDown performs scale-down for FE nodes.
// Only observer FE nodes can be safely removed.
// Follower nodes are protected and cannot be removed via scale-down.
func (m *FEScaleManager) ScaleDown(ctx context.Context, action ScaleAction) ([]string, error) {
	if !action.IsScaleDown() {
		return nil, nil
	}

	if len(action.PodsToRemove) == 0 {
		return nil, fmt.Errorf("no pods to remove in scale-down action")
	}

	frontends, err := m.client.ShowFrontends(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query FE nodes: %w", err)
	}

	var readyForRemoval []string

	for _, podName := range action.PodsToRemove {
		fe := doris_client.MatchPodToFrontend(podName, frontends)
		if fe == nil {
			feScaleLogger.Info("FE node not found in Doris cluster, safe to remove",
				"pod", podName)
			readyForRemoval = append(readyForRemoval, podName)
			continue
		}

		switch action.Strategy {
		case "drop-observer":
			if fe.Role == "FOLLOWER" || fe.IsMaster {
				return nil, fmt.Errorf(
					"cannot scale down FE %s: it is a %s node (only OBSERVER nodes can be scaled down)",
					podName, fe.Role)
			}

			feScaleLogger.Info("Dropping FE observer node",
				"pod", podName, "host", fe.Host, "port", fe.EditLogPort)
			if err := m.client.DropObserver(ctx, fe.Host, fe.EditLogPort); err != nil {
				return nil, fmt.Errorf("failed to drop FE observer %s: %w", podName, err)
			}
			readyForRemoval = append(readyForRemoval, podName)

		default:
			return nil, fmt.Errorf("unknown FE scale-down strategy: %s", action.Strategy)
		}
	}

	return readyForRemoval, nil
}

// GetFENodeStatuses converts Doris FE node info to NodeStatus slice
func (m *FEScaleManager) GetFENodeStatuses(ctx context.Context, podNames []string) ([]FENodeStatus, error) {
	frontends, err := m.client.ShowFrontends(ctx)
	if err != nil {
		return nil, err
	}

	var statuses []FENodeStatus
	for _, podName := range podNames {
		fe := doris_client.MatchPodToFrontend(podName, frontends)
		status := FENodeStatus{PodName: podName}
		if fe != nil {
			status.Host = fe.Host
			status.Role = fe.Role
			status.IsMaster = fe.IsMaster
			status.Alive = fe.Alive
		} else {
			status.Alive = false
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

// FENodeStatus represents the scale-relevant status of an FE pod
type FENodeStatus struct {
	PodName  string
	Host     string
	Role     string // FOLLOWER, OBSERVER, MASTER
	IsMaster bool
	Alive    bool
}

package scale

import (
	"context"
	"fmt"

	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
	ctrl "sigs.k8s.io/controller-runtime"
)

var feScaleLogger = ctrl.Log.WithName("scale-fe")

const (
	frontendRoleObserver = "OBSERVER"
	frontendRoleFollower = "FOLLOWER"
)

type frontendScaleClient interface {
	ShowFrontends(ctx context.Context) ([]doris_client.FrontendInfo, error)
	DropObserver(ctx context.Context, host string, port int) error
}

// FEScaleManager handles FE scale-down operations
type FEScaleManager struct {
	client frontendScaleClient
}

type frontendRemovalPlan struct {
	podName       string
	host          string
	editLogPort   int
	alreadyAbsent bool
}

type frontendScaleDownPlan struct {
	removals []frontendRemovalPlan
}

// NewFEScaleManager creates a new FE scale manager
func NewFEScaleManager(client *doris_client.DorisClient) *FEScaleManager {
	return &FEScaleManager{client: client}
}

// PrepareScaleDownActions performs every non-destructive FE check before any
// intent is authorized or any DROP OBSERVER statement is issued. Preparing the
// complete action set first prevents a later invalid role group from leaving an
// earlier group partially mutated with no path for the StatefulSet to converge.
func (m *FEScaleManager) PrepareScaleDownActions(
	ctx context.Context,
	actions []ScaleAction,
) ([]frontendScaleDownPlan, error) {
	plans := make([]frontendScaleDownPlan, len(actions))
	hasFrontendAction := false
	for i := range actions {
		action := &actions[i]
		if action.Component != constants.ComponentTypeFE || !action.IsScaleDown() {
			continue
		}
		hasFrontendAction = true
		if len(action.PodsToRemove) == 0 {
			return nil, fmt.Errorf("no pods to remove in FE scale-down action")
		}
		if action.Strategy != StrategyDropObserver {
			return nil, fmt.Errorf("unknown FE scale-down strategy: %s", action.Strategy)
		}
	}
	if !hasFrontendAction {
		return plans, nil
	}

	frontends, err := m.client.ShowFrontends(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query FE nodes before authorizing scale-down: %w", err)
	}

	for i := range actions {
		action := &actions[i]
		if action.Component != constants.ComponentTypeFE || !action.IsScaleDown() {
			continue
		}

		plan := frontendScaleDownPlan{removals: make([]frontendRemovalPlan, 0, len(action.PodsToRemove))}
		for _, podName := range action.PodsToRemove {
			fe := doris_client.MatchPodToFrontend(podName, frontends)
			if fe == nil {
				plan.removals = append(plan.removals, frontendRemovalPlan{
					podName:       podName,
					alreadyAbsent: true,
				})
				continue
			}
			if fe.Role != frontendRoleObserver || fe.IsMaster {
				return nil, fmt.Errorf(
					"cannot scale down FE %s: it is a %s node (only OBSERVER nodes can be scaled down)",
					podName,
					fe.Role,
				)
			}
			plan.removals = append(plan.removals, frontendRemovalPlan{
				podName:     podName,
				host:        fe.Host,
				editLogPort: fe.EditLogPort,
			})
		}
		plans[i] = plan
	}
	return plans, nil
}

// AuthorizeScaleDown persists the complete FE removal set behind the
// DorisCluster generation and resourceVersion fence. It must run after all
// plans have passed PrepareScaleDownActions and before ExecuteScaleDown.
func (m *FEScaleManager) AuthorizeScaleDown(
	ctx context.Context,
	plan frontendScaleDownPlan,
	tracker FrontendDropTracker,
) error {
	if tracker == nil {
		return fmt.Errorf("FE scale-down requires a persistent drop tracker")
	}
	for _, removal := range plan.removals {
		tracker.RecordFrontendDropIntent(removal.podName)
	}
	if err := tracker.Persist(ctx); err != nil {
		return fmt.Errorf("persist Doris frontend scale-down intent: %w", err)
	}
	return nil
}

// ExecuteScaleDown performs only the destructive part of a prepared and
// durably-authorized FE scale-down. A successful or already-absent node moves
// to the removed phase; its latch remains until StatefulSet convergence is
// observed on a later reconciliation.
func (m *FEScaleManager) ExecuteScaleDown(
	ctx context.Context,
	plan frontendScaleDownPlan,
	tracker FrontendDropTracker,
) ([]string, error) {
	if tracker == nil {
		return nil, fmt.Errorf("FE scale-down requires a persistent drop tracker")
	}

	readyForRemoval := make([]string, 0, len(plan.removals))
	for _, removal := range plan.removals {
		if removal.alreadyAbsent {
			feScaleLogger.Info("FE node not found in Doris cluster, safe to remove",
				"pod", removal.podName)
			readyForRemoval = append(readyForRemoval, removal.podName)
			tracker.MarkFrontendRemoved(removal.podName)
			continue
		}

		feScaleLogger.Info("Dropping FE observer node",
			"pod", removal.podName, "host", removal.host, "port", removal.editLogPort)
		if err := m.client.DropObserver(ctx, removal.host, removal.editLogPort); err != nil {
			return nil, fmt.Errorf("failed to drop FE observer %s: %w", removal.podName, err)
		}
		readyForRemoval = append(readyForRemoval, removal.podName)
		tracker.MarkFrontendRemoved(removal.podName)
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

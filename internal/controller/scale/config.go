package scale

import (
	"context"
	"time"
)

// ScaleDownPolicy provides read-only scale-down configuration.
type ScaleDownPolicy interface {
	// GetDecommissionTimeout returns the maximum duration to wait for BE decommission.
	// After this timeout, the operator will force-drop the node.
	GetDecommissionTimeout() time.Duration
}

// DecommissionTracker manages BE decommission lifecycle state.
// It tracks pre-mutation intent or start times and handles persistence of
// annotation updates.
type DecommissionTracker interface {
	// GetStart returns the tracked state for a pod: normally an RFC3339 start
	// timestamp, or an internal pre-mutation intent marker before Doris has
	// confirmed decommissioning. It returns empty string and false when unset.
	GetStart(podName string) (string, bool)
	// RecordStart records the decommission start time for a pod.
	RecordStart(podName string, timestamp string)
	// ClearStart removes the decommission start time for a pod.
	ClearStart(podName string)
	// Persist writes all pending changes (records + clears) to the backing store.
	// Must be called after RecordStart/ClearStart to take effect.
	Persist(ctx context.Context) error
	// PendingPods returns pod names that have active (non-cleared) decommission tracking.
	// Used to determine if any STS replicas need to be gated.
	PendingPods() []string
}

// FrontendDropTracker manages durable intent for destructive FE observer
// removals. The intent is persisted before DROP OBSERVER, transitions to a
// removed phase after Doris no longer contains the node, and is cleared only
// after the StatefulSet no longer retains the corresponding pod ordinal.
type FrontendDropTracker interface {
	RecordFrontendDropIntent(podName string)
	MarkFrontendRemoved(podName string)
	Persist(ctx context.Context) error
}

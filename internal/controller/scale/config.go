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
// It tracks start times and handles persistence of annotation updates.
type DecommissionTracker interface {
	// GetStart returns the decommission start time for a pod (RFC3339).
	// Returns the timestamp and true if set, empty string and false otherwise.
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

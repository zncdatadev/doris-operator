package scale

import (
	"time"
)

// ScaleConfig provides scale-down configuration for the scale manager.
type ScaleConfig interface {
	// GetDecommissionTimeout returns the maximum duration to wait for BE decommission.
	// After this timeout, the operator will force-drop the node.
	GetDecommissionTimeout() time.Duration
	// GetDecommissionStartAnnotation returns the recorded decommission start time
	// for a given pod name, and whether it exists.
	GetDecommissionStartAnnotation(podName string) (string, bool)
	// SetDecommissionStartAnnotation records the decommission start time for a pod.
	SetDecommissionStartAnnotation(podName string, timestamp string)
	// PersistAnnotations persists any pending annotation changes.
	PersistAnnotations() error
}

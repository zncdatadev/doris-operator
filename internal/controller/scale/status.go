package scale

import (
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
)

// UpdateClusterStatus updates the DorisCluster CR status with node information
// from the scale reconciliation result.
func UpdateClusterStatus(
	clusterStatus *dorisv1alpha1.DorisClusterStatus,
	beStatuses []BENodeStatus,
	feStatuses []FENodeStatus,
) {
	if clusterStatus == nil {
		return
	}

	// Update BE node statuses
	clusterStatus.BackEndNodes = make([]dorisv1alpha1.NodeStatus, len(beStatuses))
	for i, be := range beStatuses {
		phase := ""
		if be.Decommission {
			phase = "Decommissioning"
		}
		clusterStatus.BackEndNodes[i] = dorisv1alpha1.NodeStatus{
			Name:  be.PodName,
			Host:  be.Host,
			Alive: be.Alive,
			Phase: phase,
		}
	}

	// Update FE node statuses
	clusterStatus.FrontEndNodes = make([]dorisv1alpha1.NodeStatus, len(feStatuses))
	for i, fe := range feStatuses {
		clusterStatus.FrontEndNodes[i] = dorisv1alpha1.NodeStatus{
			Name:  fe.PodName,
			Host:  fe.Host,
			Role:  fe.Role,
			Alive: fe.Alive,
		}
	}
}

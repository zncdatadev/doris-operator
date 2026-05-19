package scale

import (
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
)

// UpdateClusterStatus updates the DorisCluster CR status with node information
// from the scale reconciliation result.
// It only updates fields when fresh data was successfully fetched (non-nil slices).
func UpdateClusterStatus(
	clusterStatus *dorisv1alpha1.DorisClusterStatus,
	beStatuses []BENodeStatus,
	feStatuses []FENodeStatus,
	brokerStatuses []BrokerNodeStatus,
) {
	if clusterStatus == nil {
		return
	}

	// Update BE node statuses only when data is available
	if beStatuses != nil {
		clusterStatus.BackendNodes = make([]dorisv1alpha1.NodeStatus, len(beStatuses))
		for i, be := range beStatuses {
			phase := ""
			if be.Decommission {
				phase = "Decommissioning"
			}
			clusterStatus.BackendNodes[i] = dorisv1alpha1.NodeStatus{
				Name:  be.PodName,
				Host:  be.Host,
				Alive: be.Alive,
				Phase: phase,
			}
		}
	}

	// Update FE node statuses only when data is available
	if feStatuses != nil {
		clusterStatus.FrontendNodes = make([]dorisv1alpha1.NodeStatus, len(feStatuses))
		for i, fe := range feStatuses {
			clusterStatus.FrontendNodes[i] = dorisv1alpha1.NodeStatus{
				Name:  fe.PodName,
				Host:  fe.Host,
				Role:  fe.Role,
				Alive: fe.Alive,
			}
		}
	}

	// Update Broker node statuses only when data is available
	if brokerStatuses != nil {
		clusterStatus.BrokerNodes = make([]dorisv1alpha1.NodeStatus, len(brokerStatuses))
		for i, b := range brokerStatuses {
			clusterStatus.BrokerNodes[i] = dorisv1alpha1.NodeStatus{
				Name:  b.PodName,
				Host:  b.Host,
				Alive: b.Alive,
			}
		}
	}
}

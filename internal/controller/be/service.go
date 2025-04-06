package be

import (
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// GetBEServiceConfig returns the default service configuration for BE
func GetBEServiceConfig() *common.ComponentServiceConfig {
	// Define the BE container ports - use the same names as in the StatefulSet!
	beHeartbeatPort := corev1.ContainerPort{
		Name:          constants.BEHeartbeatPortName, // Use constant port name
		ContainerPort: constants.BEHeartbeatPort,
		Protocol:      corev1.ProtocolTCP,
	}

	// Internal service only exposes heartbeat port
	internalPorts := []corev1.ContainerPort{beHeartbeatPort}

	// Access service exposes all BE ports
	accessPorts := []corev1.ContainerPort{
		{
			Name:          constants.BERpcPortName, // Use constant port name
			ContainerPort: constants.BERpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.BEHttpPortName, // Use constant port name
			ContainerPort: constants.BEHttpPort,
			Protocol:      corev1.ProtocolTCP,
		},
		beHeartbeatPort, // Reuse the same port definition
		{
			Name:          constants.BEBrpcPortName, // Use constant port name
			ContainerPort: constants.BEBrpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	return &common.ComponentServiceConfig{
		ComponentType: constants.ComponentTypeBE,
		InternalPorts: internalPorts,
		AccessPorts:   accessPorts,
	}
}

// NewBEInternalServiceReconciler creates an internal service reconciler for BE component
func NewBEInternalServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Use BE service configuration
	beServiceConfig := GetBEServiceConfig()

	// Create internal service using the common implementation
	return common.NewInternalServiceReconciler(client, roleGroupInfo, beServiceConfig)
}

// NewBEAccessServiceReconciler creates an access service reconciler for BE component
func NewBEAccessServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Use BE service configuration
	beServiceConfig := GetBEServiceConfig()

	// Create access service using the common implementation
	return common.NewAccessServiceReconciler(client, roleGroupInfo, beServiceConfig)
}

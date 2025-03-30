package fe

import (
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// GetFEServiceConfig returns the default service configuration for FE
func GetFEServiceConfig() *common.ComponentServiceConfig {
	// Define the FE container ports
	feQueryPort := corev1.ContainerPort{
		Name:          constants.ServiceQueryPortName,
		ContainerPort: constants.FEQueryPort,
		Protocol:      corev1.ProtocolTCP,
	}

	// Internal service only exposes query port
	internalPorts := []corev1.ContainerPort{feQueryPort}

	// Access service exposes all FE ports
	accessPorts := []corev1.ContainerPort{
		{
			Name:          constants.ServiceHttpPortName,
			ContainerPort: constants.FEHttpPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.ServiceRpcPortName,
			ContainerPort: constants.FERpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		feQueryPort, // Reuse the same port definition
		{
			Name:          constants.ServiceEditLogPortName,
			ContainerPort: constants.FEEditLogPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	return &common.ComponentServiceConfig{
		ComponentType: constants.ComponentTypeFE,
		InternalPorts: internalPorts,
		AccessPorts:   accessPorts,
	}
}

// NewFEInternalServiceReconciler creates an internal service reconciler for FE component
func NewFEInternalServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Use FE service configuration
	feServiceConfig := GetFEServiceConfig()

	// Create internal service using the common implementation
	return common.NewInternalServiceReconciler(client, roleGroupInfo, feServiceConfig)
}

// NewFEAccessServiceReconciler creates an access service reconciler for FE component
func NewFEAccessServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Use FE service configuration
	feServiceConfig := GetFEServiceConfig()

	// Create access service using the common implementation
	return common.NewAccessServiceReconciler(client, roleGroupInfo, feServiceConfig)
}

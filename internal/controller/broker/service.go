package broker

import (
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// GetBrokerServiceConfig returns the default service configuration for Broker
func GetBrokerServiceConfig() *common.ComponentServiceConfig {
	// Define the Broker container port
	brokerIpcPort := corev1.ContainerPort{
		Name:          constants.BrokerIpcPortName,
		ContainerPort: constants.BrokerIpcPort,
		Protocol:      corev1.ProtocolTCP,
	}

	// Internal service exposes IPC port
	internalPorts := []corev1.ContainerPort{brokerIpcPort}

	// Access service exposes IPC port
	accessPorts := []corev1.ContainerPort{brokerIpcPort}

	return &common.ComponentServiceConfig{
		ComponentType: constants.ComponentTypeBroker,
		InternalPorts: internalPorts,
		AccessPorts:   accessPorts,
	}
}

// NewBrokerInternalServiceReconciler creates an internal service reconciler for Broker component
func NewBrokerInternalServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	brokerServiceConfig := GetBrokerServiceConfig()
	return common.NewInternalServiceReconciler(client, roleGroupInfo, brokerServiceConfig)
}

// NewBrokerAccessServiceReconciler creates an access service reconciler for Broker component
func NewBrokerAccessServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	brokerServiceConfig := GetBrokerServiceConfig()
	return common.NewAccessServiceReconciler(client, roleGroupInfo, brokerServiceConfig)
}

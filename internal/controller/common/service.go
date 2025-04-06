package common

import (
	"fmt"

	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceType defines the different types of services for Doris components
type ServiceType string

const (
	// ServiceTypeInternal is for internal (headless) services
	ServiceTypeInternal ServiceType = "internal"

	// ServiceTypeAccess is for externally accessible services
	ServiceTypeAccess ServiceType = "access"
)

// ComponentServiceConfig defines the port configuration for a specific component
type ComponentServiceConfig struct {
	// ComponentType is the type of component (FE/BE)
	ComponentType constants.ComponentType

	// InternalPorts defines which ports to expose in the internal headless service
	InternalPorts []corev1.ContainerPort

	// AccessPorts defines which ports to expose in the access service
	AccessPorts []corev1.ContainerPort
}

// DorisServiceBuilder implements the ServiceBuilder interface for Doris services
type DorisServiceBuilder struct {
	*builder.BaseServiceBuilder
	componentType   constants.ComponentType
	clusterName     string
	roleGroupInfo   *reconciler.RoleGroupInfo
	serviceType     ServiceType
	publishNotReady bool
}

// NewDorisServiceBuilder creates a new DorisServiceBuilder
func NewDorisServiceBuilder(
	client *client.Client,
	serviceType ServiceType,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *ComponentServiceConfig,
) builder.ServiceBuilder {
	// Determine service name
	componentType := config.ComponentType
	serviceName := GetServiceName(roleGroupInfo.ClusterName, componentType, serviceType)

	// Get service ports based on service type
	var ports []corev1.ContainerPort
	if serviceType == ServiceTypeInternal {
		ports = config.InternalPorts
	} else {
		ports = config.AccessPorts
	}

	// Prepare labels
	matchLabels := roleGroupInfo.GetLabels()
	svcLabels := map[string]string{
		constants.OwnerReferenceLabelKey: roleGroupInfo.ClusterName,
		constants.ServiceRoleLabelKey:    string(serviceType),
		constants.ComponentLabelKey:      string(componentType),
	}

	// If this is an internal service, set it to headless
	// var listenerClass opconstants.ListenerClass
	// if serviceType == ServiceTypeInternal {
	// 	listenerClass = opconstants.ClusterInternal
	// } else {
	// 	listenerClass = opconstants.ExternalUnstable
	// }

	// Create the BaseServiceBuilder with container ports
	// ServiceBuilder will convert these to ServicePort internally
	baseBuilder := builder.NewServiceBuilder(
		client,
		serviceName,
		ports,
		func(sbo *builder.ServiceBuilderOptions) {
			sbo.Headless = (serviceType == ServiceTypeInternal)
			sbo.ListenerClass = opconstants.ClusterInternal
			sbo.Labels = svcLabels
			sbo.MatchingLabels = matchLabels
		},
	)

	return &DorisServiceBuilder{
		BaseServiceBuilder: baseBuilder,
		componentType:      componentType,
		clusterName:        roleGroupInfo.ClusterName,
		roleGroupInfo:      roleGroupInfo,
		serviceType:        serviceType,
		publishNotReady:    (serviceType == ServiceTypeInternal),
	}
}

// Get service name based on component type and service type
func GetServiceName(clusterName string, componentType constants.ComponentType, serviceType ServiceType) string {
	if serviceType == ServiceTypeInternal {
		return clusterName + "-" + string(componentType) + constants.ServiceInternalSuffix
	}
	return clusterName + "-" + string(componentType) + constants.ServiceAccessSuffix
}

// GetObject returns the final Service object
func (b *DorisServiceBuilder) GetObject() *corev1.Service {
	obj := b.BaseServiceBuilder.GetObject()
	obj.Spec.PublishNotReadyAddresses = b.publishNotReady

	// Add hash annotation for tracking changes
	if obj.Annotations == nil {
		obj.Annotations = make(map[string]string)
	}
	obj.Annotations[constants.HashAnnotationKey] = fmt.Sprintf("%d", metav1.Now().Unix())

	return obj
}

// NewInternalServiceReconciler creates an internal service reconciler for a Doris component
func NewInternalServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *ComponentServiceConfig,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Create internal service
	internalSvcBuilder := NewDorisServiceBuilder(
		client,
		ServiceTypeInternal,
		roleGroupInfo,
		config,
	)
	return reconciler.NewGenericResourceReconciler(client, internalSvcBuilder)
}

// NewAccessServiceReconciler creates an access service reconciler for a Doris component
func NewAccessServiceReconciler(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *ComponentServiceConfig,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Create access service
	accessSvcBuilder := NewDorisServiceBuilder(
		client,
		ServiceTypeAccess,
		roleGroupInfo,
		config,
	)
	return reconciler.NewGenericResourceReconciler(client, accessSvcBuilder)
}

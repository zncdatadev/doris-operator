package broker

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// BrokerStatefulSetBuilder implements common.StatefulSetComponentBuilder
type BrokerStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	brokerRole *dorisv1alpha1.ConfigSpec
}

// NewBrokerStatefulSetBuilder creates a new Broker StatefulSetBuilder
func NewBrokerStatefulSetBuilder(
	commonBuilder *common.StatefulSetBuilder,
	brokerRoleGroup *dorisv1alpha1.ConfigSpec,
) *BrokerStatefulSetBuilder {
	return &BrokerStatefulSetBuilder{
		StatefulSetBuilder: commonBuilder,
		brokerRole:         brokerRoleGroup,
	}
}

// Build calls the common Build method and returns the final StatefulSet
func (b *BrokerStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	return b.StatefulSetBuilder.Build(ctx, b)
}

// GetMainContainer implements StatefulSetComponentBuilder
func (b *BrokerStatefulSetBuilder) GetMainContainer() *corev1.Container {
	// Broker specific port configuration
	ports := []corev1.ContainerPort{
		{
			Name:          constants.BrokerIpcPortName,
			ContainerPort: constants.BrokerIpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	// Broker uses TCP health check on IPC port
	livenessProbe := b.CreateTcpProbe(constants.BrokerIpcPort, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)
	readinessProbe := b.CreateTcpProbe(constants.BrokerIpcPort, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)

	// Get resource requirements
	resources := getBrokerResourcesSpec()

	// If resources are specified in RoleConfig, use them
	if b.brokerRole != nil && b.brokerRole.Resources != nil {
		if b.brokerRole.Resources.CPU == nil && b.brokerRole.Resources.Memory == nil {
			resources = &commonsv1alpha1.ResourcesSpec{
				CPU:    nil,
				Memory: nil,
			}
		} else {
			if b.brokerRole.Resources.CPU != nil {
				if b.brokerRole.Resources.CPU.Max.IsZero() && b.brokerRole.Resources.CPU.Min.IsZero() {
					resources.CPU = nil
				} else {
					resources.CPU = b.brokerRole.Resources.CPU
				}
			}
			if b.brokerRole.Resources.Memory != nil {
				if b.brokerRole.Resources.Memory.Limit.IsZero() {
					resources.Memory = nil
				} else {
					resources.Memory = b.brokerRole.Resources.Memory
				}
			}
		}
	}

	// Create base container
	container := b.CreateBaseContainer(
		constants.BrokerContainerName,
		constants.BrokerEntrypoint,
		ports,
		resources,
		livenessProbe,
		readinessProbe,
	)

	// Broker has no PVC, only common volume mounts (config, log, podinfo)
	return container
}

// GetInitContainers implements StatefulSetComponentBuilder
// Broker has no init containers
func (b *BrokerStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{}
}

// GetVolumes implements StatefulSetComponentBuilder
// Broker has no additional volumes beyond common ones
func (b *BrokerStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{}
}

// GetVolumeClaimTemplates implements StatefulSetComponentBuilder
// Broker is stateless, no PVC
func (b *BrokerStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{}
}

// GetAdditionalEnvVars implements StatefulSetComponentBuilder
// Broker has no additional environment variables beyond common ones
func (b *BrokerStatefulSetBuilder) GetAdditionalEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{}
}

// getBrokerResourcesSpec creates default resource specification for Broker
func getBrokerResourcesSpec() *commonsv1alpha1.ResourcesSpec {
	cpuMin := resource.MustParse(constants.DefaultCPURequest)
	cpuMax := resource.MustParse(constants.DefaultCPULimit)
	memLimit := resource.MustParse(constants.BEMemoryLimit)

	return &commonsv1alpha1.ResourcesSpec{
		CPU: &commonsv1alpha1.CPUResource{
			Min: cpuMin,
			Max: cpuMax,
		},
		Memory: &commonsv1alpha1.MemoryResource{
			Limit: memLimit,
		},
	}
}

// NewBrokerStatefulSetReconciler creates a Broker StatefulSet reconciler
func NewBrokerStatefulSetReconciler(
	ctx context.Context,
	client *client.Client,
	image *opgoutil.Image,
	replicas *int32,
	dorisCluster *dorisv1alpha1.DorisCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *dorisv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) (reconciler.ResourceReconciler[builder.StatefulSetBuilder], error) {
	img := image

	if img == nil {
		brokerImage := common.GetComponentImage(dorisCluster.Spec.Image, constants.ComponentTypeBroker)
		pullPolicy := common.GetPullPolicy(dorisCluster.Spec.Image)
		img = &opgoutil.Image{Custom: brokerImage, PullPolicy: pullPolicy}
	}

	commonBuilder := common.NewStatefulSetBuilder(
		ctx,
		client,
		constants.ComponentTypeBroker,
		roleGroupInfo,
		img,
		replicas,
		roleGroupConfig,
		overrides,
		dorisCluster,
	)

	brokerBuilder := NewBrokerStatefulSetBuilder(commonBuilder, roleGroupConfig)
	// Set stopped flag
	stopped := clusterOperation != nil && clusterOperation.Stopped
	return reconciler.NewStatefulSet(
		client,
		brokerBuilder,
		stopped,
	), nil
}

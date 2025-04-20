package be

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// BeStatefulSetBuilder implements common.StatefulSetComponentBuilder
type BeStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	beRole *dorisv1alpha1.ConfigSpec
}

// NewBeStatefulSetBuilder creates a new BE StatefulSetBuilder
func NewBeStatefulSetBuilder(
	commonBuilder *common.StatefulSetBuilder,
	beRoleGroup *dorisv1alpha1.ConfigSpec,
) *BeStatefulSetBuilder {
	return &BeStatefulSetBuilder{
		StatefulSetBuilder: commonBuilder,
		beRole:             beRoleGroup,
	}
}

// Build calls the common Build method and returns the final StatefulSet
func (b *BeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	return b.StatefulSetBuilder.Build(ctx, b)
}

// GetMainContainer implements ComponentInterface
func (b *BeStatefulSetBuilder) GetMainContainer() *corev1.Container {
	// BE specific ports configuration
	ports := []corev1.ContainerPort{
		{
			Name:          constants.BERpcPortName,
			ContainerPort: constants.BERpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.BEHttpPortName,
			ContainerPort: constants.BEHttpPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.BEHeartbeatPortName,
			ContainerPort: constants.BEHeartbeatPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.BEBrpcPortName,
			ContainerPort: constants.BEBrpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	// BE specific health checks
	livenessProbe := b.CreateTcpProbe(constants.BEHeartbeatPort, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)
	readinessProbe := b.CreateHttpProbe(constants.BEHttpPort, constants.HealthCheckPath, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)

	// Get resource requirements
	resources := getBeResourcesSpec()

	// If resources are specified in RoleConfig, use them
	if b.beRole != nil && b.beRole.Resources != nil {
		resources = b.beRole.Resources
	}

	// Create base container
	container := b.CreateBaseContainer(
		constants.BEContainerName,
		constants.BEEntrypoint,
		ports,
		resources,
		livenessProbe,
		readinessProbe,
	)

	// Add BE specific volume mounts
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{
			Name:      constants.BEStorageVolume,
			MountPath: constants.BEStoragePath,
		},
	)

	return container
}

// GetInitContainers implements BE initialization containers
func (b *BeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{
		{
			Name:            constants.InitContainerName,
			Image:           common.GetInitContainerImage(),
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command: []string{
				"sh",
				"-c",
				constants.BEInitCommand,
			},
			SecurityContext: &corev1.SecurityContext{
				Privileged: ptr.To(true),
			},
		},
	}
}

// GetVolumes implements ComponentInterface, returns BE specific volumes
func (b *BeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: constants.ConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: b.GetRoleGroupInfo().GetFullName(),
					},
				},
			},
		},
	}
}

// GetVolumeClaimTemplates implements ComponentInterface, returns BE storage PVCs
func (b *BeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	storageSize := resource.MustParse(constants.BEStorageSize)
	var storageClassName *string

	// Get storage configuration from resources if specified
	resources := getBeResourcesSpec()
	if b.beRole != nil && b.beRole.Resources != nil {
		resources = b.beRole.Resources
	}

	// Get storage parameters from resource configuration
	if resources != nil && resources.Storage != nil {
		storageSize = resources.Storage.Capacity
		if resources.Storage.StorageClass != "" {
			storageClassName = &resources.Storage.StorageClass
		}
	}

	return []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: constants.BEStorageVolume,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeMode:  ptr.To(corev1.PersistentVolumeFilesystem),
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
				StorageClassName: storageClassName,
			},
		},
	}
}

// GetAdditionalEnvVars implements ComponentInterface, returns BE specific environment variables
func (b *BeStatefulSetBuilder) GetAdditionalEnvVars() []corev1.EnvVar {
	// BE component doesn't need additional environment variables
	return []corev1.EnvVar{}
}

// Create default resource specification for BE
func getBeResourcesSpec() *commonsv1alpha1.ResourcesSpec {
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
		Storage: &commonsv1alpha1.StorageResource{
			Capacity:     resource.MustParse(constants.BEStorageSize),
			StorageClass: "",
		},
	}
}

// NewBeStatefulSetReconciler creates a BE StatefulSet reconciler
func NewBeStatefulSetReconciler(
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
	// 创建镜像对象
	var img *opgoutil.Image = image

	if img == nil {
		// 使用common包中的函数获取BE组件镜像
		beImage := common.GetComponentImage(dorisCluster.Spec.Image, constants.ComponentTypeBE)
		pullPolicy := common.GetPullPolicy(dorisCluster.Spec.Image)
		img = &opgoutil.Image{Custom: beImage, PullPolicy: pullPolicy}
	}

	commonBuilder := common.NewStatefulSetBuilder(
		ctx,
		client,
		constants.ComponentTypeBE,
		roleGroupInfo,
		img,
		replicas,
		roleGroupConfig,
		overrides,
		dorisCluster,
	)

	beBuilder := NewBeStatefulSetBuilder(commonBuilder, roleGroupConfig)
	// stopped
	stopped := false
	if clusterOperation != nil && clusterOperation.Stopped {
		stopped = true
	}
	return reconciler.NewStatefulSet(
		client,
		beBuilder,
		stopped,
	), nil
}

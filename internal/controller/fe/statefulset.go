package fe

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

// FeStatefulSetBuilder implements common.StatefulSetComponentBuilder
type FeStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	feRole *dorisv1alpha1.ConfigSpec
}

// NewFeStatefulSetBuilder creates a new FE StatefulSetBuilder
func NewFeStatefulSetBuilder(
	commonBuilder *common.StatefulSetBuilder,
	feRoleConfig *dorisv1alpha1.ConfigSpec,
) *FeStatefulSetBuilder {
	return &FeStatefulSetBuilder{
		StatefulSetBuilder: commonBuilder,
		feRole:             feRoleConfig,
	}
}

// Build calls the common Build method and returns the final StatefulSet
func (b *FeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	return b.StatefulSetBuilder.Build(ctx, b)
}

// GetMainContainer implements ComponentInterface
func (b *FeStatefulSetBuilder) GetMainContainer() *corev1.Container {
	// FE specific ports configuration
	ports := []corev1.ContainerPort{
		{
			Name:          constants.FEHttpPortName,
			ContainerPort: constants.FEHttpPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.FERpcPortName,
			ContainerPort: constants.FERpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.FEQueryPortName,
			ContainerPort: constants.FEQueryPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          constants.FEEditLogPortName,
			ContainerPort: constants.FEEditLogPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	// FE specific health checks
	livenessProbe := b.CreateTcpProbe(constants.FEQueryPort, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)
	readinessProbe := b.CreateHttpProbe(constants.FEHttpPort, constants.HealthCheckPath, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)

	// Get resource requirements
	resources := getFeResourcesSpec()
	if b.feRole != nil && b.feRole.Resources != nil {
		resources = b.feRole.Resources
	}

	// Create base container
	container := b.CreateBaseContainer(
		constants.FEContainerName,
		constants.FEEntrypoint,
		ports,
		resources,
		livenessProbe,
		readinessProbe,
	)

	// Add FE specific environment variables
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  constants.FEElectNumberEnvVar,
		Value: constants.DefaultElectNumber,
	})

	// Add FE specific volume mounts
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{
			Name:      constants.FEMetadataVolume,
			MountPath: constants.FEMetadataPath,
		},
	)

	return container
}

// GetInitContainers FE doesn't need initialization containers
func (b *FeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{} // FE has no init containers
}

// GetVolumes implements ComponentInterface, returns FE specific volumes
func (b *FeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		// {
		// 	Name: constants.ConfigVolumeName,
		// 	VolumeSource: corev1.VolumeSource{
		// 		ConfigMap: &corev1.ConfigMapVolumeSource{
		// 			LocalObjectReference: corev1.LocalObjectReference{
		// 				Name: b.GetRoleGroupInfo().GetFullName(),
		// 			},
		// 		},
		// 	},
		// },
	}
}

// GetVolumeClaimTemplates implements ComponentInterface, returns FE storage PVCs
func (b *FeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	storageSize := resource.MustParse(constants.FEStorageSize)
	var storageClassName *string

	// Get storage configuration from resources if specified
	resources := getFeResourcesSpec()
	if b.feRole != nil && b.feRole.Resources != nil {
		resources = b.feRole.Resources
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
				Name: constants.FEMetadataVolume,
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

// GetAdditionalEnvVars implements ComponentInterface, returns FE specific environment variables
func (b *FeStatefulSetBuilder) GetAdditionalEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  constants.FEElectNumberEnvVar,
			Value: constants.DefaultElectNumber,
		},
	}
}

// Create default resource specification for FE
func getFeResourcesSpec() *commonsv1alpha1.ResourcesSpec {
	cpuMin := resource.MustParse(constants.DefaultCPURequest)
	cpuMax := resource.MustParse(constants.DefaultCPULimit)
	memLimit := resource.MustParse(constants.FEMemoryLimit)

	return &commonsv1alpha1.ResourcesSpec{
		CPU: &commonsv1alpha1.CPUResource{
			Min: cpuMin,
			Max: cpuMax,
		},
		Memory: &commonsv1alpha1.MemoryResource{
			Limit: memLimit,
		},
		Storage: &commonsv1alpha1.StorageResource{
			Capacity:     resource.MustParse(constants.FEStorageSize),
			StorageClass: "",
		},
	}
}

// NewFeStatefulSetReconciler creates a FE StatefulSet reconciler
func NewFeStatefulSetReconciler(
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
		// Get FE component image using common package function
		feImage := common.GetComponentImage(dorisCluster.Spec.Image, constants.ComponentTypeFE)
		pullPolicy := common.GetPullPolicy(dorisCluster.Spec.Image)
		img = &opgoutil.Image{Custom: feImage, PullPolicy: pullPolicy}
	}

	commonBuilder := common.NewStatefulSetBuilder(
		ctx,
		client,
		constants.ComponentTypeFE,
		roleGroupInfo,
		img,
		replicas,
		roleGroupConfig,
		overrides,
		dorisCluster,
	)

	feBuilder := NewFeStatefulSetBuilder(commonBuilder, roleGroupConfig)
	// Set stopped flag
	stopped := clusterOperation != nil && clusterOperation.Stopped
	return reconciler.NewStatefulSet(
		client,
		feBuilder,
		stopped,
	), nil
}

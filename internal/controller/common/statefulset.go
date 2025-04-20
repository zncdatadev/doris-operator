package common

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// ComponentType represents different Doris component types
type ComponentType string

const (
	ComponentTypeFE ComponentType = "fe"
	ComponentTypeBE ComponentType = "be"
)

// StatefulSetBuilder is the common builder for Doris StatefulSets
type StatefulSetBuilder struct {
	*builder.StatefulSet
	client        *client.Client
	componentType constants.ComponentType
	clusterName   string
	roleGroupInfo *reconciler.RoleGroupInfo
	dorisCluster  *dorisv1alpha1.DorisCluster
	ctx           context.Context
}

// NewStatefulSetBuilder creates a new StatefulSetBuilder with common configuration
func NewStatefulSetBuilder(
	ctx context.Context,
	client *client.Client,
	componentType constants.ComponentType,
	roleGroupInf *reconciler.RoleGroupInfo,
	image *opgoutil.Image,
	replicas *int32,
	roleConfig *dorisv1alpha1.ConfigSpec,
	overrdes *commonsv1alpha1.OverridesSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
) *StatefulSetBuilder {
	var roleGroupConfigSpec *commonsv1alpha1.RoleGroupConfigSpec
	if roleConfig != nil {
		roleGroupConfigSpec = roleConfig.RoleGroupConfigSpec
	}
	return &StatefulSetBuilder{
		StatefulSet: builder.NewStatefulSetBuilder(
			client,
			roleGroupInf.GetFullName(),
			replicas,
			image,
			overrdes,
			roleGroupConfigSpec,
			func(o *builder.Options) {
				o.ClusterName = roleGroupInf.ClusterName
				o.Labels = roleGroupInf.GetLabels()
				o.Annotations = roleGroupInf.GetAnnotations()
				o.RoleName = roleGroupInf.RoleName
				o.RoleGroupName = roleGroupInf.GetFullName()
			},
		),
		client:        client,
		componentType: componentType,
		clusterName:   roleGroupInf.GetClusterName(),
		roleGroupInfo: roleGroupInf,
		dorisCluster:  dorisCluster,
		ctx:           ctx,
	}
}

// StatefulSetComponentBuilder defines methods that should be implemented by BE/FE specific builders
type StatefulSetComponentBuilder interface {
	// GetMainContainer returns the main container for the component
	GetMainContainer() *corev1.Container
	// GetInitContainers returns any init containers required by the component
	GetInitContainers() []corev1.Container
	// GetVolumes returns component-specific volumes
	GetVolumes() []corev1.Volume
	// GetVolumeClaimTemplates returns PVCs for the component
	GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim
	// GetAdditionalEnvVars returns component-specific environment variables
	GetAdditionalEnvVars() []corev1.EnvVar
}

// Build constructs the StatefulSet object combining common and component-specific configurations
func (b *StatefulSetBuilder) Build(ctx context.Context, component StatefulSetComponentBuilder) (ctrlclient.Object, error) {
	// Add component-specific container
	b.AddContainer(component.GetMainContainer())

	// Add init containers if any
	initContainers := component.GetInitContainers()
	for i := range initContainers {
		b.AddInitContainer(&initContainers[i])
	}

	// Add common volumes
	b.AddVolumes(b.getCommonVolumes())

	// Add component-specific volumes
	b.AddVolumes(component.GetVolumes())

	// Add volume claim templates
	b.AddVolumeClaimTemplates(component.GetVolumeClaimTemplates())

	// Create the StatefulSet object
	sts, err := b.GetObject()
	if err != nil {
		return nil, err
	}

	// Set parallel pod management for faster scaling
	sts.Spec.PodManagementPolicy = appv1.ParallelPodManagement

	return sts, nil
}

// GetObject returns the StatefulSet object
func (b *StatefulSetBuilder) GetObject() (*appv1.StatefulSet, error) {
	tpl, err := b.GetPodTemplate()
	if err != nil {
		return nil, err
	}
	obj := &appv1.StatefulSet{
		ObjectMeta: b.GetObjectMeta(),
		Spec: appv1.StatefulSetSpec{
			Replicas:             b.GetReplicas(),
			Selector:             b.GetLabelSelector(),
			ServiceName:          b.GetName(),
			Template:             *tpl,
			VolumeClaimTemplates: b.GetVolumeClaimTemplates(),
		},
	}
	return obj, nil
}

// getCommonVolumes returns volumes common to both BE and FE components
func (b *StatefulSetBuilder) getCommonVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: constants.ConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: b.GetName(),
					},
				},
			},
		},
		{
			Name: "podinfo",
			VolumeSource: corev1.VolumeSource{
				DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{
						{
							Path: "labels",
							FieldRef: &corev1.ObjectFieldSelector{
								APIVersion: "v1",
								FieldPath:  "metadata.labels",
							},
						},
						{
							Path: "annotations",
							FieldRef: &corev1.ObjectFieldSelector{
								APIVersion: "v1",
								FieldPath:  "metadata.annotations",
							},
						},
					},
				},
			},
		},
	}
}

// getFeServiceAddress returns the FE service address for BE to connect to
func (b *StatefulSetBuilder) getFeServiceAddress() string {
	return GetServiceName(b.clusterName, constants.ComponentTypeFE, ServiceTypeAccess)
}

// CreateTcpProbe creates a TCP probe for health checking
func (b *StatefulSetBuilder) CreateTcpProbe(port int32, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
	}
}

// CreateHttpProbe creates an HTTP probe for health checking
func (b *StatefulSetBuilder) CreateHttpProbe(port int32, path string, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: path,
				Port: intstr.FromInt32(port),
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
	}
}

// CreateBaseContainer creates a basic container with common configuration that BE and FE can extend
func (b *StatefulSetBuilder) CreateBaseContainer(
	containerName string,
	entrypoint string,
	ports []corev1.ContainerPort,
	resources *commonsv1alpha1.ResourcesSpec,
	livenessProbe *corev1.Probe,
	readinessProbe *corev1.Probe,
) *corev1.Container {
	// Common environment variables
	commonEnvVars := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name: "HOST_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name:  "CONFIGMAP_MOUNT_PATH",
			Value: "/etc/doris/conf",
		},
		{
			Name:  "USER",
			Value: "root",
		},
		{
			Name:  "DORIS_ROOT",
			Value: "/opt/apache-doris",
		},
		{
			Name:  "ENV_FE_ADDR",
			Value: b.getFeServiceAddress(),
		},
		{
			Name:  "FE_QUERY_PORT",
			Value: "9030",
		},
	}

	// Common volume mounts
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "podinfo",
			MountPath: "/etc/podinfo",
		},
		{
			Name:      constants.ConfigVolumeName,
			MountPath: "/etc/doris/conf",
		},
	}

	// Use builder to create container
	containerBuilder := builder.NewContainerBuilder(containerName, b.GetImage()).
		AddEnvVars(commonEnvVars).
		SetCommand([]string{entrypoint}).
		SetArgs([]string{"$(ENV_FE_ADDR)"}).
		AddPorts(ports).
		SetLivenessProbe(livenessProbe).
		SetReadinessProbe(readinessProbe).
		AddVolumeMounts(volumeMounts)

	// Convert ResourcesSpec to k8s ResourceRequirements
	if resources != nil {
		containerBuilder.SetResources(resources)
	}

	return containerBuilder.Build()
}

// GetRoleGroupInfo returns the roleGroupInfo
func (b *StatefulSetBuilder) GetRoleGroupInfo() *reconciler.RoleGroupInfo {
	return b.roleGroupInfo
}

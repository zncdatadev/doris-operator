package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/zncdatadev/operator-go/pkg/util"
)

const (
	DefaultRepository      = "quay.io/zncdatadev"
	DefaultProductVersion  = "2.1.8"
	DefaultProductName     = "doris"
	DefaultKubedoopVersion = "0.0.0-dev"
)

type ImageSpec struct {
	// +kubebuilder:validation:Optional
	Custom string `json:"custom,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default=quay.io/zncdatadev
	Repo string `json:"repository,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="0.0.0-dev"
	KubedoopVersion string `json:"kubedoopVersion,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="3.7.1"
	ProductVersion string `json:"productVersion,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// +kubebuilder:validation:Optional
	PullSecretName string `json:"pullSecretName,omitempty"`
}

func DefaultImageSpec() *ImageSpec {
	return &ImageSpec{
		Repo:            DefaultRepository,
		ProductVersion:  DefaultProductVersion,
		KubedoopVersion: DefaultKubedoopVersion,
	}
}

func TransformImage(imageSpec *ImageSpec) *util.Image {
	if imageSpec == nil {
		return util.NewImage(DefaultProductName, DefaultKubedoopVersion, DefaultProductVersion)
	}
	var pullPolicy corev1.PullPolicy = corev1.PullIfNotPresent
	if imageSpec.PullPolicy != nil {
		pullPolicy = *imageSpec.PullPolicy
	}
	return &util.Image{
		Custom:          imageSpec.Custom,
		Repo:            imageSpec.Repo,
		KubedoopVersion: imageSpec.KubedoopVersion,
		ProductVersion:  imageSpec.ProductVersion,
		PullPolicy:      pullPolicy,
		PullSecretName:  imageSpec.PullSecretName,
		ProductName:     DefaultProductName,
	}
}

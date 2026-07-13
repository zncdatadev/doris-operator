package common

import (
	"fmt"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// GetImage returns the image for the given component.
//
// Unless imageSpec.Custom is set, it resolves to the official apache/doris
// per-component images (e.g. apache/doris:fe-2.1.8), honoring
// imageSpec.ProductVersion when provided.
//
// When imageSpec.Custom is set, it delegates to TransformImage so the custom
// image is used as-is.
//
// TODO: Once the unified quay.io/zncdatadev/doris:<version>-kubedoop<version>
// image is published and production-ready, always use TransformImage instead
// of the official per-component images.
func GetImage(imageSpec *dorisv1alpha1.ImageSpec, componentType constants.ComponentType) *opgoutil.Image {
	if imageSpec != nil && imageSpec.Custom != "" {
		return dorisv1alpha1.TransformImage(imageSpec)
	}
	productVersion := constants.DefaultProductVersion
	pullSecretName := ""
	if imageSpec != nil {
		if imageSpec.ProductVersion != "" {
			productVersion = imageSpec.ProductVersion
		}
		pullSecretName = imageSpec.PullSecretName
	}
	return &opgoutil.Image{
		Custom:         fmt.Sprintf("%s:%s-%s", constants.OfficialImageRepository, componentType, productVersion),
		PullPolicy:     GetPullPolicy(imageSpec),
		PullSecretName: pullSecretName,
	}
}

// GetInitContainerImage returns the image to use for init containers
func GetInitContainerImage() string {
	return constants.DefaultInitImage
}

// GetPullPolicy returns the image pull policy to use
func GetPullPolicy(imageSpec *dorisv1alpha1.ImageSpec) corev1.PullPolicy {
	if imageSpec == nil || imageSpec.PullPolicy == nil {
		return corev1.PullIfNotPresent
	}
	return *imageSpec.PullPolicy
}

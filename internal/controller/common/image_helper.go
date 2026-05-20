package common

import (
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// GetImage builds the unified image for all Doris components.
// All components (FE, BE, Broker) share a single image; the component type
// is passed to satisfy caller conventions but does not affect the result.
func GetImage(imageSpec *dorisv1alpha1.ImageSpec, _ constants.ComponentType) *opgoutil.Image {
	return dorisv1alpha1.TransformImage(imageSpec)
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

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
// When imageSpec is nil (no image configured in the CR), it falls back to the
// official apache/doris per-component images (e.g. apache/doris:fe-2.1.8).
//
// When imageSpec is provided, it delegates to TransformImage which builds the
// unified quay.io/zncdatadev/doris:<version>-kubedoop<kubedoop-version> image.
//
// TODO: Once the custom unified image is production-ready, remove the nil
// fallback and always use TransformImage.
func GetImage(imageSpec *dorisv1alpha1.ImageSpec, componentType constants.ComponentType) *opgoutil.Image {
	if imageSpec == nil {
		return &opgoutil.Image{
			Custom:     fmt.Sprintf("%s:%s-%s", constants.OfficialImageRepository, componentType, constants.DefaultProductVersion),
			PullPolicy: corev1.PullIfNotPresent,
		}
	}
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

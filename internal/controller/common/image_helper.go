package common

import (
	"fmt"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	corev1 "k8s.io/api/core/v1"
)

// GetComponentImage returns the appropriate image for a specific component (BE or FE)
func GetComponentImage(imageSpec *dorisv1alpha1.ImageSpec, componentType constants.ComponentType) string {
	// Default component images
	defaultImages := map[constants.ComponentType]string{
		constants.ComponentTypeFE: constants.DefaultFEImage,
		constants.ComponentTypeBE: constants.DefaultBEImage,
	}

	// Return default image if imageSpec is nil
	if imageSpec == nil {
		return defaultImages[componentType]
	}

	// If custom image is specified, use it
	if imageSpec.Custom != "" {
		return imageSpec.Custom
	}

	// Otherwise construct component-specific image
	repo := constants.DorisRepository
	if imageSpec.Repo != "" {
		repo = imageSpec.Repo
	}

	version := constants.DefaultDorisVersion
	if imageSpec.ProductVersion != "" {
		version = imageSpec.ProductVersion
	}

	// Format based on component type
	switch componentType {
	case constants.ComponentTypeFE:
		return fmt.Sprintf(constants.FEImageFormat, repo, version)
	case constants.ComponentTypeBE:
		return fmt.Sprintf(constants.BEImageFormat, repo, version)
	default:
		return defaultImages[componentType]
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

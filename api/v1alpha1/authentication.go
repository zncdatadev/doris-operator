package v1alpha1

type AuthenticationSpec struct {
	// +kubebuilder:validation:Required
	AuthenticationClass string `json:"authenticationClass,omitempty"`
}

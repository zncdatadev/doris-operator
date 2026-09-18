/*
Copyright 2025 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DorisClusterSpec defines the desired state of DorisCluster.d

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DorisCluster is the Schema for the dorisclusters API.
type DorisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DorisClusterSpec   `json:"spec,omitempty"`
	Status DorisClusterStatus `json:"status,omitempty"`
}

// DorisClusterStatus defines the observed state of DorisCluster
type DorisClusterStatus struct {
	commonsv1alpha1.GenericClusterStatus `json:",inline"`

	// URLs retains the legacy status contract while GenericClusterStatus owns
	// conditions, observedGeneration, and the role-group resource ledger.
	// +kubebuilder:validation:Optional
	URLs []StatusURL `json:"urls,omitempty"`

	// Generation, Name, and Type are retained for status compatibility with
	// clusters reconciled by the pre-Gen3 controller.
	// +kubebuilder:validation:Optional
	Generation int64 `json:"generation,omitempty"`

	// +kubebuilder:validation:Optional
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Optional
	Type string `json:"type,omitempty"`

	// +kubebuilder:validation:Optional
	// AuthInitialized indicates whether the admin user specified in authSecret
	// has been created and granted privileges in the Doris cluster.
	AuthInitialized bool `json:"authInitialized,omitempty"`

	// +kubebuilder:validation:Optional
	FrontendNodes []NodeStatus `json:"frontendNodes,omitempty"`

	// +kubebuilder:validation:Optional
	BackendNodes []NodeStatus `json:"backendNodes,omitempty"`

	// +kubebuilder:validation:Optional
	BrokerNodes []NodeStatus `json:"brokerNodes,omitempty"`
}

// StatusURL is a named URL exposed through the legacy status.urls field.
type StatusURL struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// NodeStatus represents the status of a Doris cluster node
type NodeStatus struct {
	// +kubebuilder:validation:Optional
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Optional
	Host string `json:"host,omitempty"`

	// +kubebuilder:validation:Optional
	// Role is the node role (follower/observer for FE, empty for BE/Broker)
	Role string `json:"role,omitempty"`

	// +kubebuilder:validation:Optional
	Alive bool `json:"alive,omitempty"`

	// +kubebuilder:validation:Optional
	// Phase is the node lifecycle phase: Registered / Decommissioning / Decommissioned / ForceDropped
	Phase string `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true

// DorisClusterList contains a list of DorisCluster.
type DorisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DorisCluster `json:"items"`
}

// AuthSecretSpec defines the admin credentials for Doris cluster management.
type AuthSecretSpec struct {
	// +kubebuilder:validation:Required
	// Name of the Secret in the same namespace as the DorisCluster.
	// The Secret should be of type `kubernetes.io/basic-auth` with keys:
	//   - username: the admin user name (defaults to "root" if not set)
	//   - password: the admin user password
	SecretName string `json:"secretName"`
}

// DorisClusterSpec defines the desired state of DorisCluster
type DorisClusterSpec struct {
	// +kubebuilder:validation:Optional
	Image *ImageSpec `json:"image"`

	// +kubebuilder:validation:Optional
	ClusterConfig *ClusterConfigSpec `json:"clusterConfig,omitempty"`

	// +kubebuilder:validation:Optional
	ClusterOperationSpec *commonsv1alpha1.ClusterOperationSpec `json:"clusterOperation,omitempty"`

	// +kubebuilder:validation:Required
	Frontend *RoleSpec `json:"frontend,omitempty"`

	// +kubebuilder:validation:Required
	Backend *RoleSpec `json:"backend,omitempty"`

	// +kubebuilder:validation:Optional
	Broker *RoleSpec `json:"broker,omitempty"`

	// +kubebuilder:validation:Optional
	// AuthSecret references a Secret containing the Doris cluster admin credentials.
	// The Secret must be of type `kubernetes.io/basic-auth` with keys `username` and `password`.
	// If configured, the operator will use these credentials to connect to Doris FE for scale management.
	// If the specified user does not exist in Doris, the operator will create it with NODE_PRIV
	// and GRANT_PRIV privileges on first cluster initialization.
	// If not configured, the operator defaults to root with an empty password.
	AuthSecret *AuthSecretSpec `json:"authSecret,omitempty"`
}

type ClusterConfigSpec struct {

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="cluster.local"
	ClusterDomain string `json:"clusterDomain,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="example.com"
	IngressHost string `json:"ingressHost,omitempty"`

	// Name of the Vector aggregator [discovery ConfigMap].
	// It must contain the key `ADDRESS` with the address of the Vector aggregator.
	// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
	// to learn how to configure log aggregation with Vector.

	// +kubebuilder:validation:Optional
	VectorAggregatorConfigMapName *string `json:"vectorAggregatorConfigMapName,omitempty"`

	// +kubebuilder:validation:Optional
	Authentication []AuthenticationSpec `json:"authentication,omitempty"`

	// +kubebuilder:validation:Optional
	ScaleDownPolicy *ScaleDownPolicySpec `json:"scaleDownPolicy,omitempty"`
}

// ScaleDownPolicySpec defines the scale-down policy for Doris cluster components.
type ScaleDownPolicySpec struct {
	// BackendStrategy selects how the operator removes backend nodes. The
	// default decommission strategy preserves data but Doris rejects it when the
	// remaining BE count cannot satisfy a partition's replication allocation.
	// Use force-drop only for an intentionally destructive capacity reduction.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=decommission;force-drop
	// +kubebuilder:default=decommission
	BackendStrategy string `json:"backendStrategy,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="2h"
	// DecommissionTimeout is the maximum duration to wait for BE decommission to complete.
	// After this timeout, the operator will force-drop the node instead of waiting for data migration.
	DecommissionTimeout *metav1.Duration `json:"decommissionTimeout,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=drop-observer
	// +kubebuilder:default=drop-observer
	FrontendStrategy string `json:"frontendStrategy,omitempty"`
}

type RoleSpec struct {
	// +kubebuilder:validation:Optional
	Config *ConfigSpec `json:"config,omitempty"`

	// +kubebuilder:validation:Optional
	RoleGroups map[string]RoleGroupSpec `json:"roleGroups,omitempty"`

	// +kubebuilder:validation:Optional
	RoleConfig *commonsv1alpha1.RoleConfigSpec `json:"roleConfig,omitempty"`

	*commonsv1alpha1.OverridesSpec `json:",inline"`
}

type RoleGroupSpec struct {
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`

	// +kubebuilder:validation:Optional
	Config *ConfigSpec `json:"config,omitempty"`

	*commonsv1alpha1.OverridesSpec `json:",inline"`
}
type ConfigSpec struct {
	// Keep these fields local instead of embedding the v0.13 commons type. The Doris v1alpha1
	// wire contract predates the framework's stricter gracefulShutdownTimeout validation and
	// must continue accepting legacy values such as an explicit empty string and "0s".
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=object
	Affinity *k8sruntime.RawExtension `json:"affinity,omitempty"`

	// +kubebuilder:validation:Optional
	GracefulShutdownTimeout *string `json:"gracefulShutdownTimeout,omitempty"`

	// +kubebuilder:validation:Optional
	Logging *commonsv1alpha1.LoggingSpec `json:"logging,omitempty"`

	// +kubebuilder:validation:Optional
	Resources *commonsv1alpha1.ResourcesSpec `json:"resources,omitempty"`
}

// GetSpec projects the typed Doris role fields onto the generic shape consumed
// by the Gen3 reconciler.
func (d *DorisCluster) GetSpec() *commonsv1alpha1.GenericClusterSpec {
	return d.Spec.ToGenericSpec()
}

// GetStatus returns the embedded generic status in place so framework updates
// do not overwrite Doris-specific or legacy status fields.
func (d *DorisCluster) GetStatus() *commonsv1alpha1.GenericClusterStatus {
	return &d.Status.GenericClusterStatus
}

// VectorAggregatorConfigMapName exposes the configured Vector aggregator
// discovery ConfigMap to the generic reconciler.
func (d *DorisCluster) VectorAggregatorConfigMapName() string {
	if d == nil || d.Spec.ClusterConfig == nil || d.Spec.ClusterConfig.VectorAggregatorConfigMapName == nil {
		return ""
	}

	return *d.Spec.ClusterConfig.VectorAggregatorConfigMapName
}

// ToGenericSpec adapts the flat, typed Doris spec to GenericClusterSpec.
func (s *DorisClusterSpec) ToGenericSpec() *commonsv1alpha1.GenericClusterSpec {
	result := &commonsv1alpha1.GenericClusterSpec{
		Roles: make(map[string]commonsv1alpha1.RoleSpec),
	}
	if s == nil {
		return result
	}

	result.ClusterOperation = s.ClusterOperationSpec
	result.Image = adaptImageSpec(s.Image)

	if s.Frontend != nil {
		result.Roles["fe"] = adaptRoleSpec(s.Frontend)
	}
	if s.Backend != nil {
		result.Roles["be"] = adaptRoleSpec(s.Backend)
	}
	if s.Broker != nil {
		result.Roles["broker"] = adaptRoleSpec(s.Broker)
	}

	return result
}

// adaptImageSpec intentionally leaves the structured image coordinates empty.
// Doris declares a different official image for each role; carrying the flat
// spec's repository or version into GenericClusterSpec would make the framework
// discard those role declarations when ProductName is empty. The handlers remain
// responsible for deriving apache/doris:<role>-<productVersion> from the original
// Doris spec, while these cluster-wide transport settings still reach every pod.
func adaptImageSpec(image *ImageSpec) *commonsv1alpha1.ImageSpec {
	if image == nil {
		return nil
	}

	result := &commonsv1alpha1.ImageSpec{
		Custom:         image.Custom,
		PullSecretName: image.PullSecretName,
	}
	if image.PullPolicy != nil {
		result.PullPolicy = *image.PullPolicy
	}

	return result
}

func adaptRoleSpec(role *RoleSpec) commonsv1alpha1.RoleSpec {
	result := commonsv1alpha1.RoleSpec{
		RoleConfig: role.RoleConfig,
		Config:     adaptConfigSpec(role.Config),
		RoleGroups: make(map[string]commonsv1alpha1.RoleGroupSpec, len(role.RoleGroups)),
	}
	if role.OverridesSpec != nil {
		result.ConfigOverrides = role.ConfigOverrides
		result.EnvOverrides = role.EnvOverrides
		result.CliOverrides = role.CliOverrides
		result.PodOverrides = role.PodOverrides
	}

	for name, roleGroup := range role.RoleGroups {
		result.RoleGroups[name] = adaptRoleGroupSpec(roleGroup)
	}

	return result
}

func adaptRoleGroupSpec(roleGroup RoleGroupSpec) commonsv1alpha1.RoleGroupSpec {
	result := commonsv1alpha1.RoleGroupSpec{
		Replicas: roleGroup.Replicas,
		Config:   adaptConfigSpec(roleGroup.Config),
	}
	if roleGroup.OverridesSpec != nil {
		result.ConfigOverrides = roleGroup.ConfigOverrides
		result.EnvOverrides = roleGroup.EnvOverrides
		result.CliOverrides = roleGroup.CliOverrides
		result.PodOverrides = roleGroup.PodOverrides
	}

	return result
}

func adaptConfigSpec(config *ConfigSpec) *commonsv1alpha1.RoleGroupConfigSpec {
	if config == nil {
		return nil
	}

	adapted := &commonsv1alpha1.RoleGroupConfigSpec{
		Affinity:                config.Affinity.DeepCopy(),
		GracefulShutdownTimeout: config.GracefulShutdownTimeout,
		Logging:                 config.Logging.DeepCopy(),
		Resources:               config.Resources.DeepCopy(),
	}
	if adapted.GracefulShutdownTimeout != nil && *adapted.GracefulShutdownTimeout == "" {
		// Gen 2 treated an explicit empty value as unset. The effective Kubernetes default is
		// still 30 seconds, but passing it through would make the v0.13 handler reject the role.
		adapted.GracefulShutdownTimeout = nil
	}
	if adapted.Resources != nil &&
		adapted.Resources.Storage != nil &&
		adapted.Resources.Storage.StorageClass != nil &&
		*adapted.Resources.Storage.StorageClass == "" {
		// The Doris v1alpha1 API exposed storageClass as a value string before
		// operator-go v0.13, where an explicit empty value meant "unset/inherit".
		// Preserve that wire-level contract instead of adopting the new commons
		// meaning ("disable dynamic provisioning") without an API version bump.
		adapted.Resources.Storage.StorageClass = nil
	}

	return adapted
}
func init() {
	SchemeBuilder.Register(&DorisCluster{}, &DorisClusterList{})
}

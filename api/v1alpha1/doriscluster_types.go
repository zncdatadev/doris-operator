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
	"github.com/zncdatadev/operator-go/pkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	status.Status `json:",inline"`

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
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=decommission;force-drop
	// +kubebuilder:default=decommission
	BackendStrategy string `json:"backendStrategy,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default="2h"
	// DecommissionTimeout is the maximum duration to wait for BE decommission to complete.
	// After this timeout, the operator will force-drop the node instead of waiting for data migration.
	// NOTE: This field is reserved for future implementation; currently decommission will wait indefinitely.
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
	*commonsv1alpha1.RoleGroupConfigSpec `json:",inline"`
}

func init() {
	SchemeBuilder.Register(&DorisCluster{}, &DorisClusterList{})
}

package common

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	FEConfigFilename = "fe.conf"
	BEConfigFilename = "be.conf"
)

// ConfigMapBuilder is the common builder for Doris ConfigMaps
type ConfigMapBuilder struct {
	builder.ConfigMapBuilder
	client        *client.Client
	componentType constants.ComponentType
	clusterName   string
	roleGroupInfo *reconciler.RoleGroupInfo
	dorisCluster  *dorisv1alpha1.DorisCluster
	overrides     *commonsv1alpha1.OverridesSpec
	roleConfig    *commonsv1alpha1.RoleGroupConfigSpec
	ctx           context.Context
	component     ConfigMapComponentBuilder
}

// NewConfigMapBuilder creates a new ConfigMapBuilder with common configuration
func NewConfigMapBuilder(
	ctx context.Context,
	client *client.Client,
	componentType constants.ComponentType,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
	component ConfigMapComponentBuilder,
) builder.ConfigBuilder {
	return &ConfigMapBuilder{
		ConfigMapBuilder: *builder.NewConfigMapBuilder(
			client,
			roleGroupInfo.GetFullName(),
			func(o *builder.Options) {
				o.Labels = roleGroupInfo.GetLabels()
				o.Annotations = roleGroupInfo.GetAnnotations()
			},
		),
		client:        client,
		componentType: componentType,
		clusterName:   roleGroupInfo.GetClusterName(),
		roleGroupInfo: roleGroupInfo,
		dorisCluster:  dorisCluster,
		overrides:     overrides,
		roleConfig:    roleConfig,
		ctx:           ctx,
		component:     component,
	}
}

// ConfigMapComponentBuilder defines methods that should be implemented by BE/FE specific builders
type ConfigMapComponentBuilder interface {
	// BuildConfig returns component-specific configuration content
	BuildConfig() (map[string]string, error)
}

// Build constructs the ConfigMap object combining common and component-specific configurations
func (b *ConfigMapBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Get component-specific configurations
	configs, err := b.component.BuildConfig()
	if err != nil {
		return nil, err
	}

	// Add configurations to ConfigMap
	for filename, content := range configs {
		b.AddItem(filename, content)
	}

	// Apply any overrides from the spec
	if b.overrides != nil && b.overrides.ConfigOverrides != nil {
		for filename, overrides := range b.overrides.ConfigOverrides {
			if content, ok := overrides[filename]; ok {
				b.AddItem(filename, content)
			}
		}
	}

	return b.GetObject(), nil
}

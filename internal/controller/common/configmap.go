package common

import (
	"context"

	"emperror.dev/errors"
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	FEConfigFilename = constants.FEConfigFilename
	BEConfigFilename = constants.BEConfigFilename
)

// ConfigMapBuilder is the common builder for Doris ConfigMaps
type ConfigMapBuilder struct {
	builder.ConfigMapBuilder
	client                              *client.Client
	componentType                       constants.ComponentType
	clusterName                         string
	roleGroupInfo                       *reconciler.RoleGroupInfo
	dorisCluster                        *dorisv1alpha1.DorisCluster
	overrides                           *commonsv1alpha1.OverridesSpec
	roleConfig                          *commonsv1alpha1.RoleGroupConfigSpec
	ctx                                 context.Context
	component                           ConfigMapComponentBuilder
	vectorvectorAggregatorConfigMapName string
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
	vectorvectorAggregatorConfigMapName string,
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
		client:                              client,
		componentType:                       componentType,
		clusterName:                         roleGroupInfo.GetClusterName(),
		roleGroupInfo:                       roleGroupInfo,
		dorisCluster:                        dorisCluster,
		overrides:                           overrides,
		roleConfig:                          roleConfig,
		ctx:                                 ctx,
		component:                           component,
		vectorvectorAggregatorConfigMapName: vectorvectorAggregatorConfigMapName,
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

	// vector config
	if b.roleConfig != nil && IsVectorEnable(b.roleConfig.Logging) {
		if vectorConfig, err := b.buildVectorConfig(ctx); err != nil {
			return nil, err
		} else if vectorConfig != "" {
			b.AddItem(builder.VectorConfigFileName, vectorConfig) // vector.yaml
		}
	}

	return b.GetObject(), nil
}

// vector config
func (b *ConfigMapBuilder) buildVectorConfig(ctx context.Context) (string, error) {
	if b.roleConfig != nil && b.roleConfig.Logging != nil && b.roleConfig.Logging.EnableVectorAgent != nil {
		if b.vectorvectorAggregatorConfigMapName == "" {
			return "", errors.New("vector is enabled but vectorAggregatorConfigMapName is not set")
		}
		if *b.roleConfig.Logging.EnableVectorAgent { // corrected from roleGroupConfig to roleConfig
			s, err := productlogging.MakeVectorYaml(
				ctx,
				b.Client.Client,
				b.Client.GetOwnerNamespace(),
				b.ClusterName,
				b.RoleName,
				b.RoleGroupName,
				b.vectorvectorAggregatorConfigMapName,
			)
			if err != nil {
				return "", err
			}
			return s, nil
		}
	}
	return "", nil
}

func GetVectorConfigMapName(cluster *dorisv1alpha1.DorisCluster) string {
	if cluster == nil {
		return ""
	}
	if cluster.Spec.ClusterConfig != nil && *cluster.Spec.ClusterConfig.VectorAggregatorConfigMapName != "" {
		return *cluster.Spec.ClusterConfig.VectorAggregatorConfigMapName
	}
	return ""
}

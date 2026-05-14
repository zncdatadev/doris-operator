package broker

import (
	"context"
	"fmt"
	"strings"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// BrokerConfigMapBuilder implements common.ConfigMapComponentBuilder
type BrokerConfigMapBuilder struct {
	*builder.ConfigMapBuilder
}

// NewBrokerConfigMapReconciler creates a Broker ConfigMap reconciler
func NewBrokerConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
) reconciler.ResourceReconciler[builder.ConfigBuilder] {
	brokerBuilder := &BrokerConfigMapBuilder{
		ConfigMapBuilder: builder.NewConfigMapBuilder(
			client,
			roleGroupInfo.GetFullName(),
			func(o *builder.Options) {
				o.Labels = roleGroupInfo.GetLabels()
				o.Annotations = roleGroupInfo.GetAnnotations()
			}),
	}
	commonBuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constants.ComponentTypeBroker,
		roleGroupInfo,
		overrides,
		roleConfig,
		dorisCluster,
		brokerBuilder,
		common.GetVectorConfigMapName(dorisCluster),
	)
	return reconciler.NewGenericResourceReconciler(client, commonBuilder)
}

// BuildConfig returns component-specific configuration content
func (b *BrokerConfigMapBuilder) BuildConfig(_ context.Context) (map[string]string, error) {
	configs := make(map[string]string)

	// Default Broker configuration
	brokerConfig := []string{
		"sys_log_level = INFO",
		fmt.Sprintf("broker_ipc_port = %d", constants.BrokerIpcPort),
		"client_expire_seconds = 3600",
		"enable_fqdn_mode = true",
	}

	configs[constants.BrokerConfigFilename] = strings.Join(brokerConfig, "\n")
	return configs, nil
}

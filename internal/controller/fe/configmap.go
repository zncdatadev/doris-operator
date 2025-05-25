package fe

import (
	"context"
	"strings"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// FEConfigMapBuilder implements common.ConfigMapComponentBuilder
type FEConfigMapBuilder struct {
	*builder.ConfigMapBuilder
	overrides  *commonsv1alpha1.OverridesSpec
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec
}

func NewFEConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
) reconciler.ResourceReconciler[builder.ConfigBuilder] {
	feBuilder := &FEConfigMapBuilder{
		overrides:  overrides,
		roleConfig: roleConfig,
	}
	commonBuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constants.ComponentTypeFE,
		roleGroupInfo,
		overrides,
		roleConfig,
		dorisCluster,
		feBuilder,
		common.GetVectorConfigMapName(dorisCluster),
	)
	return reconciler.NewGenericResourceReconciler(client, commonBuilder)
}

// BuildConfig returns component-specific configuration content
func (b *FEConfigMapBuilder) BuildConfig() (map[string]string, error) {
	configs := make(map[string]string)

	// Default FE configuration
	feConfig := []string{
		// Shell environment variables
		"CUR_DATE=`date +%Y%m%d-%H%M%S`",
		// Log directory configuration
		"LOG_DIR=/kubedoop/log",
		// Java options configuration
		"JAVA_OPTS=-Xss4m -Xmx8192m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$CUR_DATE:time",
		"JAVA_OPTS_FOR_JDK_9=-Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$CUR_DATE:time",
		// System log configuration
		"sys_log_level = INFO",
		"sys_log_mode = NORMAL",
		// JDBC driver configuration
		"# jdbc_drivers_dir = ${DORIS_HOME}/jdbc_drivers",
		// FE service ports configuration
		"http_port = 8030",
		"rpc_port = 9020",
		"query_port = 9030",
		"edit_log_port = 9010",
		// Advanced configuration
		"max_conn_per_be = 1024",
		"disable_storage_medium_check = false",
	}

	configs[string(constants.FEConfigFilename)] = strings.Join(feConfig, "\n")
	configs[string(constants.FELog4j2ConfigFilename)] = b.log4j2ConfigContent()
	return configs, nil
}

// only fe add log4j2-spring.xml
func (b *FEConfigMapBuilder) log4j2ConfigContent() string {
	if b.roleConfig != nil && b.roleConfig.Logging != nil && b.roleConfig.Logging.Containers != nil {
		if mainContainerLogging, ok := b.roleConfig.Logging.Containers[string(constants.ComponentTypeFE)]; ok {
			loggingSpec := &mainContainerLogging
			loggingConfig, err := productlogging.NewConfigGenerator(
				loggingSpec,
				string(constants.ComponentTypeFE),
				constants.FELogFileName,
				productlogging.LogTypeLog4j2,
			)
			if err != nil {
				return ""
			}

			if logConfig, err := loggingConfig.Content(); err == nil {
				return logConfig
			} else {
				return ""
			}
		}
	}
	return ""
}

// GetPriorityNetworks returns the priority networks configuration for FE
func GetPriorityNetworks() string {
	return "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
}

// Unused function commented out to fix lint errors
/*
func formatConfig(configs []string) string {
	var result string
	for _, line := range configs {
		result += line + "\n"
	}
	return result
}
*/

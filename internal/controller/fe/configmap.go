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
		// FE service ports configuration
		"http_port = 8030",
		"rpc_port = 9020",
		"query_port = 9030",
		"edit_log_port = 9010",
		"arrow_flight_sql_port = -1",
		// System log configuration
		"sys_log_level = INFO",
		"sys_log_mode = NORMAL",
		"enable_fqdn_mode = true",
		// Java options configuration for different JDK versions
		"JAVA_OPTS=\"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$LOG_DIR/fe.gc.log.$CUR_DATE -Dlog4j2.formatMsgNoLookups=tru\"",
		"JAVA_OPTS_FOR_JDK_9=\"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xlog:gc*:$LOG_DIR/fe.gc.log.$CUR_DATE:time -Dlog4j2.formatMsgNoLookups=true\"",
		"JAVA_OPTS_FOR_JDK_17=\"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -XX:+UseG1GC -Xmx8192m -Xms8192m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_DIR/ -Xlog:gc*:$LOG_DIR/fe.gc.log.$CUR_DATE:time\"",
		"enable_fqdn_mode = true",
	}

	configs[string(constants.FEConfigFilename)] = strings.Join(feConfig, "\n")
	// configs[string(constants.FELog4j2ConfigFilename)] = b.log4j2ConfigContent()
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

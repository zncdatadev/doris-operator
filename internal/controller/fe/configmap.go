package fe

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// FEConfigMapBuilder implements common.ConfigMapComponentBuilder
type FEConfigMapBuilder struct {
	*builder.ConfigMapBuilder
}

func NewFEConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
) reconciler.ResourceReconciler[builder.ConfigBuilder] {
	feBuilder := &FEConfigMapBuilder{}
	commonBuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constants.ComponentTypeFE,
		roleGroupInfo,
		overrides,
		roleConfig,
		dorisCluster,
		feBuilder,
	)
	return reconciler.NewGenericResourceReconciler(client, commonBuilder)
}

func (b *FEConfigMapBuilder) BuildConfig() (map[string]string, error) {
	configs := make(map[string]string)

	// FE 默认配置
	feConfig := []string{
		// Shell 变量设置
		"CUR_DATE=`date +%Y%m%d-%H%M%S`",
		// 日志目录配置
		"LOG_DIR=${DORIS_HOME}/log",
		// Java 选项配置
		"JAVA_OPTS=-Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$CUR_DATE",
		// JDK 9+ Java 选项配置
		"JAVA_OPTS_FOR_JDK_9=-Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xlog:gc*:$DORIS_HOME/log/fe.gc.log.$CUR_DATE:time",
		// 系统日志配置
		"sys_log_level = INFO",
		"sys_log_mode = NORMAL",
		// JDBC 驱动配置
		"# jdbc_drivers_dir = ${DORIS_HOME}/jdbc_drivers",
		// FE 服务端口配置
		"http_port = 8030",
		"rpc_port = 9020",
		"query_port = 9030",
		"edit_log_port = 9010",
		"enable_fqdn_mode = true",
	}

	configs[common.FEConfigFilename] = formatConfig(feConfig)
	return configs, nil
}

// GetPriorityNetworks returns the priority networks configuration for FE
func GetPriorityNetworks() string {
	return "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
}

// formatConfig converts a slice of config lines to a single string
func formatConfig(configs []string) string {
	var result string
	for _, line := range configs {
		result += line + "\n"
	}
	return result
}

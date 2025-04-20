package be

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

// BEConfigMapBuilder implements common.ConfigMapComponentBuilder
type BEConfigMapBuilder struct {
	*builder.ConfigMapBuilder
}

func NewBEConfigMapReconciler(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
) reconciler.ResourceReconciler[builder.ConfigBuilder] {
	beBuilder := &BEConfigMapBuilder{}
	commonBuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constants.ComponentTypeBE,
		roleGroupInfo,
		overrides,
		roleConfig,
		dorisCluster,
		beBuilder,
	)
	return reconciler.NewGenericResourceReconciler(client, commonBuilder)
}

func (b *BEConfigMapBuilder) BuildConfig() (map[string]string, error) {
	configs := make(map[string]string)

	// BE 默认配置
	beConfig := []string{
		// Shell 变量设置
		"CUR_DATE=`date +%Y%m%d-%H%M%S`",
		"PPROF_TMPDIR=$DORIS_HOME/log/",
		// Java 选项配置
		"JAVA_OPTS=-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xloggc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000",
		// JDK 9+ Java 选项配置
		"JAVA_OPTS_FOR_JDK_9=-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xlog:gc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000",
		// Jemalloc 配置
		"JEMALLOC_CONF=percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false",
		"JEMALLOC_PROF_PRFIX=",
		// 系统日志级别
		"sys_log_level = INFO",
		// BE 服务端口配置
		"be_port = 9060",
		"webserver_port = 8040",
		"heartbeat_service_port = 9050",
		"brpc_port = 8060",
	}

	configs[common.BEConfigFilename] = formatConfig(beConfig)
	return configs, nil
}

// GetPriorityNetworks returns the priority networks configuration for BE
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

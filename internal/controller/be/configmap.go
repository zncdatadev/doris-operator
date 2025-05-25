package be

import (
	"context"
	"strings"

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
		common.GetVectorConfigMapName(dorisCluster),
	)
	return reconciler.NewGenericResourceReconciler(client, commonBuilder)
}

// BuildConfig returns component-specific configuration content
func (b *BEConfigMapBuilder) BuildConfig() (map[string]string, error) {
	configs := make(map[string]string)

	// Default BE configuration
	beConfig := []string{
		// Shell environment variables
		"CUR_DATE=`date +%Y%m%d-%H%M%S`",
		// Profile directory configuration
		"PPROF_TMPDIR=$DORIS_HOME/log/",
		// Java options configuration
		"JAVA_OPTS=-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xloggc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000",
		// Java options for JDK 9+
		"JAVA_OPTS_FOR_JDK_9=-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Xlog:gc:$DORIS_HOME/log/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000",
		// Jemalloc configuration
		"JEMALLOC_CONF=percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:15000,dirty_decay_ms:15000,oversize_threshold:0,lg_tcache_max:20,prof:false,lg_prof_interval:32,lg_prof_sample:19,prof_gdump:false,prof_accum:false,prof_leak:false,prof_final:false",
		// BE service ports configuration
		"# See more BE configurations at https://doris.apache.org/docs/admin-manual/config/be-config",
		"be_port = 9060",
		"webserver_port = 8040",
		"heartbeat_service_port = 9050",
		"brpc_port = 8060",
		// Storage configuration
		"storage_root_path = ${DORIS_HOME}/storage",
		// System settings
		"sys_log_level = INFO",
		"be_service_type = LOCAL",
	}

	configs["be.conf"] = strings.Join(beConfig, "\n")
	return configs, nil
}

// GetPriorityNetworks returns the priority networks configuration for BE
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

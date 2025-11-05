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
	beBuilder := &BEConfigMapBuilder{
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
func (b *BEConfigMapBuilder) BuildConfig(ctx context.Context) (map[string]string, error) {
	configs := make(map[string]string)
	beConfig := []string{
		// Default BE configuration
		"CUR_DATE=`date +%Y%m%d-%H%M%S`",
		"LOG_DIR=/kubedoop/log",
		"JAVA_OPTS=\"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xloggc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -Darrow.enable_null_check_for_get=false\"",
		"JAVA_OPTS_FOR_JDK_9=\"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xlog:gc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_null_check_for_get=false\"",
		"JAVA_OPTS_FOR_JDK_17=\"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xlog:gc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_null_check_for_get=false\"",
		"JEMALLOC_CONF=\"percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof:true,prof_active:false,lg_prof_interval:-1\"",
		"JEMALLOC_PROF_PRFIX=\"jemalloc_heap_profile_\"",
		"be_port=9060",
		"webserver_port=8040",
		"heartbeat_service_port=9050",
		"brpc_port=8060",
		"arrow_flight_sql_port=-1",
		"enable_https=false",
		"ssl_certificate_path=\"$DORIS_HOME/conf/cert.pem\"",
		"ssl_private_key_path=\"$DORIS_HOME/conf/key.pem\"",
		"sys_log_level=INFO",
		"aws_log_level=0",
		"AWS_EC2_METADATA_DISABLED=true",
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

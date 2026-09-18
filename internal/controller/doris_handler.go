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

package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	opconstant "github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	legacyAppName     = "doriscluster"
	legacyManagedBy   = "doris.kubedoop.dev"
	dorisLogDirectory = "/kubedoop/log"
	configQueryPort   = "query_port"
	configSysLogLevel = "sys_log_level"
	configValueInfo   = "INFO"
	configValueTrue   = "true"
	configCurrentDate = "CUR_DATE"
	configLogDir      = "LOG_DIR"
	configHTTPPort    = "http_port"
	configRPCPort     = "rpc_port"
	configEditLogPort = "edit_log_port"
	configArrowPort   = "arrow_flight_sql_port"
	configSysLogMode  = "sys_log_mode"
	configJavaOpts    = "JAVA_OPTS"
	configJavaOpts9   = "JAVA_OPTS_FOR_JDK_9"
	configJavaOpts17  = "JAVA_OPTS_FOR_JDK_17"
	configFQDNMode    = "enable_fqdn_mode"
	configJemalloc    = "JEMALLOC_CONF"
	configJemallocPre = "JEMALLOC_PROF_PRFIX"
	configBEPort      = "be_port"
	configBEWebPort   = "webserver_port"
	configBEHeartPort = "heartbeat_service_port"
	configBEBrpcPort  = "brpc_port"
	configHTTPSEnable = "enable_https"
	configSSLCert     = "ssl_certificate_path"
	configSSLKey      = "ssl_private_key_path"
	configAWSLogLevel = "aws_log_level"
	configAWSMetadata = "AWS_EC2_METADATA_DISABLED"
	configBrokerPort  = "broker_ipc_port"
	configClientTTL   = "client_expire_seconds"
)

// Operator-side RBAC for GenericReconciler plus Doris management credentials and LDAP lookup.
// CR body update/patch remains necessary because safe decommission progress is persisted in an
// annotation; workload pods themselves do not receive Kubernetes API permissions.
//
// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=doris.kubedoop.dev,resources=dorisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=authentication.kubedoop.dev,resources=authenticationclasses,verbs=get;list;watch

// DorisRoleGroupHandler is the Gen 3 resource seam for all Doris roles.  The framework owns
// reconciliation and apply ordering; this handler declares the product facts and preserves the
// workload identities created by the pre-v0.13 controller.
type DorisRoleGroupHandler struct {
	*reconciler.BaseRoleGroupHandler[*dorisv1alpha1.DorisCluster]
}

var _ reconciler.RoleGroupHandler[*dorisv1alpha1.DorisCluster] = &DorisRoleGroupHandler{}
var _ reconciler.RoleProvider[*dorisv1alpha1.DorisCluster] = &DorisRoleGroupHandler{}

func NewDorisRoleGroupHandler(scheme *runtime.Scheme) *DorisRoleGroupHandler {
	base := reconciler.NewBaseRoleGroupHandler[*dorisv1alpha1.DorisCluster](scheme)
	base.ConfigMountPath = constants.DefaultConfigMapPath
	// The official Apache Doris images and the privileged BE sysctl init container require root.
	// Keep the legacy execution contract until Doris ships a verified non-root image.
	base.WithoutDefaultSecurityContext()
	return &DorisRoleGroupHandler{BaseRoleGroupHandler: base}
}

func (h *DorisRoleGroupHandler) DeclareRoles(
	_ context.Context,
	_ ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
) (reconciler.RoleCatalog, error) {
	version := constants.DefaultProductVersion
	if cr.Spec.Image != nil && cr.Spec.Image.ProductVersion != "" {
		version = cr.Spec.Image.ProductVersion
	}

	commonEnv := []corev1.EnvVar{
		fieldEnv(constants.PodNameEnvVar, "metadata.name"),
		fieldEnv(constants.PodIPEnvVar, "status.podIP"),
		fieldEnv(constants.HostIPEnvVar, "status.hostIP"),
		fieldEnv(constants.PodNamespaceEnvVar, "metadata.namespace"),
		{Name: constants.ConfigMapPathEnvVar, Value: constants.DefaultConfigMapPath},
		{Name: constants.UserEnvVar, Value: constants.DefaultUser},
		{Name: constants.DorisRootEnvVar, Value: constants.DefaultDorisRoot},
		{Name: constants.FEAddrEnvVar, Value: cr.Name + "-fe" + constants.ServiceAccessSuffix},
		{Name: constants.FEQueryPortEnvVar, Value: constants.DefaultQueryPort},
	}

	feEnv := append([]corev1.EnvVar{}, commonEnv...)
	feEnv = append(feEnv, corev1.EnvVar{Name: constants.FEElectNumberEnvVar, Value: constants.DefaultElectNumber})

	return reconciler.RoleCatalog{
		string(constants.ComponentTypeFE): {
			Image:                    officialRoleImage(constants.ComponentTypeFE, version),
			MainContainerName:        constants.FEContainerName,
			ContainerPorts:           feContainerPorts(),
			ServicePorts:             servicePorts(feContainerPorts()),
			Command:                  []string{constants.FEEntrypoint},
			LivenessProbe:            tcpProbe(constants.FEQueryPort),
			ReadinessProbe:           httpProbe(constants.FEHttpPort),
			DataVolume:               &reconciler.DataVolume{Name: constants.FEMetadataVolume, MountPath: constants.FEMetadataPath},
			PublishNotReadyAddresses: true,
			Env:                      feEnv,
			ConfigDefaults:           defaultResources(constants.FEMemoryLimit, constants.FEStorageSize),
		},
		string(constants.ComponentTypeBE): {
			Image:                    officialRoleImage(constants.ComponentTypeBE, version),
			MainContainerName:        constants.BEContainerName,
			ContainerPorts:           beContainerPorts(),
			ServicePorts:             servicePorts(beContainerPorts()),
			Command:                  []string{constants.BEEntrypoint},
			LivenessProbe:            tcpProbe(constants.BEHeartbeatPort),
			ReadinessProbe:           httpProbe(constants.BEHttpPort),
			DataVolume:               &reconciler.DataVolume{Name: constants.BEStorageVolume, MountPath: constants.BEStoragePath},
			PublishNotReadyAddresses: true,
			Env:                      append([]corev1.EnvVar{}, commonEnv...),
			ConfigDefaults:           defaultResources(constants.BEMemoryLimit, constants.BEStorageSize),
		},
		string(constants.ComponentTypeBroker): {
			Image:                    officialRoleImage(constants.ComponentTypeBroker, version),
			MainContainerName:        constants.BrokerContainerName,
			ContainerPorts:           brokerContainerPorts(),
			ServicePorts:             servicePorts(brokerContainerPorts()),
			Command:                  []string{constants.BrokerEntrypoint},
			LivenessProbe:            tcpProbe(constants.BrokerIpcPort),
			ReadinessProbe:           tcpProbe(constants.BrokerIpcPort),
			PublishNotReadyAddresses: true,
			Env:                      append([]corev1.EnvVar{}, commonEnv...),
			ConfigDefaults:           defaultResources(constants.BrokerMemoryLimit, ""),
			Optional:                 true,
		},
	}, nil
}

func officialRoleImage(role constants.ComponentType, version string) *commonsv1alpha1.ImageSpec {
	return &commonsv1alpha1.ImageSpec{
		Custom: fmt.Sprintf("%s:%s-%s", constants.OfficialImageRepository, role, version),
	}
}

func defaultResources(memory, storage string) *commonsv1alpha1.RoleGroupConfigSpec {
	cpuMin := resource.MustParse(constants.DefaultCPURequest)
	cpuMax := resource.MustParse(constants.DefaultCPULimit)
	memLimit := resource.MustParse(memory)
	resources := &commonsv1alpha1.ResourcesSpec{
		CPU:    &commonsv1alpha1.CPUResource{Min: &cpuMin, Max: &cpuMax},
		Memory: &commonsv1alpha1.MemoryResource{Limit: &memLimit},
	}
	if storage != "" {
		capacity := resource.MustParse(storage)
		resources.Storage = &commonsv1alpha1.StorageResource{Capacity: &capacity}
	}
	return &commonsv1alpha1.RoleGroupConfigSpec{Resources: resources}
}

func fieldEnv(name, path string) corev1.EnvVar {
	return corev1.EnvVar{
		Name:      name,
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}},
	}
}

func tcpProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)}},
		InitialDelaySeconds: constants.DefaultInitialDelaySeconds,
		PeriodSeconds:       constants.DefaultPeriodSeconds,
	}
}

func httpProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: constants.HealthCheckPath,
			Port: intstr.FromInt32(port),
		}},
		InitialDelaySeconds: constants.DefaultInitialDelaySeconds,
		PeriodSeconds:       constants.DefaultPeriodSeconds,
	}
}

func feContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		containerPort(constants.FEHttpPortName, constants.FEHttpPort),
		containerPort(constants.FERpcPortName, constants.FERpcPort),
		containerPort(constants.FEQueryPortName, constants.FEQueryPort),
		containerPort(constants.FEEditLogPortName, constants.FEEditLogPort),
	}
}

func beContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		containerPort(constants.BERpcPortName, constants.BERpcPort),
		containerPort(constants.BEHttpPortName, constants.BEHttpPort),
		containerPort(constants.BEHeartbeatPortName, constants.BEHeartbeatPort),
		containerPort(constants.BEBrpcPortName, constants.BEBrpcPort),
	}
}

func brokerContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{containerPort(constants.BrokerIpcPortName, constants.BrokerIpcPort)}
}

func containerPort(name string, port int32) corev1.ContainerPort {
	return corev1.ContainerPort{Name: name, ContainerPort: port, Protocol: corev1.ProtocolTCP}
}

func servicePorts(ports []corev1.ContainerPort) []corev1.ServicePort {
	result := make([]corev1.ServicePort, 0, len(ports))
	for _, port := range ports {
		result = append(result, corev1.ServicePort{
			Name:       port.Name,
			Port:       port.ContainerPort,
			TargetPort: intstr.FromString(port.Name),
			Protocol:   port.Protocol,
		})
	}
	return result
}

// ResolveDorisRoleGroup supplies product config defaults beneath both user override levels.
func ResolveDorisRoleGroup(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.Contribution, error) {
	configOverrides, err := resolveDorisProductConfig(ctx, k8sClient, cr, buildCtx.RoleName)
	if err != nil {
		return nil, err
	}
	contribution := &reconciler.Contribution{
		ConfigOverrides: configOverrides,
		PodOverrides:    dorisProductPodOverrides(buildCtx.RoleName),
	}
	return contribution, nil
}

func resolveDorisProductConfig(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
	roleName string,
) (map[string]map[string]string, error) {
	config := defaultConfig(roleName)
	if roleName == string(constants.ComponentTypeFE) {
		ldapConfig, enabled, err := resolveLDAPConfig(ctx, k8sClient, cr)
		if err != nil {
			return nil, err
		}
		if enabled {
			config[constants.FEConfigFilename]["authentication_type"] = "ldap"
			config[constants.LDAPConfigFilename] = ldapConfig
		}
	}
	return config, nil
}

func dorisProductPodOverrides(roleName string) *corev1.PodTemplateSpec {
	if roleName != string(constants.ComponentTypeBE) {
		return nil
	}
	return &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		InitContainers: []corev1.Container{{
			Name:            constants.InitContainerName,
			Image:           constants.DefaultInitImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"sh", "-c", constants.BEInitCommand},
			SecurityContext: &corev1.SecurityContext{Privileged: ptr.To(true)},
		}},
	}}
}

func defaultConfig(roleName string) map[string]map[string]string {
	switch roleName {
	case string(constants.ComponentTypeFE):
		return map[string]map[string]string{constants.FEConfigFilename: {
			configCurrentDate: "`date +%Y%m%d-%H%M%S`",
			configLogDir:      dorisLogDirectory,
			configHTTPPort:    strconv.Itoa(constants.FEHttpPort),
			configRPCPort:     strconv.Itoa(constants.FERpcPort),
			configQueryPort:   strconv.Itoa(constants.FEQueryPort),
			configEditLogPort: strconv.Itoa(constants.FEEditLogPort),
			configArrowPort:   "-1",
			configSysLogLevel: configValueInfo,
			configSysLogMode:  "NORMAL",
			configJavaOpts:    `"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -Xloggc:$LOG_DIR/fe.gc.log.$CUR_DATE -Dlog4j2.formatMsgNoLookups=true"`,
			configJavaOpts9:   `"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -Xss4m -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xlog:gc*:$LOG_DIR/fe.gc.log.$CUR_DATE:time -Dlog4j2.formatMsgNoLookups=true"`,
			configJavaOpts17:  `"-Dfile.encoding=UTF-8 -Djavax.security.auth.useSubjectCredsOnly=false -XX:+UseG1GC -Xmx8192m -Xms8192m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_DIR/ -Xlog:gc*:$LOG_DIR/fe.gc.log.$CUR_DATE:time"`,
			configFQDNMode:    configValueTrue,
		}}
	case string(constants.ComponentTypeBE):
		return map[string]map[string]string{constants.BEConfigFilename: {
			configCurrentDate: "`date +%Y%m%d-%H%M%S`",
			configLogDir:      dorisLogDirectory,
			configJavaOpts:    `"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xloggc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -Darrow.enable_null_check_for_get=false"`,
			configJavaOpts9:   `"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xlog:gc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_null_check_for_get=false"`,
			configJavaOpts17:  `"-Dfile.encoding=UTF-8 -Xmx2048m -DlogPath=$LOG_DIR/jni.log -Xlog:gc:$LOG_DIR/be.gc.log.$CUR_DATE -Djavax.security.auth.useSubjectCredsOnly=false -Dsun.security.krb5.debug=true -Dsun.java.command=DorisBE -XX:-CriticalJNINatives --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_null_check_for_get=false"`,
			configJemalloc:    `"percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof:true,prof_active:false,lg_prof_interval:-1"`,
			configJemallocPre: `"jemalloc_heap_profile_"`,
			configBEPort:      strconv.Itoa(constants.BERpcPort),
			configBEWebPort:   strconv.Itoa(constants.BEHttpPort),
			configBEHeartPort: strconv.Itoa(constants.BEHeartbeatPort),
			configBEBrpcPort:  strconv.Itoa(constants.BEBrpcPort),
			configArrowPort:   "-1",
			configHTTPSEnable: "false",
			configSSLCert:     `"$DORIS_HOME/conf/cert.pem"`,
			configSSLKey:      `"$DORIS_HOME/conf/key.pem"`,
			configSysLogLevel: configValueInfo,
			configAWSLogLevel: "0",
			configAWSMetadata: configValueTrue,
		}}
	case string(constants.ComponentTypeBroker):
		return map[string]map[string]string{constants.BrokerConfigFilename: {
			configSysLogLevel: configValueInfo,
			configBrokerPort:  strconv.Itoa(constants.BrokerIpcPort),
			configClientTTL:   "3600",
			configFQDNMode:    configValueTrue,
		}}
	default:
		return map[string]map[string]string{}
	}
}

func resolveLDAPConfig(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
) (map[string]string, bool, error) {
	if cr.Spec.ClusterConfig == nil {
		return nil, false, nil
	}
	for _, auth := range cr.Spec.ClusterConfig.Authentication {
		authClass := &authv1alpha1.AuthenticationClass{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: auth.AuthenticationClass}, authClass); err != nil {
			return nil, false, fmt.Errorf("resolve AuthenticationClass %q: %w", auth.AuthenticationClass, err)
		}
		if authClass.Spec.AuthenticationProvider == nil || authClass.Spec.AuthenticationProvider.LDAP == nil {
			continue
		}
		ldap := authClass.Spec.AuthenticationProvider.LDAP
		if ldap.BindCredentials == nil || ldap.BindCredentials.SecretClass == "" {
			return nil, false, fmt.Errorf("AuthenticationClass %q LDAP bindCredentials.secretClass is required", auth.AuthenticationClass)
		}
		secret := &corev1.Secret{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: ldap.BindCredentials.SecretClass}, secret); err != nil {
			return nil, false, fmt.Errorf("resolve LDAP bind credentials Secret %q: %w", ldap.BindCredentials.SecretClass, err)
		}
		user, ok := secret.Data["user"]
		if !ok {
			return nil, false, fmt.Errorf("LDAP bind credentials Secret %q has no user key", secret.Name)
		}
		return map[string]string{
			"ldap_host":         ldap.Hostname,
			"ldap_port":         strconv.Itoa(ldap.Port),
			"ldap_admin_name":   string(user),
			"ldap_user_basedn":  ldap.SearchBase,
			"ldap_user_filter":  ldap.SearchFilter,
			"ldap_group_basedn": ldap.SearchBase,
		}, true, nil
	}
	return nil, false, nil
}

func (h *DorisRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *dorisv1alpha1.DorisCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, dorisCommonVolumes{})
	// The legacy controller never propagated DorisCluster metadata labels to generated
	// resources. Letting the v0.13 base handler do so would mutate every PodTemplate on
	// upgrade (and could unexpectedly opt workloads into label-driven admission policy).
	legacyBuildCtx, legacyPodOverrides, err := legacyCompatibleBuildContext(buildCtx)
	if err != nil {
		return nil, fmt.Errorf("restore Doris Gen 2 override semantics for %s/%s: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}
	legacyTerminationGracePeriod := prepareLegacyGracefulShutdownTimeout(legacyBuildCtx)
	legacyBuildCtx.ClusterLabels = nil
	resources, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, legacyBuildCtx)
	if err != nil {
		return nil, fmt.Errorf("build Doris %s/%s resources: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}

	if resources.StatefulSet == nil || len(resources.StatefulSet.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("base handler produced no StatefulSet main container for %s/%s", buildCtx.RoleName, buildCtx.RoleGroupName)
	}

	sts := resources.StatefulSet
	preserveLegacyPodDefaults(sts)
	sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
	sts.Spec.ServiceName = buildCtx.ResourceName
	selector := legacyRoleGroupSelector(buildCtx.ClusterName, buildCtx.RoleName, buildCtx.RoleGroupName)
	preserveLegacyResourceLabels(resources, selector)
	sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: maps.Clone(selector)}
	sts.Spec.Template.Labels = maps.Clone(selector)

	main := &sts.Spec.Template.Spec.Containers[0]
	preserveLegacyContainerResources(cr, buildCtx, main)
	preserveLegacyCommandAndArgs(main, buildCtx)
	preserveLegacyConfigVolume(sts)
	preserveLegacyVolumeMountOrder(main, buildCtx.RoleName)
	if legacyTerminationGracePeriod != nil {
		sts.Spec.Template.Spec.TerminationGracePeriodSeconds = legacyTerminationGracePeriod
	}
	mergedTemplate, err := applyLegacyPodOverrides(&sts.Spec.Template, legacyPodOverrides)
	if err != nil {
		return nil, fmt.Errorf("apply legacy podOverrides for %s/%s: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}
	sts.Spec.Template = *mergedTemplate

	// Use the framework's fixed Service slot as the StatefulSet governing service.  Naming it
	// exactly like the StatefulSet preserves the immutable serviceName used by every old cluster.
	resources.HeadlessService = nil
	if resources.Service != nil {
		resources.Service.Spec.Type = corev1.ServiceTypeClusterIP
		resources.Service.Spec.ClusterIP = corev1.ClusterIPNone
		resources.Service.Spec.ClusterIPs = []string{corev1.ClusterIPNone}
		resources.Service.Spec.PublishNotReadyAddresses = true
		resources.Service.Spec.Selector = maps.Clone(selector)
	}

	if metricsPort, targetPort, ok := metricsForRole(buildCtx.RoleName); ok {
		resources.MetricsService = builder.NewMetricsServiceBuilder(
			buildCtx.ResourceName,
			buildCtx.ClusterNamespace,
			metricsPort,
			maps.Clone(sts.Labels),
		).WithSelector(maps.Clone(selector)).WithTargetPortName(targetPort).WithPath("/metrics").Build()
		metricsLabels := maps.Clone(selector)
		metricsLabels["prometheus.io/scrape"] = configValueTrue
		replaceLabels(resources.MetricsService, metricsLabels)
	} else {
		resources.MetricsService = nil
	}

	legacyConfigData, err := buildLegacyConfigMapData(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, fmt.Errorf("build legacy Doris %s/%s config: %w", buildCtx.RoleName, buildCtx.RoleGroupName, err)
	}
	resources.ConfigMap.Data = legacyConfigData
	if err := preserveLegacyVectorConfig(ctx, k8sClient, cr, buildCtx, resources.ConfigMap); err != nil {
		return nil, err
	}

	return resources, nil
}

// prepareLegacyGracefulShutdownTimeout lets the strict v0.13 base builder construct the
// workload while retaining the Gen 2 duration conversion it used. An empty
// string is unset (and therefore effectively Kubernetes' 30-second default); every valid Go
// duration is truncated to whole seconds exactly as Gen 2 did. The old value is restored on the
// finished PodTemplate before legacy podOverrides are applied, so those overrides keep priority.
func prepareLegacyGracefulShutdownTimeout(buildCtx *reconciler.RoleGroupBuildContext) *int64 {
	config := buildCtx.EffectiveConfig()
	if config.GracefulShutdownTimeout == nil {
		return nil
	}

	timeout := *config.GracefulShutdownTimeout
	if timeout == "" {
		config = config.DeepCopy()
		config.GracefulShutdownTimeout = nil
		buildCtx.RoleGroupSpec.Config = config
		return nil
	}

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		// Leave malformed input untouched so the base handler returns its contextual error.
		return nil
	}
	legacySeconds := int64(duration.Seconds())
	if duration <= 0 {
		// v0.13 rejects non-positive durations before building. Use a harmless value and
		// restore the Gen 2 result below, including its Kubernetes validation behavior.
		config = config.DeepCopy()
		config.GracefulShutdownTimeout = ptr.To("1s")
		buildCtx.RoleGroupSpec.Config = config
	}
	return ptr.To(legacySeconds)
}

// preserveLegacyCommandAndArgs restores the pre-v0.13 override contract. The Gen 2 builder
// treated cliOverrides as a complete command replacement and cleared args, then applied
// podOverrides last. The v0.13 builder treats the same field as appended args, so leaving its
// output untouched would run the Doris entrypoint with tokens intended to replace it.
func preserveLegacyCommandAndArgs(
	main *corev1.Container,
	buildCtx *reconciler.RoleGroupBuildContext,
) {
	cliOverrides := legacyCLIOverrides(buildCtx)
	if len(cliOverrides) > 0 {
		main.Command = cliOverrides
		main.Args = []string{}
	} else {
		main.Command = slices.Clone(buildCtx.Declaration.Command)
		main.Args = []string{"$(" + constants.FEAddrEnvVar + ")"}
	}
}

// legacyCLIOverrides preserves the Gen 2 JSON merge behavior, where role-group slices were
// appended to role slices. operator-go v0.13 intentionally replaces slices at the higher layer.
func legacyCLIOverrides(buildCtx *reconciler.RoleGroupBuildContext) []string {
	var cli []string
	if buildCtx.RoleSpec != nil {
		cli = append(cli, buildCtx.RoleSpec.CliOverrides...)
	}
	cli = append(cli, buildCtx.RoleGroupSpec.CliOverrides...)
	return cli
}

func renderLegacyConfigFile(filename string, values map[string]string) string {
	orderedKeys := legacyConfigKeyOrder(filename)
	seen := make(map[string]struct{}, len(values))
	lines := make([]string, 0, len(values))
	separator := "="
	if filename == constants.BrokerConfigFilename {
		separator = " = "
	}
	appendKey := func(key string) {
		value, exists := values[key]
		if !exists {
			return
		}
		lines = append(lines, key+separator+value)
		seen[key] = struct{}{}
	}
	for _, key := range orderedKeys {
		appendKey(key)
	}
	remaining := make([]string, 0, len(values)-len(seen))
	for key := range values {
		if _, exists := seen[key]; !exists {
			remaining = append(remaining, key)
		}
	}
	sort.Strings(remaining)
	for _, key := range remaining {
		appendKey(key)
	}
	return strings.Join(lines, "\n")
}

func legacyConfigKeyOrder(filename string) []string {
	switch filename {
	case constants.FEConfigFilename:
		return []string{
			configCurrentDate, configLogDir, configHTTPPort, configRPCPort, configQueryPort,
			configEditLogPort, configArrowPort, configSysLogLevel, configSysLogMode,
			configJavaOpts, configJavaOpts9, configJavaOpts17, configFQDNMode,
		}
	case constants.BEConfigFilename:
		return []string{
			configCurrentDate, configLogDir, configJavaOpts, configJavaOpts9, configJavaOpts17,
			configJemalloc, configJemallocPre, configBEPort, configBEWebPort,
			configBEHeartPort, configBEBrpcPort, configArrowPort, configHTTPSEnable,
			configSSLCert, configSSLKey, configSysLogLevel, configAWSLogLevel,
			configAWSMetadata,
		}
	case constants.BrokerConfigFilename:
		return []string{configSysLogLevel, configBrokerPort, configClientTTL, configFQDNMode}
	case constants.LDAPConfigFilename:
		return []string{
			"ldap_host", "ldap_port", "ldap_admin_name", "ldap_user_basedn",
			"ldap_user_filter", "ldap_group_basedn",
		}
	default:
		return nil
	}
}

// preserveLegacyPodDefaults prevents the framework migration from changing the
// identity and environment of every Doris pod. The legacy controller left the
// service account unset (so Kubernetes used the namespace default account) and
// relied on Kubernetes' enableServiceLinks=true default. User podOverrides are
// applied after these compatibility defaults and therefore retain precedence.
func preserveLegacyPodDefaults(sts *appsv1.StatefulSet) {
	sts.Spec.Template.Spec.ServiceAccountName = ""
	enableServiceLinks := true
	sts.Spec.Template.Spec.EnableServiceLinks = &enableServiceLinks
}

func (h *DorisRoleGroupHandler) BuildRolePodDisruptionBudget(
	buildCtx *reconciler.RoleBuildContext,
) *policyv1.PodDisruptionBudget {
	pdb := h.BaseRoleGroupHandler.BuildRolePodDisruptionBudget(buildCtx)
	if pdb != nil {
		labels := legacyRoleSelector(buildCtx.ClusterName, buildCtx.RoleName)
		replaceLabels(pdb, labels)
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
	}
	return pdb
}

type dorisCommonVolumes struct{}

func (dorisCommonVolumes) Volumes() []corev1.Volume {
	return []corev1.Volume{
		{Name: constants.LogVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{
			Name: constants.PodinfoVolumeName,
			VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{
				{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.labels"}},
				{Path: "annotations", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.annotations"}},
			}}},
		},
	}
}

func (dorisCommonVolumes) VolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: constants.LogVolumeName, MountPath: dorisLogDirectory},
		{Name: constants.PodinfoVolumeName, MountPath: constants.PodinfoMountPath},
	}
}

func preserveLegacyConfigVolume(sts *appsv1.StatefulSet) {
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == reconciler.ConfigVolumeName {
			sts.Spec.Template.Spec.Volumes[i].Name = constants.ConfigVolumeName
		}
	}
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		main := &sts.Spec.Template.Spec.Containers[0]
		for i := range main.VolumeMounts {
			mount := &main.VolumeMounts[i]
			if mount.Name == reconciler.ConfigVolumeName &&
				mount.MountPath == constants.DefaultConfigMapPath {
				// The old Doris builder left this field false. ConfigMap volumes are
				// inherently read-only, but changing the field to true still rolls pods.
				mount.ReadOnly = false
			}
		}
	}
	for i := range sts.Spec.Template.Spec.InitContainers {
		renameConfigMounts(sts.Spec.Template.Spec.InitContainers[i].VolumeMounts)
	}
	for i := range sts.Spec.Template.Spec.Containers {
		renameConfigMounts(sts.Spec.Template.Spec.Containers[i].VolumeMounts)
	}
}

func renameConfigMounts(mounts []corev1.VolumeMount) {
	for i := range mounts {
		if mounts[i].Name == reconciler.ConfigVolumeName {
			mounts[i].Name = constants.ConfigVolumeName
		}
	}
}

// preserveLegacyVolumeMountOrder restores the order emitted by the Gen2 Doris
// builders. VolumeMount order is semantically irrelevant to kubelet, but it is
// part of the StatefulSet PodTemplate hash, so changing it rolls every pod.
//
// User podOverrides are strategic-merged after this normalization, reproducing
// the Gen 2 insertion and replacement order for explicit mounts.
func preserveLegacyVolumeMountOrder(container *corev1.Container, roleName string) {
	legacyMountPaths := []string{
		constants.PodinfoMountPath,
		constants.DefaultConfigMapPath,
		dorisLogDirectory,
	}
	switch roleName {
	case string(constants.ComponentTypeFE):
		legacyMountPaths = append(legacyMountPaths, constants.FEMetadataPath)
	case string(constants.ComponentTypeBE):
		legacyMountPaths = append(legacyMountPaths, constants.BEStoragePath)
	}

	mounts := container.VolumeMounts
	legacyMounts := make([]corev1.VolumeMount, 0, len(legacyMountPaths))
	legacySlots := make([]bool, len(mounts))
	for _, mountPath := range legacyMountPaths {
		for i := range mounts {
			if legacySlots[i] || mounts[i].MountPath != mountPath {
				continue
			}
			legacyMounts = append(legacyMounts, mounts[i])
			legacySlots[i] = true
			break
		}
	}

	ordered := make([]corev1.VolumeMount, len(mounts))
	legacyIndex := 0
	for i := range mounts {
		if !legacySlots[i] {
			ordered[i] = mounts[i]
			continue
		}
		ordered[i] = legacyMounts[legacyIndex]
		legacyIndex++
	}
	container.VolumeMounts = ordered
}

func legacyRoleGroupSelector(clusterName, roleName, roleGroupName string) map[string]string {
	selector := legacyRoleSelector(clusterName, roleName)
	selector["app.kubernetes.io/role-group"] = roleGroupName
	return selector
}

func legacyRoleSelector(clusterName, roleName string) map[string]string {
	return map[string]string{
		opconstant.LabelKubernetesName:      legacyAppName,
		opconstant.LabelKubernetesInstance:  clusterName,
		opconstant.LabelKubernetesComponent: roleName,
		opconstant.LabelKubernetesManagedBy: legacyManagedBy,
	}
}

func preserveLegacyResourceLabels(resources *reconciler.RoleGroupResources, labels map[string]string) {
	replaceLabels(resources.ConfigMap, labels)
	replaceLabels(resources.Service, labels)
	replaceLabels(resources.HeadlessService, labels)
	replaceLabels(resources.StatefulSet, labels)
}

func replaceLabels(object metav1.Object, labels map[string]string) {
	if object == nil {
		return
	}
	object.SetLabels(maps.Clone(labels))
}

func metricsForRole(roleName string) (int32, string, bool) {
	switch roleName {
	case string(constants.ComponentTypeFE):
		return constants.FEHttpPort, constants.FEHttpPortName, true
	case string(constants.ComponentTypeBE):
		return constants.BEHttpPort, constants.BEHttpPortName, true
	default:
		return 0, "", false
	}
}

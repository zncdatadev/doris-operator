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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opconfig "github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opvector "github.com/zncdatadev/operator-go/pkg/vector"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	handlerTestDefaultGroup = "default"
	handlerTestNamespace    = handlerTestDefaultGroup
	handlerTestRoleValue    = "role"
	handlerTestGroupValue   = "group"
)

func TestDorisRoleGroupHandlerDeclareRoles(t *testing.T) {
	cluster := testDorisCluster()
	cluster.Spec.Image = &dorisv1alpha1.ImageSpec{ProductVersion: "3.0.5"}
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())

	roles, err := handler.DeclareRoles(context.Background(), nil, cluster)
	if err != nil {
		t.Fatalf("DeclareRoles() error = %v", err)
	}
	if len(roles) != 3 {
		t.Fatalf("len(DeclareRoles()) = %d, want 3", len(roles))
	}

	tests := []struct {
		role              string
		image             string
		containerName     string
		command           string
		ports             map[string]int32
		livenessTCPPort   int32
		readinessTCPPort  int32
		readinessHTTPPort int32
		dataVolumeName    string
		dataMountPath     string
		storage           string
		optional          bool
	}{
		{
			role:          string(constants.ComponentTypeFE),
			image:         "apache/doris:fe-3.0.5",
			containerName: constants.FEContainerName,
			command:       constants.FEEntrypoint,
			ports: map[string]int32{
				constants.FEHttpPortName:    constants.FEHttpPort,
				constants.FERpcPortName:     constants.FERpcPort,
				constants.FEQueryPortName:   constants.FEQueryPort,
				constants.FEEditLogPortName: constants.FEEditLogPort,
			},
			livenessTCPPort:   constants.FEQueryPort,
			readinessHTTPPort: constants.FEHttpPort,
			dataVolumeName:    constants.FEMetadataVolume,
			dataMountPath:     constants.FEMetadataPath,
			storage:           constants.FEStorageSize,
		},
		{
			role:          string(constants.ComponentTypeBE),
			image:         "apache/doris:be-3.0.5",
			containerName: constants.BEContainerName,
			command:       constants.BEEntrypoint,
			ports: map[string]int32{
				constants.BERpcPortName:       constants.BERpcPort,
				constants.BEHttpPortName:      constants.BEHttpPort,
				constants.BEHeartbeatPortName: constants.BEHeartbeatPort,
				constants.BEBrpcPortName:      constants.BEBrpcPort,
			},
			livenessTCPPort:   constants.BEHeartbeatPort,
			readinessHTTPPort: constants.BEHttpPort,
			dataVolumeName:    constants.BEStorageVolume,
			dataMountPath:     constants.BEStoragePath,
			storage:           constants.BEStorageSize,
		},
		{
			role:          string(constants.ComponentTypeBroker),
			image:         "apache/doris:broker-3.0.5",
			containerName: constants.BrokerContainerName,
			command:       constants.BrokerEntrypoint,
			ports: map[string]int32{
				constants.BrokerIpcPortName: constants.BrokerIpcPort,
			},
			livenessTCPPort:  constants.BrokerIpcPort,
			readinessTCPPort: constants.BrokerIpcPort,
			optional:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			declaration, found := roles[tt.role]
			if !found {
				t.Fatalf("DeclareRoles() has no %q role", tt.role)
			}
			if declaration.Image == nil || declaration.Image.Custom != tt.image {
				t.Errorf("Image = %#v, want custom %q", declaration.Image, tt.image)
			}
			if declaration.MainContainerName != tt.containerName {
				t.Errorf("MainContainerName = %q, want %q", declaration.MainContainerName, tt.containerName)
			}
			if !reflect.DeepEqual(declaration.Command, []string{tt.command}) {
				t.Errorf("Command = %#v, want [%q]", declaration.Command, tt.command)
			}
			assertContainerPorts(t, declaration.ContainerPorts, tt.ports)
			assertServicePorts(t, declaration.ServicePorts, tt.ports)
			assertTCPProbe(t, "liveness", declaration.LivenessProbe, tt.livenessTCPPort)
			if tt.readinessHTTPPort != 0 {
				assertHTTPProbe(t, "readiness", declaration.ReadinessProbe, tt.readinessHTTPPort)
			} else {
				assertTCPProbe(t, "readiness", declaration.ReadinessProbe, tt.readinessTCPPort)
			}
			if !declaration.PublishNotReadyAddresses {
				t.Error("PublishNotReadyAddresses = false, want true")
			}
			if declaration.Optional != tt.optional {
				t.Errorf("Optional = %t, want %t", declaration.Optional, tt.optional)
			}

			if tt.dataVolumeName == "" {
				if declaration.DataVolume != nil {
					t.Errorf("DataVolume = %#v, want nil", declaration.DataVolume)
				}
				if declaration.ConfigDefaults.Resources.Storage != nil {
					t.Errorf("default storage = %#v, want nil", declaration.ConfigDefaults.Resources.Storage)
				}
				return
			}
			if declaration.DataVolume == nil {
				t.Fatal("DataVolume = nil")
			}
			if declaration.DataVolume.Name != tt.dataVolumeName || declaration.DataVolume.MountPath != tt.dataMountPath {
				t.Errorf("DataVolume = %#v, want name %q mounted at %q", declaration.DataVolume, tt.dataVolumeName, tt.dataMountPath)
			}
			gotCapacity := declaration.ConfigDefaults.Resources.Storage.GetCapacity()
			wantCapacity := resource.MustParse(tt.storage)
			if gotCapacity.Cmp(wantCapacity) != 0 {
				t.Errorf("default storage capacity = %s, want %s", gotCapacity.String(), wantCapacity.String())
			}
		})
	}
}

func TestResolveDorisRoleGroup(t *testing.T) {
	cluster := testDorisCluster()
	tests := []struct {
		role       string
		filename   string
		key        string
		want       string
		wantBEInit bool
	}{
		{
			role:     string(constants.ComponentTypeFE),
			filename: constants.FEConfigFilename,
			key:      configQueryPort,
			want:     "9030",
		},
		{
			role:       string(constants.ComponentTypeBE),
			filename:   constants.BEConfigFilename,
			key:        configBEHeartPort,
			want:       "9050",
			wantBEInit: true,
		},
		{
			role:     string(constants.ComponentTypeBroker),
			filename: constants.BrokerConfigFilename,
			key:      configBrokerPort,
			want:     "8000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			contribution, err := ResolveDorisRoleGroup(
				context.Background(),
				nil,
				cluster,
				&reconciler.RoleGroupBuildContext{RoleName: tt.role},
			)
			if err != nil {
				t.Fatalf("ResolveDorisRoleGroup() error = %v", err)
			}
			if got := contribution.ConfigOverrides[tt.filename][tt.key]; got != tt.want {
				t.Errorf("ConfigOverrides[%q][%q] = %q, want %q", tt.filename, tt.key, got, tt.want)
			}

			if !tt.wantBEInit {
				if contribution.PodOverrides != nil {
					t.Errorf("PodOverrides = %#v, want nil", contribution.PodOverrides)
				}
				return
			}
			assertPrivilegedBEInitContainer(t, contribution.PodOverrides.Spec.InitContainers)
		})
	}
}

func TestDorisBuildResourcesPreservesLegacyConfigEvaluationOrder(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	tests := []struct {
		role     string
		filename string
		wantKeys []string
	}{
		{
			role:     string(constants.ComponentTypeFE),
			filename: constants.FEConfigFilename,
			wantKeys: []string{
				configCurrentDate, configLogDir, configHTTPPort, configRPCPort, configQueryPort,
				configEditLogPort, configArrowPort, configSysLogLevel, configSysLogMode,
				configJavaOpts, configJavaOpts9, configJavaOpts17, configFQDNMode,
			},
		},
		{
			role:     string(constants.ComponentTypeBE),
			filename: constants.BEConfigFilename,
			wantKeys: []string{
				configCurrentDate, configLogDir, configJavaOpts, configJavaOpts9, configJavaOpts17,
				configJemalloc, configJemallocPre, configBEPort, configBEWebPort,
				configBEHeartPort, configBEBrpcPort, configArrowPort, configHTTPSEnable,
				configSSLCert, configSSLKey, configSysLogLevel, configAWSLogLevel,
				configAWSMetadata,
			},
		},
		{
			role:     string(constants.ComponentTypeBroker),
			filename: constants.BrokerConfigFilename,
			wantKeys: []string{configSysLogLevel, configBrokerPort, configClientTTL, configFQDNMode},
		},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			roleSpec := commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{handlerTestDefaultGroup: {}},
			}
			resources, _ := buildDorisRoleGroup(t, handler, cluster, tt.role, handlerTestDefaultGroup, roleSpec)
			content := resources.ConfigMap.Data[tt.filename]
			if got := configPropertyKeys(content); !reflect.DeepEqual(got, tt.wantKeys) {
				t.Errorf("%s property order = %#v, want %#v\n%s", tt.filename, got, tt.wantKeys, content)
			}
			if tt.filename == constants.BrokerConfigFilename && !strings.HasPrefix(content, "sys_log_level = INFO\n") {
				t.Errorf("%s did not preserve legacy spaced assignment:\n%s", tt.filename, content)
			}
		})
	}
}

func TestPreserveLegacyVectorConfig(t *testing.T) {
	const (
		aggregatorConfigMap = "vector-aggregator"
		aggregatorAddress   = "vector-aggregator.default.svc:6000"
	)
	scheme := k8sruntime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core API to scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: aggregatorConfigMap, Namespace: handlerTestNamespace},
		Data:       map[string]string{opvector.AddressKey: aggregatorAddress},
	}).Build()
	cluster := testDorisCluster()
	cluster.Spec.ClusterConfig = &dorisv1alpha1.ClusterConfigSpec{
		VectorAggregatorConfigMapName: func() *string {
			name := aggregatorConfigMap
			return &name
		}(),
	}
	enabled := true
	buildCtx := &reconciler.RoleGroupBuildContext{
		ClusterName:      cluster.Name,
		ClusterNamespace: cluster.Namespace,
		RoleName:         string(constants.ComponentTypeFE),
		RoleGroupName:    handlerTestDefaultGroup,
		RoleGroupSpec: commonsv1alpha1.RoleGroupSpec{Config: &commonsv1alpha1.RoleGroupConfigSpec{
			Logging: &commonsv1alpha1.LoggingSpec{EnableVectorAgent: &enabled},
		}},
	}
	configMap := &corev1.ConfigMap{Data: map[string]string{"fe.conf": "preserved"}}
	if err := preserveLegacyVectorConfig(context.Background(), k8sClient, cluster, buildCtx, configMap); err != nil {
		t.Fatalf("preserveLegacyVectorConfig() error = %v", err)
	}
	content := configMap.Data[opvector.VectorConfigFileName]
	const legacyVectorSHA256 = "93c3c868ab8c63e956868d4a78c99b056c1492dd1ccb8164de6f68f38235f811"
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(content))); got != legacyVectorSHA256 {
		t.Errorf("vector config SHA-256 = %s, want exact operator-go v0.12.6 output %s", got, legacyVectorSHA256)
	}
	if !strings.Contains(content, aggregatorAddress) ||
		!strings.Contains(content, cluster.Name) ||
		!strings.Contains(content, handlerTestDefaultGroup) {
		t.Errorf("vector config is missing legacy discovery metadata:\n%s", content)
	}
	if configMap.Data["fe.conf"] != "preserved" {
		t.Errorf("existing config data = %#v, want preserved", configMap.Data)
	}
	if strings.Contains(content, "internal_metrics") || strings.Contains(content, "prometheus_exporter") {
		t.Errorf("legacy vector config unexpectedly enables the v0.13 metrics pipeline:\n%s", content)
	}
}

func TestResolveDorisRoleGroupLDAPDoesNotPersistPassword(t *testing.T) {
	const (
		authenticationClassName = "ldap"
		credentialsSecretName   = "ldap-bind"
		bindUser                = "cn=admin,dc=example,dc=com"
		bindPassword            = "do-not-write-this-password"
	)

	scheme := k8sruntime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core API to scheme: %v", err)
	}
	if err := authv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add authentication API to scheme: %v", err)
	}

	authenticationClass := &authv1alpha1.AuthenticationClass{
		ObjectMeta: metav1.ObjectMeta{Name: authenticationClassName},
		Spec: authv1alpha1.AuthenticationClassSpec{AuthenticationProvider: &authv1alpha1.AuthenticationProvider{
			LDAP: &authv1alpha1.LDAPProvider{
				BindCredentials: &commonsv1alpha1.Credentials{SecretClass: credentialsSecretName},
				Hostname:        "ldap.example.com",
				Port:            389,
				SearchBase:      "ou=users,dc=example,dc=com",
				SearchFilter:    "(uid={login})",
			},
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName, Namespace: handlerTestNamespace},
		Data: map[string][]byte{
			"user":     []byte(bindUser),
			"password": []byte(bindPassword),
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(authenticationClass, secret).Build()
	cluster := testDorisCluster()
	cluster.Spec.ClusterConfig = &dorisv1alpha1.ClusterConfigSpec{Authentication: []dorisv1alpha1.AuthenticationSpec{{
		AuthenticationClass: authenticationClassName,
	}}}

	contribution, err := ResolveDorisRoleGroup(
		context.Background(),
		k8sClient,
		cluster,
		&reconciler.RoleGroupBuildContext{RoleName: string(constants.ComponentTypeFE)},
	)
	if err != nil {
		t.Fatalf("ResolveDorisRoleGroup() error = %v", err)
	}
	if got := contribution.ConfigOverrides[constants.FEConfigFilename]["authentication_type"]; got != "ldap" {
		t.Errorf("authentication_type = %q, want ldap", got)
	}
	ldapConfig := contribution.ConfigOverrides[constants.LDAPConfigFilename]
	if got := ldapConfig["ldap_admin_name"]; got != bindUser {
		t.Errorf("ldap_admin_name = %q, want %q", got, bindUser)
	}
	if _, found := ldapConfig["ldap_admin_password"]; found {
		t.Error("ldap_admin_password must not be persisted in a ConfigMap contribution")
	}
	serialized, err := json.Marshal(contribution.ConfigOverrides)
	if err != nil {
		t.Fatalf("marshal config overrides: %v", err)
	}
	if strings.Contains(string(serialized), bindPassword) {
		t.Error("LDAP bind password leaked into ConfigMap contribution")
	}
}

func TestDorisBuildResourcesPreservesStatefulSetAndServiceIdentity(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
		"alpha": {},
		"beta":  {},
	}}

	alphaResources, alphaContext := buildDorisRoleGroup(t, handler, cluster, string(constants.ComponentTypeFE), "alpha", roleSpec)
	statefulSet := alphaResources.StatefulSet
	expectedSelector := legacyRoleGroupSelector(cluster.Name, string(constants.ComponentTypeFE), "alpha")
	if statefulSet.Spec.ServiceName != alphaContext.ResourceName {
		t.Errorf("StatefulSet serviceName = %q, want %q", statefulSet.Spec.ServiceName, alphaContext.ResourceName)
	}
	if statefulSet.Spec.PodManagementPolicy != appsv1.ParallelPodManagement {
		t.Errorf("PodManagementPolicy = %q, want %q", statefulSet.Spec.PodManagementPolicy, appsv1.ParallelPodManagement)
	}
	if !reflect.DeepEqual(statefulSet.Spec.Selector.MatchLabels, expectedSelector) {
		t.Errorf("StatefulSet selector = %#v, want %#v", statefulSet.Spec.Selector.MatchLabels, expectedSelector)
	}
	if !reflect.DeepEqual(statefulSet.Spec.Template.Labels, expectedSelector) {
		t.Errorf("pod labels = %#v, want exact legacy labels %#v", statefulSet.Spec.Template.Labels, expectedSelector)
	}
	assertLegacyConfigVolume(t, statefulSet, alphaContext.ResourceName)

	if alphaResources.HeadlessService != nil {
		t.Errorf("HeadlessService = %#v, want nil because the legacy governing Service occupies the fixed Service slot", alphaResources.HeadlessService)
	}
	if alphaResources.Service == nil {
		t.Fatal("Service = nil")
	}
	if alphaResources.Service.Name != alphaContext.ResourceName {
		t.Errorf("governing Service name = %q, want %q", alphaResources.Service.Name, alphaContext.ResourceName)
	}
	if alphaResources.Service.Spec.ClusterIP != corev1.ClusterIPNone ||
		!reflect.DeepEqual(alphaResources.Service.Spec.ClusterIPs, []string{corev1.ClusterIPNone}) {
		t.Errorf("governing Service cluster IPs = (%q, %#v), want headless None", alphaResources.Service.Spec.ClusterIP, alphaResources.Service.Spec.ClusterIPs)
	}
	if !reflect.DeepEqual(alphaResources.Service.Spec.Selector, expectedSelector) {
		t.Errorf("governing Service selector = %#v, want %#v", alphaResources.Service.Spec.Selector, expectedSelector)
	}

	if len(alphaResources.ExtraResources) != 0 {
		t.Fatalf("ExtraResources = %#v, want role-level Services to be owned by the cluster extension", alphaResources.ExtraResources)
	}
	roleSelector := legacyRoleSelector(cluster.Name, string(constants.ComponentTypeFE))
	for name, object := range map[string]metav1.Object{
		"ConfigMap":   alphaResources.ConfigMap,
		"Service":     alphaResources.Service,
		"StatefulSet": alphaResources.StatefulSet,
	} {
		if !reflect.DeepEqual(object.GetLabels(), expectedSelector) {
			t.Errorf("%s metadata labels = %#v, want exact legacy labels %#v", name, object.GetLabels(), expectedSelector)
		}
	}
	metricsLabels := maps.Clone(expectedSelector)
	metricsLabels["prometheus.io/scrape"] = configValueTrue
	if !reflect.DeepEqual(alphaResources.MetricsService.Labels, metricsLabels) {
		t.Errorf("Metrics metadata labels = %#v, want exact legacy labels %#v", alphaResources.MetricsService.Labels, metricsLabels)
	}
	if got := alphaResources.MetricsService.Annotations["prometheus.io/path"]; got != "/metrics" {
		t.Errorf("Metrics prometheus path = %q, want legacy /metrics annotation", got)
	}

	betaResources, _ := buildDorisRoleGroup(t, handler, cluster, string(constants.ComponentTypeFE), "beta", roleSpec)
	if len(betaResources.ExtraResources) != 0 {
		t.Errorf("role group emitted cluster-level compatibility Services: %#v", betaResources.ExtraResources)
	}
	if !selectorMatches(roleSelector, alphaResources.StatefulSet.Spec.Template.Labels) ||
		!selectorMatches(roleSelector, betaResources.StatefulSet.Spec.Template.Labels) {
		t.Errorf("role-level selector %#v does not select both alpha and beta pods", roleSelector)
	}
}

func TestDorisBuildRolePodDisruptionBudgetUsesExactLegacyLabels(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	maxUnavailable := int32(1)
	roleSpec := &commonsv1alpha1.RoleSpec{RoleConfig: &commonsv1alpha1.RoleConfigSpec{
		PodDisruptionBudget: &commonsv1alpha1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
		},
	}}
	buildContext := &reconciler.RoleBuildContext{
		ClusterName:      "example",
		ClusterNamespace: handlerTestNamespace,
		ClusterLabels:    map[string]string{"test-label": "test-value"},
		RoleName:         string(constants.ComponentTypeFE),
		RoleSpec:         roleSpec,
		ProductName:      "doris",
		ProductVersion:   "3.0.5",
	}

	pdb := handler.BuildRolePodDisruptionBudget(buildContext)
	if pdb == nil {
		t.Fatal("BuildRolePodDisruptionBudget() = nil")
	}
	wantLabels := legacyRoleSelector(buildContext.ClusterName, buildContext.RoleName)
	if !reflect.DeepEqual(pdb.Labels, wantLabels) {
		t.Errorf("PDB labels = %#v, want exact legacy labels %#v", pdb.Labels, wantLabels)
	}
	if !reflect.DeepEqual(pdb.Spec.Selector.MatchLabels, wantLabels) {
		t.Errorf("PDB selector = %#v, want exact legacy labels %#v", pdb.Spec.Selector.MatchLabels, wantLabels)
	}
}

func TestDorisBuildResourcesPreservesDataVolumesAndBEInit(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	tests := []struct {
		role       string
		claimName  string
		mountPath  string
		capacity   string
		wantBEInit bool
	}{
		{
			role:      string(constants.ComponentTypeFE),
			claimName: constants.FEMetadataVolume,
			mountPath: constants.FEMetadataPath,
			capacity:  constants.FEStorageSize,
		},
		{
			role:       string(constants.ComponentTypeBE),
			claimName:  constants.BEStorageVolume,
			mountPath:  constants.BEStoragePath,
			capacity:   constants.BEStorageSize,
			wantBEInit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			roleSpec := commonsv1alpha1.RoleSpec{RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{handlerTestDefaultGroup: {}}}
			resources, _ := buildDorisRoleGroup(t, handler, cluster, tt.role, handlerTestDefaultGroup, roleSpec)
			statefulSet := resources.StatefulSet
			if len(statefulSet.Spec.VolumeClaimTemplates) != 1 {
				t.Fatalf("len(VolumeClaimTemplates) = %d, want 1", len(statefulSet.Spec.VolumeClaimTemplates))
			}
			claim := statefulSet.Spec.VolumeClaimTemplates[0]
			if claim.Name != tt.claimName {
				t.Errorf("PVC name = %q, want %q", claim.Name, tt.claimName)
			}
			gotCapacity := claim.Spec.Resources.Requests[corev1.ResourceStorage]
			wantCapacity := resource.MustParse(tt.capacity)
			if gotCapacity.Cmp(wantCapacity) != 0 {
				t.Errorf("PVC capacity = %s, want %s", gotCapacity.String(), wantCapacity.String())
			}
			main := statefulSet.Spec.Template.Spec.Containers[0]
			mount := findVolumeMount(main.VolumeMounts, tt.claimName)
			if mount == nil || mount.MountPath != tt.mountPath {
				t.Errorf("data volume mount = %#v, want name %q at %q", mount, tt.claimName, tt.mountPath)
			}
			if tt.wantBEInit {
				assertPrivilegedBEInitContainer(t, statefulSet.Spec.Template.Spec.InitContainers)
			} else if len(statefulSet.Spec.Template.Spec.InitContainers) != 0 {
				t.Errorf("FE init containers = %#v, want none", statefulSet.Spec.Template.Spec.InitContainers)
			}
		})
	}
}

func TestDorisBuildResourcesSkipsBrokerMetricsService(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{handlerTestDefaultGroup: {}}}

	resources, _ := buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeBroker),
		handlerTestDefaultGroup,
		roleSpec,
	)
	if resources.MetricsService != nil {
		t.Errorf("MetricsService = %#v, want nil because the Broker IPC port is not an HTTP metrics endpoint", resources.MetricsService)
	}
}

func TestDorisBuildResourcesPreservesLegacyUserOverrideSemantics(t *testing.T) {
	const (
		customFilename = "custom-runtime.conf"
		customContent  = "first=line\nsecond=line"
	)
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{
		ConfigOverrides: map[string]map[string]string{
			constants.FEConfigFilename: {
				configQueryPort: "19030",
				"role_only":     handlerTestRoleValue,
			},
			customFilename: {customFilename: customContent},
		},
		EnvOverrides: map[string]string{
			constants.UserEnvVar: "role-user",
			"ROLE_ONLY":          "role",
		},
		CliOverrides: []string{"--from-role"},
		RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
			handlerTestDefaultGroup: {
				ConfigOverrides: map[string]map[string]string{
					constants.FEConfigFilename: {
						configQueryPort: "29030",
						"group_only":    handlerTestGroupValue,
					},
				},
				EnvOverrides: map[string]string{
					constants.UserEnvVar: "group-user",
					"GROUP_ONLY":         "group",
				},
				CliOverrides: []string{"--from-group"},
			},
		},
	}

	resources, _ := buildDorisRoleGroup(t, handler, cluster, string(constants.ComponentTypeFE), handlerTestDefaultGroup, roleSpec)
	main := resources.StatefulSet.Spec.Template.Spec.Containers[0]
	wantCommand := []string{"--from-role", "--from-group"}
	if !reflect.DeepEqual(main.Command, wantCommand) {
		t.Errorf("main command = %#v, want appended role and role-group cliOverrides %#v", main.Command, wantCommand)
	}
	if len(main.Args) != 0 {
		t.Errorf("main args = %#v, want empty after legacy cliOverrides command replacement", main.Args)
	}
	if got, found := lastEnvValue(main.Env, constants.UserEnvVar); !found || got != "group-user" {
		t.Errorf("effective %s = (%q, %t), want group-user", constants.UserEnvVar, got, found)
	}
	if got, found := lastEnvValue(main.Env, "ROLE_ONLY"); !found || got != "role" {
		t.Errorf("effective ROLE_ONLY = (%q, %t), want role", got, found)
	}
	if got, found := lastEnvValue(main.Env, "GROUP_ONLY"); !found || got != "group" {
		t.Errorf("effective GROUP_ONLY = (%q, %t), want group", got, found)
	}

	configFile := resources.ConfigMap.Data[constants.FEConfigFilename]
	if !hasProperty(configFile, configQueryPort, "9030") {
		t.Errorf("%s does not retain the product query port:\n%s", constants.FEConfigFilename, configFile)
	}
	for _, dormant := range []string{
		"query_port=19030", "query_port=29030",
		"role_only=" + handlerTestRoleValue,
		"group_only=" + handlerTestGroupValue,
	} {
		if hasPropertyLine(configFile, dormant) {
			t.Errorf("%s activated legacy dormant per-key override %q:\n%s", constants.FEConfigFilename, dormant, configFile)
		}
	}
	if got := resources.ConfigMap.Data[customFilename]; got != customContent {
		t.Errorf("legacy whole-file override %q = %q, want %q", customFilename, got, customContent)
	}
}

func TestDorisBuildResourcesKeepsPodOverridesAboveLegacyCLIOverride(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	tests := []struct {
		name        string
		cli         []string
		podOverride string
		wantCommand []string
		wantArgs    []string
	}{
		{
			name:        "pod command and args replace cli override",
			cli:         []string{"/from-cli"},
			podOverride: `{"spec":{"containers":[{"name":"fe","command":["/from-pod"],"args":["--pod-arg"]}]}}`,
			wantCommand: []string{"/from-pod"},
			wantArgs:    []string{"--pod-arg"},
		},
		{
			name:        "explicit empty pod args retain legacy default",
			podOverride: `{"spec":{"containers":[{"name":"fe","args":[]}]}}`,
			wantCommand: []string{constants.FEEntrypoint},
			wantArgs:    []string{"$(" + constants.FEAddrEnvVar + ")"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roleSpec := commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
					handlerTestDefaultGroup: {
						CliOverrides: tt.cli,
						PodOverrides: &k8sruntime.RawExtension{Raw: []byte(tt.podOverride)},
					},
				},
			}
			resources, _ := buildDorisRoleGroup(
				t,
				handler,
				cluster,
				string(constants.ComponentTypeFE),
				handlerTestDefaultGroup,
				roleSpec,
			)
			main := resources.StatefulSet.Spec.Template.Spec.Containers[0]
			if !reflect.DeepEqual(main.Command, tt.wantCommand) {
				t.Errorf("main command = %#v, want %#v", main.Command, tt.wantCommand)
			}
			if !reflect.DeepEqual(main.Args, tt.wantArgs) {
				t.Errorf("main args = %#v, want %#v", main.Args, tt.wantArgs)
			}
		})
	}
}

func TestDorisBuildResourcesAppendsLegacyPodOverrideListsAcrossLayers(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{
		PodOverrides: &k8sruntime.RawExtension{Raw: []byte(
			`{"spec":{"tolerations":[{"key":"role-taint","operator":"Exists"}]}}`,
		)},
		RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
			handlerTestDefaultGroup: {
				PodOverrides: &k8sruntime.RawExtension{Raw: []byte(
					`{"spec":{"tolerations":[{"key":"group-taint","operator":"Exists"}]}}`,
				)},
			},
		},
	}

	resources, _ := buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeFE),
		handlerTestDefaultGroup,
		roleSpec,
	)
	tolerations := resources.StatefulSet.Spec.Template.Spec.Tolerations
	wantKeys := []string{"role-taint", "group-taint"}
	gotKeys := make([]string, 0, len(tolerations))
	for _, toleration := range tolerations {
		gotKeys = append(gotKeys, toleration.Key)
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Errorf("toleration keys = %#v, want Gen 2 append order %#v", gotKeys, wantKeys)
	}
}

func TestDorisBuildResourcesDeepMergesLegacyAffinityAcrossLayers(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{
		Config: &commonsv1alpha1.RoleGroupConfigSpec{Affinity: &k8sruntime.RawExtension{Raw: []byte(
			`{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"topology.kubernetes.io/zone","operator":"In","values":["zone-a"]}]}]}}}`,
		)}},
		RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
			handlerTestDefaultGroup: {
				Config: &commonsv1alpha1.RoleGroupConfigSpec{Affinity: &k8sruntime.RawExtension{Raw: []byte(
					`{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"node-type","operator":"In","values":["storage"]}]}]}},"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app":"doris"}},"topologyKey":"kubernetes.io/hostname"}]}}`,
				)}},
			},
		},
	}

	resources, _ := buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeFE),
		handlerTestDefaultGroup,
		roleSpec,
	)
	affinity := resources.StatefulSet.Spec.Template.Spec.Affinity
	if affinity == nil || affinity.NodeAffinity == nil ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("merged node affinity = %#v, want required node affinity", affinity)
	}
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 2 {
		t.Errorf("node selector term count = %d, want appended role and role-group terms", len(terms))
	}
	if affinity.PodAntiAffinity == nil ||
		len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Errorf("merged pod anti-affinity = %#v, want role-group constraint", affinity.PodAntiAffinity)
	}
}

func TestDorisBuildResourcesPreservesLegacyPodDefaultsAndExplicitOverrides(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	roleSpec := commonsv1alpha1.RoleSpec{
		RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
			handlerTestDefaultGroup: {},
		},
	}

	resources, _ := buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeFE),
		handlerTestDefaultGroup,
		roleSpec,
	)
	podSpec := resources.StatefulSet.Spec.Template.Spec
	if podSpec.ServiceAccountName != "" {
		t.Errorf("serviceAccountName = %q, want namespace default service account", podSpec.ServiceAccountName)
	}
	if podSpec.EnableServiceLinks == nil || !*podSpec.EnableServiceLinks {
		t.Errorf("enableServiceLinks = %v, want legacy default true", podSpec.EnableServiceLinks)
	}

	disableServiceLinks := false
	group := roleSpec.RoleGroups[handlerTestDefaultGroup]
	group.PodOverrides = &k8sruntime.RawExtension{Raw: []byte(`{"metadata":{"labels":{"example.com/explicit":"true"}},"spec":{"serviceAccountName":"custom-doris","enableServiceLinks":false,"containers":[{"name":"fe","volumeMounts":[{"name":"doris-config","mountPath":"/etc/doris/conf","readOnly":true}]}]}}`)}
	roleSpec.RoleGroups[handlerTestDefaultGroup] = group
	resources, _ = buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeFE),
		handlerTestDefaultGroup,
		roleSpec,
	)
	podSpec = resources.StatefulSet.Spec.Template.Spec
	if podSpec.ServiceAccountName != "custom-doris" {
		t.Errorf("serviceAccountName = %q, want explicit override custom-doris", podSpec.ServiceAccountName)
	}
	if podSpec.EnableServiceLinks == nil || *podSpec.EnableServiceLinks != disableServiceLinks {
		t.Errorf("enableServiceLinks = %v, want explicit override false", podSpec.EnableServiceLinks)
	}
	if got := resources.StatefulSet.Spec.Template.Labels["example.com/explicit"]; got != configValueTrue {
		t.Errorf("explicit pod label = %q, want true", got)
	}
	wantPodLabels := legacyRoleGroupSelector(cluster.Name, string(constants.ComponentTypeFE), handlerTestDefaultGroup)
	wantPodLabels["example.com/explicit"] = configValueTrue
	if !reflect.DeepEqual(resources.StatefulSet.Spec.Template.Labels, wantPodLabels) {
		t.Errorf("pod labels = %#v, want legacy plus explicit overrides %#v", resources.StatefulSet.Spec.Template.Labels, wantPodLabels)
	}
	configMount := findVolumeMount(podSpec.Containers[0].VolumeMounts, constants.ConfigVolumeName)
	if configMount == nil || !configMount.ReadOnly {
		t.Errorf("explicit read-only config mount = %#v, want readOnly=true", configMount)
	}
}

func TestDorisBuildResourcesPreservesLegacyTerminationTruncation(t *testing.T) {
	tests := []struct {
		timeout     string
		wantSeconds int64
	}{
		{timeout: "0s", wantSeconds: 0},
		{timeout: "500ms", wantSeconds: 0},
		{timeout: "1500ms", wantSeconds: 1},
		{timeout: "-500ms", wantSeconds: 0},
	}
	for _, tt := range tests {
		t.Run(tt.timeout, func(t *testing.T) {
			cluster := testDorisCluster()
			cluster.Spec.Frontend = &dorisv1alpha1.RoleSpec{
				Config: &dorisv1alpha1.ConfigSpec{GracefulShutdownTimeout: &tt.timeout},
				RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
					handlerTestDefaultGroup: {},
				},
			}
			genericRole := cluster.Spec.ToGenericSpec().Roles[string(constants.ComponentTypeFE)]

			resources, _ := buildDorisRoleGroup(
				t,
				NewDorisRoleGroupHandler(k8sruntime.NewScheme()),
				cluster,
				string(constants.ComponentTypeFE),
				handlerTestDefaultGroup,
				genericRole,
			)
			gracePeriod := resources.StatefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds
			if gracePeriod == nil || *gracePeriod != tt.wantSeconds {
				t.Errorf("terminationGracePeriodSeconds = %v, want %d", gracePeriod, tt.wantSeconds)
			}
		})
	}
}

func TestDorisBuildResourcesAcceptsLegacyConfigVolumeOverrides(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	const secretName = "custom-doris-config"
	tests := []struct {
		name         string
		podOverride  string
		wantReadOnly bool
		wantSubPath  string
	}{
		{
			name:        "volume source only",
			podOverride: `{"spec":{"volumes":[{"name":"doris-config","secret":{"secretName":"custom-doris-config"}}]}}`,
		},
		{
			name:         "volume source and main mount",
			podOverride:  `{"spec":{"volumes":[{"name":"doris-config","secret":{"secretName":"custom-doris-config"}}],"containers":[{"name":"fe","volumeMounts":[{"name":"doris-config","mountPath":"/etc/doris/conf","readOnly":true,"subPath":"fe.conf"}]}]}}`,
			wantReadOnly: true,
			wantSubPath:  constants.FEConfigFilename,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roleSpec := commonsv1alpha1.RoleSpec{
				RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
					handlerTestDefaultGroup: {
						PodOverrides: &k8sruntime.RawExtension{Raw: []byte(tt.podOverride)},
					},
				},
			}
			resources, _ := buildDorisRoleGroup(
				t,
				handler,
				cluster,
				string(constants.ComponentTypeFE),
				handlerTestDefaultGroup,
				roleSpec,
			)

			configVolumes := 0
			for i := range resources.StatefulSet.Spec.Template.Spec.Volumes {
				volume := &resources.StatefulSet.Spec.Template.Spec.Volumes[i]
				if volume.Name == reconciler.ConfigVolumeName {
					t.Errorf("framework config volume %q leaked into legacy StatefulSet", reconciler.ConfigVolumeName)
				}
				if volume.Name != constants.ConfigVolumeName {
					continue
				}
				configVolumes++
				if volume.Secret == nil || volume.Secret.SecretName != secretName {
					t.Errorf("legacy config volume source = %#v, want Secret %q", volume.VolumeSource, secretName)
				}
			}
			if configVolumes != 1 {
				t.Errorf("legacy config volume count = %d, want 1", configVolumes)
			}
			main := resources.StatefulSet.Spec.Template.Spec.Containers[0]
			mount := findVolumeMount(main.VolumeMounts, constants.ConfigVolumeName)
			if mount == nil {
				t.Fatal("legacy config mount is absent")
			}
			if mount.ReadOnly != tt.wantReadOnly || mount.SubPath != tt.wantSubPath {
				t.Errorf("legacy config mount = %#v, want readOnly=%t subPath=%q", mount, tt.wantReadOnly, tt.wantSubPath)
			}
		})
	}
}

func TestDorisBuildResourcesAllowsLegacyCustomConfigMountReplacement(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	const (
		customVolume = "custom-config"
		secretName   = "external-doris-config"
	)
	roleSpec := commonsv1alpha1.RoleSpec{
		RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
			handlerTestDefaultGroup: {
				PodOverrides: &k8sruntime.RawExtension{Raw: []byte(
					`{"spec":{"volumes":[{"name":"custom-config","secret":{"secretName":"external-doris-config"}}],"containers":[{"name":"fe","volumeMounts":[{"name":"custom-config","mountPath":"/etc/doris/conf","readOnly":true}]}]}}`,
				)},
			},
		},
	}

	resources, _ := buildDorisRoleGroup(
		t,
		handler,
		cluster,
		string(constants.ComponentTypeFE),
		handlerTestDefaultGroup,
		roleSpec,
	)
	var custom *corev1.Volume
	for i := range resources.StatefulSet.Spec.Template.Spec.Volumes {
		volume := &resources.StatefulSet.Spec.Template.Spec.Volumes[i]
		if volume.Name == customVolume {
			custom = volume
		}
	}
	if custom == nil || custom.Secret == nil || custom.Secret.SecretName != secretName {
		t.Fatalf("custom config volume = %#v, want Secret %q", custom, secretName)
	}
	main := resources.StatefulSet.Spec.Template.Spec.Containers[0]
	mount := findVolumeMount(main.VolumeMounts, customVolume)
	if mount == nil || mount.MountPath != constants.DefaultConfigMapPath || !mount.ReadOnly {
		t.Errorf("custom config mount = %#v, want read-only replacement at %q", mount, constants.DefaultConfigMapPath)
	}
}

func TestDorisBuildResourcesPreservesLegacyVolumeAndMountOrder(t *testing.T) {
	handler := NewDorisRoleGroupHandler(k8sruntime.NewScheme())
	cluster := testDorisCluster()
	const (
		extraVolumeName = "explicit-extra"
		extraMountPath  = "/opt/doris/extra"
	)

	tests := []struct {
		role       string
		container  string
		dataVolume string
	}{
		{
			role:       string(constants.ComponentTypeFE),
			container:  constants.FEContainerName,
			dataVolume: constants.FEMetadataVolume,
		},
		{
			role:       string(constants.ComponentTypeBE),
			container:  constants.BEContainerName,
			dataVolume: constants.BEStorageVolume,
		},
		{
			role:      string(constants.ComponentTypeBroker),
			container: constants.BrokerContainerName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			override := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name: extraVolumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				}},
				Containers: []corev1.Container{{
					Name: tt.container,
					VolumeMounts: []corev1.VolumeMount{{
						Name:      extraVolumeName,
						MountPath: extraMountPath,
					}},
				}},
			}}
			rawOverride, err := json.Marshal(override)
			if err != nil {
				t.Fatalf("marshal pod override: %v", err)
			}
			roleGroup := commonsv1alpha1.RoleGroupSpec{}
			roleGroup.PodOverrides = &k8sruntime.RawExtension{Raw: rawOverride}
			roleSpec := commonsv1alpha1.RoleSpec{RoleGroups: map[string]commonsv1alpha1.RoleGroupSpec{
				handlerTestDefaultGroup: roleGroup,
			}}

			resources, _ := buildDorisRoleGroup(
				t,
				handler,
				cluster,
				tt.role,
				handlerTestDefaultGroup,
				roleSpec,
			)
			podSpec := resources.StatefulSet.Spec.Template.Spec

			// Strategic merge prepends a new explicit mount in both Gen2 and v0.13.
			// Compatibility sorting leaves that user-owned slot in place and only
			// restores the relative order of the legacy mounts.
			wantMounts := []string{
				extraVolumeName,
				constants.PodinfoVolumeName,
				constants.ConfigVolumeName,
				constants.LogVolumeName,
			}
			if tt.dataVolume != "" {
				wantMounts = append(wantMounts, tt.dataVolume)
			}
			if got := volumeMountNames(podSpec.Containers[0].VolumeMounts); !reflect.DeepEqual(got, wantMounts) {
				t.Errorf("volume mount order = %v, want legacy order plus explicit mount %v", got, wantMounts)
			}

			// Gen2 sorted its builder-owned volumes by name before applying podOverrides.
			// The v0.13 builder already emits the same base order, so assert it rather
			// than reordering to the legacy source slice's pre-builder order.
			wantVolumes := []string{
				extraVolumeName,
				constants.ConfigVolumeName,
				constants.LogVolumeName,
				constants.PodinfoVolumeName,
			}
			if got := volumeNames(podSpec.Volumes); !reflect.DeepEqual(got, wantVolumes) {
				t.Errorf("volume order = %v, want Gen2 builder order plus explicit volume %v", got, wantVolumes)
			}
		})
	}
}

func TestDorisBuildResourcesPreservesLegacyContainerResourceSemantics(t *testing.T) {
	quantity := func(value string) *resource.Quantity {
		result := resource.MustParse(value)
		return &result
	}
	tests := []struct {
		name           string
		roleResources  *commonsv1alpha1.ResourcesSpec
		groupResources *commonsv1alpha1.ResourcesSpec
		wantCPURequest *resource.Quantity
		wantCPULimit   *resource.Quantity
		wantMemory     bool
	}{
		{
			name:           "no user resources keeps product defaults",
			wantCPURequest: quantity(constants.DefaultCPURequest),
			wantCPULimit:   quantity(constants.DefaultCPULimit),
			wantMemory:     true,
		},
		{
			name:           "empty CPU clears CPU and keeps default memory",
			groupResources: &commonsv1alpha1.ResourcesSpec{CPU: &commonsv1alpha1.CPUResource{}},
			wantMemory:     true,
		},
		{
			name:           "empty resources clears CPU and memory",
			groupResources: &commonsv1alpha1.ResourcesSpec{},
		},
		{
			name: "single CPU leaf does not inherit its sibling",
			groupResources: &commonsv1alpha1.ResourcesSpec{CPU: &commonsv1alpha1.CPUResource{
				Max: quantity("600m"),
			}},
			wantCPURequest: quantity("0"),
			wantCPULimit:   quantity("600m"),
			wantMemory:     true,
		},
		{
			name: "empty group CPU replaces role CPU",
			roleResources: &commonsv1alpha1.ResourcesSpec{CPU: &commonsv1alpha1.CPUResource{
				Min: quantity("200m"), Max: quantity("600m"),
			}},
			groupResources: &commonsv1alpha1.ResourcesSpec{CPU: &commonsv1alpha1.CPUResource{}},
			wantMemory:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := testDorisCluster()
			role := &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
				handlerTestDefaultGroup: {},
			}}
			if tt.roleResources != nil {
				role.Config = &dorisv1alpha1.ConfigSpec{Resources: tt.roleResources}
			}
			if tt.groupResources != nil {
				group := role.RoleGroups[handlerTestDefaultGroup]
				group.Config = &dorisv1alpha1.ConfigSpec{Resources: tt.groupResources}
				role.RoleGroups[handlerTestDefaultGroup] = group
			}
			cluster.Spec.Frontend = role
			genericRole := cluster.Spec.ToGenericSpec().Roles[string(constants.ComponentTypeFE)]
			resources, _ := buildDorisRoleGroup(
				t,
				NewDorisRoleGroupHandler(k8sruntime.NewScheme()),
				cluster,
				string(constants.ComponentTypeFE),
				handlerTestDefaultGroup,
				genericRole,
			)
			containerResources := resources.StatefulSet.Spec.Template.Spec.Containers[0].Resources
			assertQuantityEntry(t, containerResources.Requests, corev1.ResourceCPU, tt.wantCPURequest)
			assertQuantityEntry(t, containerResources.Limits, corev1.ResourceCPU, tt.wantCPULimit)
			_, memoryRequest := containerResources.Requests[corev1.ResourceMemory]
			_, memoryLimit := containerResources.Limits[corev1.ResourceMemory]
			if memoryRequest != tt.wantMemory || memoryLimit != tt.wantMemory {
				t.Errorf("memory presence = request:%t limit:%t, want both %t", memoryRequest, memoryLimit, tt.wantMemory)
			}
		})
	}
}

func testDorisCluster() *dorisv1alpha1.DorisCluster {
	return &dorisv1alpha1.DorisCluster{ObjectMeta: metav1.ObjectMeta{
		Name:      "example",
		Namespace: handlerTestNamespace,
		Labels:    map[string]string{"test-label": "test-value"},
	}}
}

func buildDorisRoleGroup(
	t *testing.T,
	handler *DorisRoleGroupHandler,
	cluster *dorisv1alpha1.DorisCluster,
	roleName string,
	roleGroupName string,
	roleSpec commonsv1alpha1.RoleSpec,
) (*reconciler.RoleGroupResources, *reconciler.RoleGroupBuildContext) {
	t.Helper()
	declarations, err := handler.DeclareRoles(context.Background(), nil, cluster)
	if err != nil {
		t.Fatalf("DeclareRoles() error = %v", err)
	}
	declaration, found := declarations[roleName]
	if !found {
		t.Fatalf("DeclareRoles() has no %q role", roleName)
	}
	rawRoleGroup, found := roleSpec.RoleGroups[roleGroupName]
	if !found {
		t.Fatalf("role %q has no %q group", roleName, roleGroupName)
	}

	effectiveConfig, _, err := reconciler.FoldCommonConfig(
		declaration.ConfigDefaults,
		roleSpec.Config,
		rawRoleGroup.Config,
	)
	if err != nil {
		t.Fatalf("FoldCommonConfig() error = %v", err)
	}
	buildRoleGroup := rawRoleGroup
	buildRoleGroup.Config = effectiveConfig
	clusterSpec := &commonsv1alpha1.GenericClusterSpec{Roles: map[string]commonsv1alpha1.RoleSpec{roleName: roleSpec}}
	buildContext := &reconciler.RoleGroupBuildContext{
		ResolvedImage: reconciler.ResolvedImage{
			Reference:  declaration.Image.Custom,
			PullPolicy: corev1.PullIfNotPresent,
		},
		ClusterName:        cluster.Name,
		ClusterNamespace:   cluster.Namespace,
		ClusterLabels:      cluster.Labels,
		ClusterSpec:        clusterSpec,
		RoleName:           roleName,
		RoleSpec:           &roleSpec,
		RoleGroupName:      roleGroupName,
		RoleGroupSpec:      buildRoleGroup,
		ResourceName:       reconciler.RoleGroupResourceName(cluster.Name, roleName, roleGroupName),
		ServiceAccountName: "doriscluster-" + cluster.Name,
		Declaration:        declaration,
	}

	contribution, err := ResolveDorisRoleGroup(context.Background(), nil, cluster, buildContext)
	if err != nil {
		t.Fatalf("ResolveDorisRoleGroup() error = %v", err)
	}
	derivedOverrides := contributionAsOverrides(t, contribution)
	buildContext.MergedConfig = opconfig.NewConfigMerger().Merge(
		derivedOverrides,
		roleSpec.GetOverrides(),
		rawRoleGroup.GetOverrides(),
	)

	resources, err := handler.BuildResources(context.Background(), nil, cluster, buildContext)
	if err != nil {
		t.Fatalf("BuildResources(%s/%s) error = %v", roleName, roleGroupName, err)
	}
	return resources, buildContext
}

func contributionAsOverrides(t *testing.T, contribution *reconciler.Contribution) *commonsv1alpha1.OverridesSpec {
	t.Helper()
	if contribution == nil {
		return nil
	}
	overrides := &commonsv1alpha1.OverridesSpec{
		ConfigOverrides: contribution.ConfigOverrides,
		EnvOverrides:    contribution.EnvVars,
	}
	if contribution.PodOverrides != nil {
		raw, err := json.Marshal(contribution.PodOverrides)
		if err != nil {
			t.Fatalf("marshal contribution PodOverrides: %v", err)
		}
		overrides.PodOverrides = &k8sruntime.RawExtension{Raw: raw}
	}
	return overrides
}

func assertContainerPorts(t *testing.T, ports []corev1.ContainerPort, want map[string]int32) {
	t.Helper()
	got := make(map[string]int32, len(ports))
	for _, port := range ports {
		got[port.Name] = port.ContainerPort
		if port.Protocol != corev1.ProtocolTCP {
			t.Errorf("container port %q protocol = %q, want TCP", port.Name, port.Protocol)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("container ports = %#v, want %#v", got, want)
	}
}

func assertServicePorts(t *testing.T, ports []corev1.ServicePort, want map[string]int32) {
	t.Helper()
	if len(ports) != len(want) {
		t.Errorf("len(service ports) = %d, want %d", len(ports), len(want))
	}
	for _, port := range ports {
		wantNumber, found := want[port.Name]
		if !found {
			t.Errorf("unexpected service port %q", port.Name)
			continue
		}
		if port.Port != wantNumber || port.TargetPort != intstr.FromString(port.Name) {
			t.Errorf("service port %q = port %d target %#v, want port %d target name %q", port.Name, port.Port, port.TargetPort, wantNumber, port.Name)
		}
	}
}

func assertTCPProbe(t *testing.T, name string, probe *corev1.Probe, wantPort int32) {
	t.Helper()
	if probe == nil || probe.TCPSocket == nil {
		t.Fatalf("%s probe = %#v, want TCP probe", name, probe)
	}
	if probe.TCPSocket.Port != intstr.FromInt32(wantPort) {
		t.Errorf("%s TCP port = %#v, want %d", name, probe.TCPSocket.Port, wantPort)
	}
	if probe.InitialDelaySeconds != constants.DefaultInitialDelaySeconds || probe.PeriodSeconds != constants.DefaultPeriodSeconds {
		t.Errorf("%s timing = initial %d period %d, want initial %d period %d", name, probe.InitialDelaySeconds, probe.PeriodSeconds, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)
	}
}

func assertHTTPProbe(t *testing.T, name string, probe *corev1.Probe, wantPort int32) {
	t.Helper()
	if probe == nil || probe.HTTPGet == nil {
		t.Fatalf("%s probe = %#v, want HTTP probe", name, probe)
	}
	if probe.HTTPGet.Port != intstr.FromInt32(wantPort) || probe.HTTPGet.Path != constants.HealthCheckPath {
		t.Errorf("%s HTTP target = port %#v path %q, want port %d path %q", name, probe.HTTPGet.Port, probe.HTTPGet.Path, wantPort, constants.HealthCheckPath)
	}
	if probe.InitialDelaySeconds != constants.DefaultInitialDelaySeconds || probe.PeriodSeconds != constants.DefaultPeriodSeconds {
		t.Errorf("%s timing = initial %d period %d, want initial %d period %d", name, probe.InitialDelaySeconds, probe.PeriodSeconds, constants.DefaultInitialDelaySeconds, constants.DefaultPeriodSeconds)
	}
}

func assertPrivilegedBEInitContainer(t *testing.T, containers []corev1.Container) {
	t.Helper()
	if len(containers) != 1 {
		t.Fatalf("BE init containers = %#v, want exactly one", containers)
	}
	initContainer := containers[0]
	if initContainer.Name != constants.InitContainerName || initContainer.Image != constants.DefaultInitImage {
		t.Errorf("BE init container identity = (%q, %q), want (%q, %q)", initContainer.Name, initContainer.Image, constants.InitContainerName, constants.DefaultInitImage)
	}
	if !reflect.DeepEqual(initContainer.Command, []string{"sh", "-c", constants.BEInitCommand}) {
		t.Errorf("BE init command = %#v, want sh -c %q", initContainer.Command, constants.BEInitCommand)
	}
	if initContainer.SecurityContext == nil || initContainer.SecurityContext.Privileged == nil || !*initContainer.SecurityContext.Privileged {
		t.Errorf("BE init SecurityContext = %#v, want privileged=true", initContainer.SecurityContext)
	}
}

func assertQuantityEntry(
	t *testing.T,
	resources corev1.ResourceList,
	name corev1.ResourceName,
	want *resource.Quantity,
) {
	t.Helper()
	got, exists := resources[name]
	if want == nil {
		if exists {
			t.Errorf("resource %q = %s, want absent", name, got.String())
		}
		return
	}
	if !exists {
		t.Errorf("resource %q is absent, want %s", name, want.String())
		return
	}
	if got.Cmp(*want) != 0 {
		t.Errorf("resource %q = %s, want %s", name, got.String(), want.String())
	}
}

func assertLegacyConfigVolume(t *testing.T, statefulSet *appsv1.StatefulSet, configMapName string) {
	t.Helper()
	var configVolume *corev1.Volume
	for i := range statefulSet.Spec.Template.Spec.Volumes {
		volume := &statefulSet.Spec.Template.Spec.Volumes[i]
		if volume.Name == reconciler.ConfigVolumeName {
			t.Errorf("StatefulSet still contains framework config volume name %q", reconciler.ConfigVolumeName)
		}
		if volume.Name == constants.ConfigVolumeName {
			configVolume = volume
		}
	}
	if configVolume == nil || configVolume.ConfigMap == nil || configVolume.ConfigMap.Name != configMapName {
		t.Fatalf("legacy config volume = %#v, want ConfigMap %q", configVolume, configMapName)
	}
	main := statefulSet.Spec.Template.Spec.Containers[0]
	mount := findVolumeMount(main.VolumeMounts, constants.ConfigVolumeName)
	if mount == nil || mount.MountPath != constants.DefaultConfigMapPath || mount.ReadOnly {
		t.Errorf("legacy config mount = %#v, want readOnly=false %q at %q", mount, constants.ConfigVolumeName, constants.DefaultConfigMapPath)
	}
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func volumeMountNames(mounts []corev1.VolumeMount) []string {
	names := make([]string, 0, len(mounts))
	for _, mount := range mounts {
		names = append(names, mount.Name)
	}
	return names
}

func volumeNames(volumes []corev1.Volume) []string {
	names := make([]string, 0, len(volumes))
	for _, volume := range volumes {
		names = append(names, volume.Name)
	}
	return names
}

func selectorMatches(selector, labels map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func lastEnvValue(env []corev1.EnvVar, name string) (string, bool) {
	for i := len(env) - 1; i >= 0; i-- {
		if env[i].Name == name {
			return env[i].Value, true
		}
	}
	return "", false
}

func configPropertyKeys(content string) []string {
	lines := strings.Split(content, "\n")
	keys := make([]string, 0, len(lines))
	for _, line := range lines {
		key, _, found := strings.Cut(line, "=")
		if found {
			keys = append(keys, strings.TrimSpace(key))
		}
	}
	return keys
}

func hasProperty(content, key, value string) bool {
	return hasPropertyLine(content, key+"="+value)
}

func hasPropertyLine(content, line string) bool {
	for _, candidate := range strings.Split(content, "\n") {
		if candidate == line {
			return true
		}
	}
	return false
}

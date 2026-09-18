/*
Copyright 2026 zncdatadev.

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
	"errors"
	"fmt"
	"sort"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	opcommon "github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	legacyRoleServiceExtensionName = "legacy-role-services"
	legacyInternalServiceRole      = "internal"
	legacyAccessServiceRole        = "access"
	legacyFixedServiceRoleGroup    = "service"
)

var _ opcommon.ClusterExtension[*dorisv1alpha1.DorisCluster] = (*LegacyRoleServiceExtension)(nil)

// LegacyRoleServiceExtension owns the two legacy, role-wide Services independently
// of any role group. It also bridges the legacy workload label identity into the
// Gen 3 status-ledger and health contracts; see legacy_compatibility.go.
type LegacyRoleServiceExtension struct {
	scheme *runtime.Scheme
}

// NewLegacyRoleServiceExtension creates the cluster extension for legacy role Services.
func NewLegacyRoleServiceExtension(scheme *runtime.Scheme) *LegacyRoleServiceExtension {
	return &LegacyRoleServiceExtension{scheme: scheme}
}

// Name implements common.Extension.
func (e *LegacyRoleServiceExtension) Name() string {
	return legacyRoleServiceExtensionName
}

// PreReconcile creates or updates role-wide Services before role-group resources
// are reconciled. The Services carry only a cluster controller reference, never a
// role-group identity label, so role-group cleanup cannot claim them.
func (e *LegacyRoleServiceExtension) PreReconcile(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	if e.scheme == nil {
		return fmt.Errorf("reconcile legacy Doris role Services: scheme is required")
	}
	if err := restoreLegacyRoleGroupLedger(ctx, k8sClient, cluster); err != nil {
		return err
	}

	roles := []legacyRoleServicePlan{
		{component: constants.ComponentTypeFE, spec: cluster.Spec.Frontend},
		{component: constants.ComponentTypeBE, spec: cluster.Spec.Backend},
		{component: constants.ComponentTypeBroker, spec: cluster.Spec.Broker, optional: true},
	}

	// A fixed role-group Service is named <cluster>-<role>-<group>. The two
	// legacy role-wide names therefore collide with fixed slots for groups named
	// "internal" or "service". Validate every role before mutating anything so
	// an invalid declaration cannot leave a partially reconciled Service set.
	for _, role := range roles {
		if role.spec == nil {
			continue
		}
		if err := validateLegacyRoleServiceNames(cluster.Name, role.component, role.spec); err != nil {
			return err
		}
		if err := validateFixedRoleGroupServiceOwnership(ctx, k8sClient, cluster, role.component, role.spec); err != nil {
			return err
		}
	}

	var errs []error
	for _, role := range roles {
		if role.spec == nil {
			if role.optional {
				errs = append(errs, deleteOwnedLegacyRoleServices(ctx, k8sClient, cluster, role.component))
			}
			continue
		}

		if err := e.reconcileRoleServices(ctx, k8sClient, cluster, role.component); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func validateFixedRoleGroupServiceOwnership(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
	role *dorisv1alpha1.RoleSpec,
) error {
	groups := make([]string, 0, len(role.RoleGroups))
	for group := range role.RoleGroups {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	for _, group := range groups {
		service := &corev1.Service{}
		key := types.NamespacedName{
			Name:      reconciler.RoleGroupResourceName(cluster.Name, string(component), group),
			Namespace: cluster.Namespace,
		}
		if err := k8sClient.Get(ctx, key, service); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("preflight Doris %s/%s governing Service %s: %w", component, group, key, err)
		}
		if !metav1.IsControlledBy(service, cluster) {
			return fmt.Errorf(
				"cannot create Doris %s/%s governing Service %s: an existing Service at the new operator-go v0.13 slot is not controlled by this DorisCluster; remove or migrate that Service before upgrading",
				component,
				group,
				key,
			)
		}
	}
	return nil
}

// PostReconcile implements common.ClusterExtension.
func (e *LegacyRoleServiceExtension) PostReconcile(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
) error {
	return reportLegacyPodFailures(ctx, k8sClient, cluster)
}

// OnReconcileError implements common.ClusterExtension.
func (e *LegacyRoleServiceExtension) OnReconcileError(
	_ context.Context,
	_ ctrlclient.Client,
	_ *dorisv1alpha1.DorisCluster,
	_ error,
) error {
	return nil
}

type legacyRoleServicePlan struct {
	component constants.ComponentType
	spec      *dorisv1alpha1.RoleSpec
	optional  bool
}

func validateLegacyRoleServiceNames(
	clusterName string,
	component constants.ComponentType,
	role *dorisv1alpha1.RoleSpec,
) error {
	for _, reserved := range []string{legacyInternalServiceRole, legacyFixedServiceRoleGroup} {
		if _, exists := role.RoleGroups[reserved]; exists {
			return fmt.Errorf(
				"doris %s role group %q is not supported: its fixed Service name collides with the legacy role-wide Service",
				component,
				reserved,
			)
		}
	}
	if component != constants.ComponentTypeFE && component != constants.ComponentTypeBE {
		return nil
	}

	groups := make([]string, 0, len(role.RoleGroups))
	fixedServiceOwners := make(map[string]string, len(role.RoleGroups))
	for roleGroupName := range role.RoleGroups {
		groups = append(groups, roleGroupName)
		fixedServiceOwners[reconciler.RoleGroupResourceName(
			clusterName,
			string(component),
			roleGroupName,
		)] = roleGroupName
	}
	sort.Strings(groups)
	for _, roleGroupName := range groups {
		metricsServiceName := reconciler.RoleGroupResourceName(
			clusterName,
			string(component),
			roleGroupName,
		) + "-metrics"
		if fixedOwner, exists := fixedServiceOwners[metricsServiceName]; exists {
			return fmt.Errorf(
				"doris %s role groups %q and %q are not supported together: the first group's metrics Service name collides with the second group's fixed Service name",
				component,
				roleGroupName,
				fixedOwner,
			)
		}
	}
	return nil
}

func (e *LegacyRoleServiceExtension) reconcileRoleServices(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
) error {
	definitions := []legacyRoleServiceDefinition{
		{
			name:            cluster.Name + "-" + string(component) + constants.ServiceInternalSuffix,
			serviceRole:     legacyInternalServiceRole,
			ports:           internalPortsForRole(component),
			headless:        true,
			publishNotReady: true,
		},
		{
			name:        cluster.Name + "-" + string(component) + constants.ServiceAccessSuffix,
			serviceRole: legacyAccessServiceRole,
			ports:       accessPortsForRole(component),
		},
	}

	var errs []error
	for _, definition := range definitions {
		if err := e.reconcileRoleService(ctx, k8sClient, cluster, component, definition); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type legacyRoleServiceDefinition struct {
	name            string
	serviceRole     string
	ports           []corev1.ServicePort
	headless        bool
	publishNotReady bool
}

// internalPortsForRole preserves the legacy peer-discovery contract: FE peers
// discover the query port, BE peers discover the heartbeat port, and Broker's
// sole IPC port is used for both internal and client access.
func internalPortsForRole(component constants.ComponentType) []corev1.ServicePort {
	switch component {
	case constants.ComponentTypeFE:
		return servicePorts([]corev1.ContainerPort{
			containerPort(constants.FEQueryPortName, constants.FEQueryPort),
		})
	case constants.ComponentTypeBE:
		return servicePorts([]corev1.ContainerPort{
			containerPort(constants.BEHeartbeatPortName, constants.BEHeartbeatPort),
		})
	case constants.ComponentTypeBroker:
		return servicePorts(brokerContainerPorts())
	default:
		return nil
	}
}

// accessPortsForRole exposes every legacy client-facing port for the role.
func accessPortsForRole(component constants.ComponentType) []corev1.ServicePort {
	switch component {
	case constants.ComponentTypeFE:
		return servicePorts(feContainerPorts())
	case constants.ComponentTypeBE:
		return servicePorts(beContainerPorts())
	case constants.ComponentTypeBroker:
		return servicePorts(brokerContainerPorts())
	default:
		return nil
	}
}

func (e *LegacyRoleServiceExtension) reconcileRoleService(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
	definition legacyRoleServiceDefinition,
) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      definition.name,
		Namespace: cluster.Namespace,
	}}

	if _, err := controllerutil.CreateOrPatch(ctx, k8sClient, service, func() error {
		creating := service.ResourceVersion == ""
		if !creating {
			switch {
			case definition.headless && service.Spec.ClusterIP != corev1.ClusterIPNone:
				return fmt.Errorf("existing Service %s/%s is not headless and its clusterIP is immutable", service.Namespace, service.Name)
			case !definition.headless && service.Spec.ClusterIP == corev1.ClusterIPNone:
				return fmt.Errorf("existing Service %s/%s is headless and its clusterIP is immutable", service.Namespace, service.Name)
			}
		}

		service.Labels = legacyRoleServiceLabels(cluster.Name, component, definition.serviceRole)
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Selector = legacyRoleSelector(cluster.Name, string(component))
		service.Spec.Ports = preserveAllocatedServicePorts(service.Spec.Ports, definition.ports)
		service.Spec.PublishNotReadyAddresses = definition.publishNotReady
		if creating && definition.headless {
			// Set only clusterIP on create. clusterIPs, ipFamilies and
			// ipFamilyPolicy are allocated/defaulted by the API server and are
			// deliberately left untouched on every update.
			service.Spec.ClusterIP = corev1.ClusterIPNone
		}

		return controllerutil.SetControllerReference(cluster, service, e.scheme)
	}); err != nil {
		return fmt.Errorf("reconcile legacy Doris %s Service %s/%s: %w", definition.serviceRole, service.Namespace, service.Name, err)
	}

	return nil
}

func legacyRoleServiceLabels(clusterName string, component constants.ComponentType, serviceRole string) map[string]string {
	return map[string]string{
		constants.OwnerReferenceLabelKey: clusterName,
		constants.ServiceRoleLabelKey:    serviceRole,
		constants.ComponentLabelKey:      string(component),
	}
}

func preserveAllocatedServicePorts(live, desired []corev1.ServicePort) []corev1.ServicePort {
	result := make([]corev1.ServicePort, len(desired))
	copy(result, desired)
	for i := range result {
		for _, livePort := range live {
			if (result[i].Name != "" && result[i].Name == livePort.Name) ||
				(result[i].Name == "" && result[i].Port == livePort.Port) {
				if result[i].NodePort == 0 {
					result[i].NodePort = livePort.NodePort
				}
				break
			}
		}
	}
	return result
}

func deleteOwnedLegacyRoleServices(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
) error {
	var errs []error
	for _, suffix := range []string{constants.ServiceInternalSuffix, constants.ServiceAccessSuffix} {
		service := &corev1.Service{}
		key := types.NamespacedName{
			Name:      cluster.Name + "-" + string(component) + suffix,
			Namespace: cluster.Namespace,
		}
		if err := k8sClient.Get(ctx, key, service); err != nil {
			if !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("get optional Doris %s Service %s: %w", component, key, err))
			}
			continue
		}
		if !metav1.IsControlledBy(service, cluster) {
			continue
		}
		if err := k8sClient.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete optional Doris %s Service %s: %w", component, key, err))
		}
	}
	return errors.Join(errs...)
}

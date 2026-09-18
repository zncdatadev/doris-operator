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
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const legacyServiceTestClusterIP = "10.96.0.42"

func TestLegacyRoleServiceExtensionCreatesRoleWideServices(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Spec.Broker = &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
		handlerTestDefaultGroup: {Replicas: ptr.To[int32](1)},
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster).Build()

	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())

	expectedPorts := map[constants.ComponentType]struct {
		internal []string
		access   []string
	}{
		constants.ComponentTypeFE: {
			internal: []string{"fe-query/9030/fe-query/TCP"},
			access: []string{
				"fe-http/8030/fe-http/TCP",
				"fe-rpc/9020/fe-rpc/TCP",
				"fe-query/9030/fe-query/TCP",
				"fe-edit-log/9010/fe-edit-log/TCP",
			},
		},
		constants.ComponentTypeBE: {
			internal: []string{"be-heartbeat/9050/be-heartbeat/TCP"},
			access: []string{
				"be-rpc/9060/be-rpc/TCP",
				"be-http/8040/be-http/TCP",
				"be-heartbeat/9050/be-heartbeat/TCP",
				"be-brpc/8060/be-brpc/TCP",
			},
		},
		constants.ComponentTypeBroker: {
			internal: []string{"broker-ipc/8000/broker-ipc/TCP"},
			access:   []string{"broker-ipc/8000/broker-ipc/TCP"},
		},
	}

	for _, component := range []constants.ComponentType{
		constants.ComponentTypeFE,
		constants.ComponentTypeBE,
		constants.ComponentTypeBroker,
	} {
		internal := getLegacyRoleService(t, k8sClient, cluster, component, constants.ServiceInternalSuffix)
		g.Expect(internal.Labels).To(Equal(legacyRoleServiceLabels(cluster.Name, component, legacyInternalServiceRole)))
		g.Expect(internal.Labels).NotTo(HaveKey("app.kubernetes.io/role-group"))
		g.Expect(internal.Spec.Selector).To(Equal(legacyRoleSelector(cluster.Name, string(component))))
		g.Expect(internal.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		g.Expect(internal.Spec.PublishNotReadyAddresses).To(BeTrue())
		g.Expect(legacyServicePortContract(internal.Spec.Ports)).To(Equal(expectedPorts[component].internal))
		g.Expect(metav1.IsControlledBy(internal, cluster)).To(BeTrue())

		access := getLegacyRoleService(t, k8sClient, cluster, component, constants.ServiceAccessSuffix)
		g.Expect(access.Labels).To(Equal(legacyRoleServiceLabels(cluster.Name, component, legacyAccessServiceRole)))
		g.Expect(access.Labels).NotTo(HaveKey("app.kubernetes.io/role-group"))
		g.Expect(access.Spec.Selector).To(Equal(legacyRoleSelector(cluster.Name, string(component))))
		g.Expect(access.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		g.Expect(access.Spec.PublishNotReadyAddresses).To(BeFalse())
		g.Expect(legacyServicePortContract(access.Spec.Ports)).To(Equal(expectedPorts[component].access))
		g.Expect(metav1.IsControlledBy(access, cluster)).To(BeTrue())
	}
}

func TestLegacyRoleServiceExtensionPreservesAllocatedServiceFields(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	singleStack := corev1.IPFamilyPolicySingleStack

	internal := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-fe" + constants.ServiceInternalSuffix,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:      corev1.ClusterIPNone,
			ClusterIPs:     []string{corev1.ClusterIPNone},
			IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
			IPFamilyPolicy: &singleStack,
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, internal, testScheme)).To(Succeed())

	access := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-fe" + constants.ServiceAccessSuffix,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:      legacyServiceTestClusterIP,
			ClusterIPs:     []string{legacyServiceTestClusterIP},
			IPFamilies:     []corev1.IPFamily{corev1.IPv4Protocol},
			IPFamilyPolicy: &singleStack,
		},
	}
	g.Expect(controllerutil.SetControllerReference(cluster, access, testScheme)).To(Succeed())

	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, internal, access).Build()
	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())

	updatedInternal := getLegacyRoleService(t, k8sClient, cluster, constants.ComponentTypeFE, constants.ServiceInternalSuffix)
	g.Expect(updatedInternal.Spec.ClusterIPs).To(Equal([]string{corev1.ClusterIPNone}))
	g.Expect(updatedInternal.Spec.IPFamilies).To(Equal([]corev1.IPFamily{corev1.IPv4Protocol}))
	g.Expect(updatedInternal.Spec.IPFamilyPolicy).To(Equal(&singleStack))

	updatedAccess := getLegacyRoleService(t, k8sClient, cluster, constants.ComponentTypeFE, constants.ServiceAccessSuffix)
	g.Expect(updatedAccess.Spec.ClusterIP).To(Equal(legacyServiceTestClusterIP))
	g.Expect(updatedAccess.Spec.ClusterIPs).To(Equal([]string{legacyServiceTestClusterIP}))
	g.Expect(updatedAccess.Spec.IPFamilies).To(Equal([]corev1.IPFamily{corev1.IPv4Protocol}))
	g.Expect(updatedAccess.Spec.IPFamilyPolicy).To(Equal(&singleStack))
}

func TestLegacyRoleServiceExtensionDeletesServicesForAbsentOptionalRole(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()

	objects := make([]ctrlclient.Object, 0, 3)
	objects = append(objects, cluster)
	for _, suffix := range []string{constants.ServiceInternalSuffix, constants.ServiceAccessSuffix} {
		service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-broker" + suffix,
			Namespace: cluster.Namespace,
		}}
		g.Expect(controllerutil.SetControllerReference(cluster, service, testScheme)).To(Succeed())
		objects = append(objects, service)
	}

	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(objects...).Build()
	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())

	for _, suffix := range []string{constants.ServiceInternalSuffix, constants.ServiceAccessSuffix} {
		service := &corev1.Service{}
		err := k8sClient.Get(context.Background(), types.NamespacedName{
			Name:      cluster.Name + "-broker" + suffix,
			Namespace: cluster.Namespace,
		}, service)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
	}
}

func TestLegacyRoleServiceExtensionDoesNotDeleteUnownedOptionalRoleService(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	unowned := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      cluster.Name + "-broker" + constants.ServiceAccessSuffix,
		Namespace: cluster.Namespace,
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, unowned).Build()

	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())
	g.Expect(k8sClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(unowned), &corev1.Service{})).To(Succeed())
}

func TestLegacyRoleServiceExtensionRejectsFixedSlotNameCollisionsBeforeMutation(t *testing.T) {
	for _, reserved := range []string{legacyInternalServiceRole, legacyFixedServiceRoleGroup} {
		t.Run(reserved, func(t *testing.T) {
			g := NewWithT(t)
			testScheme := legacyRoleServiceTestScheme(t)
			cluster := legacyRoleServiceTestCluster()
			cluster.Spec.Frontend.RoleGroups[reserved] = dorisv1alpha1.RoleGroupSpec{}
			k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster).Build()

			extension := NewLegacyRoleServiceExtension(testScheme)
			err := extension.PreReconcile(context.Background(), k8sClient, cluster)
			g.Expect(err).To(MatchError(ContainSubstring("fixed Service name collides")))

			services := &corev1.ServiceList{}
			g.Expect(k8sClient.List(context.Background(), services, ctrlclient.InNamespace(cluster.Namespace))).To(Succeed())
			g.Expect(services.Items).To(BeEmpty(), "name validation must finish before the first Service mutation")
		})
	}
}

func TestLegacyRoleServiceExtensionRejectsMetricsSiblingCollisionBeforeMutation(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Spec.Frontend.RoleGroups[handlerTestDefaultGroup+"-metrics"] = dorisv1alpha1.RoleGroupSpec{}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster).Build()

	extension := NewLegacyRoleServiceExtension(testScheme)
	err := extension.PreReconcile(context.Background(), k8sClient, cluster)
	g.Expect(err).To(MatchError(ContainSubstring("metrics Service name collides")))

	services := &corev1.ServiceList{}
	g.Expect(k8sClient.List(context.Background(), services, ctrlclient.InNamespace(cluster.Namespace))).To(Succeed())
	g.Expect(services.Items).To(BeEmpty(), "name validation must finish before the first Service mutation")
}

func TestLegacyRoleServiceExtensionRejectsExternalFixedServiceBeforeMutation(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	external := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: reconciler.RoleGroupResourceName(
			cluster.Name,
			string(constants.ComponentTypeFE),
			handlerTestDefaultGroup,
		),
		Namespace: cluster.Namespace,
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, external).Build()

	err := NewLegacyRoleServiceExtension(testScheme).PreReconcile(context.Background(), k8sClient, cluster)
	g.Expect(err).To(MatchError(ContainSubstring("is not controlled by this DorisCluster")))

	services := &corev1.ServiceList{}
	g.Expect(k8sClient.List(context.Background(), services, ctrlclient.InNamespace(cluster.Namespace))).To(Succeed())
	g.Expect(services.Items).To(HaveLen(1), "ownership validation must finish before role-wide Service mutation")
	g.Expect(services.Items[0].Name).To(Equal(external.Name))
}

func TestLegacyRoleServiceExtensionAllowsOwnedFixedService(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	owned := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: reconciler.RoleGroupResourceName(
			cluster.Name,
			string(constants.ComponentTypeFE),
			handlerTestDefaultGroup,
		),
		Namespace: cluster.Namespace,
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, owned, testScheme)).To(Succeed())
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, owned).Build()

	g.Expect(NewLegacyRoleServiceExtension(testScheme).PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())
}

func TestLegacyRoleServiceExtensionAllowsNonCollidingMetricsSuffixGroups(t *testing.T) {
	tests := []struct {
		name      string
		component constants.ComponentType
		group     string
	}{
		{
			name:      "broker has no metrics Service",
			component: constants.ComponentTypeBroker,
			group:     handlerTestDefaultGroup,
		},
		{
			name:      "hashed FE names occupy distinct slots",
			component: constants.ComponentTypeFE,
			group:     strings.Repeat("x", 50),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewWithT(t)
			testScheme := legacyRoleServiceTestScheme(t)
			cluster := legacyRoleServiceTestCluster()
			role := &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
				test.group:              {},
				test.group + "-metrics": {},
			}}
			switch test.component {
			case constants.ComponentTypeFE:
				cluster.Spec.Frontend = role
			case constants.ComponentTypeBroker:
				cluster.Spec.Broker = role
			}
			k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster).Build()

			extension := NewLegacyRoleServiceExtension(testScheme)
			g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())
		})
	}
}

func TestLegacyRoleServiceExtensionRestoresOwnedRoleGroupLedger(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()

	ownedStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      reconciler.RoleGroupResourceName(cluster.Name, "fe", "removed"),
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "fe", "removed"),
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, ownedStatefulSet, testScheme)).To(Succeed())
	ownedConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      reconciler.RoleGroupResourceName(cluster.Name, "be", "archived"),
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "be", "archived"),
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, ownedConfigMap, testScheme)).To(Succeed())
	foreignStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      reconciler.RoleGroupResourceName(cluster.Name, "broker", "foreign"),
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "broker", "foreign"),
	}}
	wrongName := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      "not-a-role-group-slot",
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "fe", "ignored"),
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, wrongName, testScheme)).To(Succeed())
	serviceOnly := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      reconciler.RoleGroupResourceName(cluster.Name, "fe", "service-only") + "-metrics",
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "fe", "service-only"),
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, serviceOnly, testScheme)).To(Succeed())

	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(
		cluster,
		ownedStatefulSet,
		ownedConfigMap,
		foreignStatefulSet,
		wrongName,
		serviceOnly,
	).Build()
	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PreReconcile(context.Background(), k8sClient, cluster)).To(Succeed())

	g.Expect(cluster.Status.RoleGroups["fe"]).To(ConsistOf("removed", "service-only"))
	g.Expect(cluster.Status.RoleGroups["be"]).To(ConsistOf("archived"))
	g.Expect(cluster.Status.RoleGroups).NotTo(HaveKey("broker"))
}

func TestLegacyRoleServiceExtensionRejectsPreHashResourceIdentity(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Name = "doris-cluster-with-a-name-long-enough-for-migration"
	legacyName := cluster.Name + "-fe-default"
	g.Expect(legacyName).NotTo(Equal(reconciler.RoleGroupResourceName(cluster.Name, "fe", "default")))

	legacyStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      legacyName,
		Namespace: cluster.Namespace,
		Labels:    legacyRoleGroupSelector(cluster.Name, "fe", "default"),
	}}
	g.Expect(controllerutil.SetControllerReference(cluster, legacyStatefulSet, testScheme)).To(Succeed())
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, legacyStatefulSet).Build()

	err := NewLegacyRoleServiceExtension(testScheme).PreReconcile(context.Background(), k8sClient, cluster)
	g.Expect(err).To(MatchError(ContainSubstring("cannot migrate legacy Doris role group fe/default")))
}

func TestLegacyRoleServiceExtensionReportsLegacyPodFailures(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Status.SetObservedGeneration(7)
	cluster.Status.SetDegraded(false, commonsv1alpha1.ReasonAvailable, "No failing pods")

	failedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "simple-doris-fe-default-0",
			Namespace: cluster.Namespace,
			Labels:    legacyRoleGroupSelector(cluster.Name, "fe", handlerTestDefaultGroup),
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "fe",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: legacyReasonCrashLoopBackOff,
			}},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, failedPod).Build()
	extension := NewLegacyRoleServiceExtension(testScheme)
	g.Expect(extension.PostReconcile(context.Background(), k8sClient, cluster)).To(Succeed())

	condition := cluster.Status.GetCondition(commonsv1alpha1.ConditionDegraded)
	g.Expect(condition).NotTo(BeNil())
	g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(condition.Reason).To(Equal(commonsv1alpha1.ReasonPodFailure))
	g.Expect(condition.Message).To(ContainSubstring("simple-doris-fe-default-0 (" + legacyReasonCrashLoopBackOff + ")"))
}

func TestLegacyRoleServiceExtensionPreservesMoreSpecificDegradedCondition(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Status.SetDegraded(true, commonsv1alpha1.ReasonWorkloadUnreadable, "StatefulSet missing")
	failedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "simple-doris-fe-default-0",
			Namespace: cluster.Namespace,
			Labels:    legacyRoleGroupSelector(cluster.Name, "fe", handlerTestDefaultGroup),
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: legacyReasonImagePullBackOff}},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, failedPod).Build()

	g.Expect(NewLegacyRoleServiceExtension(testScheme).PostReconcile(
		context.Background(), k8sClient, cluster,
	)).To(Succeed())
	condition := cluster.Status.GetCondition(commonsv1alpha1.ConditionDegraded)
	g.Expect(condition.Reason).To(Equal(commonsv1alpha1.ReasonWorkloadUnreadable))
	g.Expect(condition.Message).To(Equal("StatefulSet missing"))
}

func TestLegacyRoleServiceExtensionIgnoresTransientAndTerminatingPods(t *testing.T) {
	g := NewWithT(t)
	testScheme := legacyRoleServiceTestScheme(t)
	cluster := legacyRoleServiceTestCluster()
	cluster.Status.SetDegraded(false, commonsv1alpha1.ReasonAvailable, "No failing pods")
	now := metav1.Now()
	podLabels := legacyRoleGroupSelector(cluster.Name, "fe", handlerTestDefaultGroup)
	transient := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "transient", Namespace: cluster.Namespace, Labels: podLabels},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
		}}},
	}
	terminating := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "terminating",
			Namespace:         cluster.Namespace,
			Labels:            podLabels,
			DeletionTimestamp: &now,
			Finalizers:        []string{"test.kubedoop.dev/hold"},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: legacyReasonCrashLoopBackOff}},
		}}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(cluster, transient, terminating).Build()

	g.Expect(NewLegacyRoleServiceExtension(testScheme).PostReconcile(
		context.Background(), k8sClient, cluster,
	)).To(Succeed())
	condition := cluster.Status.GetCondition(commonsv1alpha1.ConditionDegraded)
	g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
	g.Expect(condition.Reason).To(Equal(commonsv1alpha1.ReasonAvailable))
}

func TestLegacyPodFailureReasonIncludesUnschedulableAndInitContainers(t *testing.T) {
	tests := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{
			name: "unschedulable",
			pod: corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			}}}},
			want: corev1.PodReasonUnschedulable,
		},
		{
			name: "init image pull",
			pod: corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: legacyReasonImagePullBackOff}},
			}}}},
			want: legacyReasonImagePullBackOff,
		},
		{name: "healthy", pod: corev1.Pod{}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyPodFailureReason(&tt.pod); got != tt.want {
				t.Fatalf("legacyPodFailureReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func legacyRoleServiceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	testScheme := runtime.NewScheme()
	g := NewWithT(t)
	g.Expect(appsv1.AddToScheme(testScheme)).To(Succeed())
	g.Expect(corev1.AddToScheme(testScheme)).To(Succeed())
	g.Expect(dorisv1alpha1.AddToScheme(testScheme)).To(Succeed())
	return testScheme
}

func legacyRoleServiceTestCluster() *dorisv1alpha1.DorisCluster {
	return &dorisv1alpha1.DorisCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "simple-doris",
			Namespace: handlerTestNamespace,
			UID:       types.UID("doris-cluster-uid"),
		},
		Spec: dorisv1alpha1.DorisClusterSpec{
			Frontend: &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
				handlerTestDefaultGroup: {Replicas: ptr.To[int32](1)},
			}},
			Backend: &dorisv1alpha1.RoleSpec{RoleGroups: map[string]dorisv1alpha1.RoleGroupSpec{
				handlerTestDefaultGroup: {Replicas: ptr.To[int32](1)},
			}},
		},
	}
}

func getLegacyRoleService(
	t *testing.T,
	k8sClient ctrlclient.Client,
	cluster *dorisv1alpha1.DorisCluster,
	component constants.ComponentType,
	suffix string,
) *corev1.Service {
	t.Helper()
	service := &corev1.Service{}
	g := NewWithT(t)
	g.Expect(k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      cluster.Name + "-" + string(component) + suffix,
		Namespace: cluster.Namespace,
	}, service)).To(Succeed())
	return service
}

func legacyServicePortContract(ports []corev1.ServicePort) []string {
	result := make([]string, 0, len(ports))
	for _, port := range ports {
		result = append(result, fmt.Sprintf(
			"%s/%d/%s/%s",
			port.Name,
			port.Port,
			port.TargetPort.String(),
			port.Protocol,
		))
	}
	return result
}

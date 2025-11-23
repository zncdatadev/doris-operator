package common

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	ctrl "sigs.k8s.io/controller-runtime"
)

var logger = ctrl.Log.WithName("role-reconciler")

// DorisComponentReconciler is the interface that all component reconcilers must implement
type DorisComponentReconciler interface {
	// RegisterResourceWithRoleGroup registers resources for a specific role group
	RegisterResourceWithRoleGroup(
		ctx context.Context,
		replicas *int32,
		roleGroupInfo *reconciler.RoleGroupInfo,
		overrides *commonsv1alpha1.OverridesSpec,
		config *dorisv1alpha1.ConfigSpec,
	) ([]reconciler.Reconciler, error)
}

// DorisComponentResourceBuilder defines methods that component implementations should provide
type DorisComponentResourceBuilder interface {
	// CreateServiceReconcilers returns internal and access service reconcilers
	CreateServiceReconcilers(
		client *client.Client,
		roleGroupInfo *reconciler.RoleGroupInfo,
	) []reconciler.Reconciler

	// CreateStatefulSetReconciler returns statefulset reconciler
	CreateStatefulSetReconciler(
		ctx context.Context,
		client *client.Client,
		image *opgoutil.Image,
		replicas *int32,
		dorisCluster *dorisv1alpha1.DorisCluster,
		clusterOperation *commonsv1alpha1.ClusterOperationSpec,
		roleGroupInfo *reconciler.RoleGroupInfo,
		config *dorisv1alpha1.ConfigSpec,
		overrides *commonsv1alpha1.OverridesSpec,
	) (reconciler.Reconciler, error)
}

// BaseDorisRoleReconciler is the common base for both FE and BE role reconcilers
type BaseDorisRoleReconciler struct {
	reconciler.BaseRoleReconciler[*dorisv1alpha1.RoleSpec]
	DorisCluster     *dorisv1alpha1.DorisCluster
	ClusterConfig    *dorisv1alpha1.ClusterConfigSpec
	ClusterOperation *commonsv1alpha1.ClusterOperationSpec
	Image            *opgoutil.Image
	ComponentType    string
	ComponentRec     DorisComponentReconciler
}

// NewBaseDorisRoleReconciler creates a new base role reconciler for Doris components
func NewBaseDorisRoleReconciler(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *dorisv1alpha1.RoleSpec,
	dorisCluster *dorisv1alpha1.DorisCluster,
	image *opgoutil.Image,
	componentType string,
	componentRec DorisComponentReconciler,
) *BaseDorisRoleReconciler {
	stopped := false
	if dorisCluster.Spec.ClusterOperationSpec != nil {
		stopped = dorisCluster.Spec.ClusterOperationSpec.Stopped
	}

	return &BaseDorisRoleReconciler{
		BaseRoleReconciler: *reconciler.NewBaseRoleReconciler(
			client,
			stopped,
			roleInfo,
			spec,
		),
		DorisCluster:     dorisCluster,
		ClusterConfig:    dorisCluster.Spec.ClusterConfig,
		ClusterOperation: dorisCluster.Spec.ClusterOperationSpec,
		Image:            image,
		ComponentType:    componentType,
		ComponentRec:     componentRec,
	}
}

// RegisterResources registers all resources for all role groups
func (r *BaseDorisRoleReconciler) RegisterResources(ctx context.Context) error {
	for name, roleGroup := range r.Spec.RoleGroups {
		// Merge configurations
		mergedConfig, err := opgoutil.MergeObject(r.Spec.Config, roleGroup.Config)
		if err != nil {
			return err
		}

		// Merge override configurations
		overrides, err := opgoutil.MergeObject(r.Spec.OverridesSpec, roleGroup.OverridesSpec)
		if err != nil {
			return err
		}

		if overrides == nil {
			overrides = &commonsv1alpha1.OverridesSpec{}
		}

		info := &reconciler.RoleGroupInfo{
			RoleInfo:      r.RoleInfo,
			RoleGroupName: name,
		}

		reconcilers, err := r.ComponentRec.RegisterResourceWithRoleGroup(
			ctx,
			roleGroup.Replicas,
			info,
			overrides,
			mergedConfig,
		)
		if err != nil {
			return err
		}

		for _, reconciler := range reconcilers {
			r.AddResource(reconciler)
			logger.Info("registered resource", "role", r.GetName(), "roleGroup", name, "reconciler", reconciler.GetName())
		}
	}
	return nil
}

// RegisterStandardResources registers common resources for a Doris component
func RegisterStandardResources(
	ctx context.Context,
	client *client.Client,
	builder DorisComponentResourceBuilder,
	replicas *int32,
	image *opgoutil.Image,
	dorisCluster *dorisv1alpha1.DorisCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *dorisv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) ([]reconciler.Reconciler, error) {
	var reconcilers = make([]reconciler.Reconciler, 0)

	// Create services
	serviceReconcilers := builder.CreateServiceReconcilers(client, roleGroupInfo)
	reconcilers = append(reconcilers, serviceReconcilers...)

	// Create metrics service
	metricsSvc := NewRoleGroupMetricsService(client, roleGroupInfo)
	if metricsSvc != nil {
		reconcilers = append(reconcilers, metricsSvc)
	}

	// Create StatefulSet
	statefulSetReconciler, err := builder.CreateStatefulSetReconciler(
		ctx,
		client,
		image,
		replicas,
		dorisCluster,
		clusterOperation,
		roleGroupInfo,
		config,
		overrides,
	)
	if err != nil {
		return nil, err
	}
	reconcilers = append(reconcilers, statefulSetReconciler)

	return reconcilers, nil
}

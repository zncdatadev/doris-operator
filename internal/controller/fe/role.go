package fe

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
)

// FEReconciler is the reconciler for FE component and implements ComponentReconciler interface
type FEReconciler struct {
	*common.BaseDorisRoleReconciler
	client *client.Client
}

// NewFEReconciler creates a new FE reconciler
func NewFEReconciler(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *dorisv1alpha1.RoleSpec,
	image *opgoutil.Image,
	dorisCluster *dorisv1alpha1.DorisCluster,
) *FEReconciler {
	feReconciler := &FEReconciler{
		client: client,
	}

	// Create base role reconciler with FE as component type
	baseReconciler := common.NewBaseDorisRoleReconciler(
		client,
		roleInfo,
		spec,
		dorisCluster,
		image,
		string(constants.ComponentTypeFE),
		feReconciler, // Pass itself as the componentRec
	)

	feReconciler.BaseDorisRoleReconciler = baseReconciler
	return feReconciler
}

// RegisterResourceWithRoleGroup implements DorisComponentReconciler interface
func (r *FEReconciler) RegisterResourceWithRoleGroup(
	ctx context.Context,
	replicas *int32,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	config *dorisv1alpha1.ConfigSpec,
) ([]reconciler.Reconciler, error) {
	// Use common resource registration logic
	reconcilers, err := common.RegisterStandardResources(
		ctx,
		r.client,
		r, // FEReconciler implements DorisComponentResourceBuilder interface
		replicas,
		r.Image,
		r.DorisCluster,
		r.ClusterOperation,
		roleGroupInfo,
		config,
		overrides,
	)
	if err != nil {
		return nil, err
	}

	// Add FE-specific resources here, such as Ingress, etc.
	// ...

	return reconcilers, nil
}

// CreateServiceReconcilers implements DorisComponentResourceBuilder interface
func (r *FEReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	var reconcilers []reconciler.Reconciler

	// Create internal service
	internalSvc := NewFEInternalServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, internalSvc)

	// Create access service
	accessSvc := NewFEAccessServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, accessSvc)

	return reconcilers
}

// CreateStatefulSetReconciler implements DorisComponentResourceBuilder interface
func (r *FEReconciler) CreateStatefulSetReconciler(
	ctx context.Context,
	client *client.Client,
	image *opgoutil.Image,
	replicas *int32,
	dorisCluster *dorisv1alpha1.DorisCluster,
	clusterOperation *commonsv1alpha1.ClusterOperationSpec,
	roleGroupInfo *reconciler.RoleGroupInfo,
	config *dorisv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
) (reconciler.Reconciler, error) {
	return NewFeStatefulSetReconciler(
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
}

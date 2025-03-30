package be

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

// BEReconciler is the reconciler for BE component and implements ComponentReconciler interface
type BEReconciler struct {
	*common.BaseDorisRoleReconciler
	client *client.Client
}

// NewBEReconciler creates a new BE reconciler
func NewBEReconciler(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *dorisv1alpha1.RoleSpec,
	image *opgoutil.Image,
	dorisCluster *dorisv1alpha1.DorisCluster,
) *BEReconciler {
	beReconciler := &BEReconciler{
		client: client,
	}

	// Create base role reconciler with BE as component type
	baseReconciler := common.NewBaseDorisRoleReconciler(
		client,
		roleInfo,
		spec,
		dorisCluster,
		image,
		string(constants.ComponentTypeBE),
		beReconciler, // Pass itself as the componentRec
	)

	beReconciler.BaseDorisRoleReconciler = baseReconciler
	return beReconciler
}

// RegisterResourceWithRoleGroup implements DorisComponentReconciler interface
func (r *BEReconciler) RegisterResourceWithRoleGroup(
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
		r, // BEReconciler implements DorisComponentResourceBuilder interface
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

	// Add BE-specific resources here, such as PDB, etc.
	// ...

	return reconcilers, nil
}

// CreateServiceReconcilers implements DorisComponentResourceBuilder interface
func (r *BEReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	var reconcilers []reconciler.Reconciler

	// Create internal service
	internalSvc := NewBEInternalServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, internalSvc)

	// Create access service
	accessSvc := NewBEAccessServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, accessSvc)

	return reconcilers
}

// CreateStatefulSetReconciler implements DorisComponentResourceBuilder interface
func (r *BEReconciler) CreateStatefulSetReconciler(
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
	return NewBeStatefulSetReconciler(
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

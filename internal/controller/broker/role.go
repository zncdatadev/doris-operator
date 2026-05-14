package broker

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

// BrokerReconciler is the reconciler for Broker component and implements DorisComponentReconciler interface
type BrokerReconciler struct {
	*common.BaseDorisRoleReconciler
	client *client.Client
}

// NewBrokerReconciler creates a new Broker reconciler
func NewBrokerReconciler(
	client *client.Client,
	roleInfo reconciler.RoleInfo,
	spec *dorisv1alpha1.RoleSpec,
	image *opgoutil.Image,
	dorisCluster *dorisv1alpha1.DorisCluster,
) *BrokerReconciler {
	brokerReconciler := &BrokerReconciler{
		client: client,
	}

	// Create base role reconciler with Broker as component type
	baseReconciler := common.NewBaseDorisRoleReconciler(
		client,
		roleInfo,
		spec,
		dorisCluster,
		image,
		string(constants.ComponentTypeBroker),
		brokerReconciler,
	)

	brokerReconciler.BaseDorisRoleReconciler = baseReconciler
	return brokerReconciler
}

// RegisterResourceWithRoleGroup implements DorisComponentReconciler interface
func (r *BrokerReconciler) RegisterResourceWithRoleGroup(
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
		r,
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

	// Create Broker configmap reconciler
	var roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	if config != nil {
		roleGroupConfig = config.RoleGroupConfigSpec
	}
	configMapRec := NewBrokerConfigMapReconciler(
		ctx,
		r.client,
		roleGroupInfo,
		overrides,
		roleGroupConfig,
		r.DorisCluster,
	)
	reconcilers = append(reconcilers, configMapRec)

	return reconcilers, nil
}

// CreateServiceReconcilers implements DorisComponentResourceBuilder interface
func (r *BrokerReconciler) CreateServiceReconcilers(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
) []reconciler.Reconciler {
	reconcilers := make([]reconciler.Reconciler, 0, 2)

	// Create internal service
	internalSvc := NewBrokerInternalServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, internalSvc)

	// Create access service
	accessSvc := NewBrokerAccessServiceReconciler(client, roleGroupInfo)
	reconcilers = append(reconcilers, accessSvc)

	return reconcilers
}

// CreateStatefulSetReconciler implements DorisComponentResourceBuilder interface
func (r *BrokerReconciler) CreateStatefulSetReconciler(
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
	return NewBrokerStatefulSetReconciler(
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

package controller

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/be"
	"github.com/zncdatadev/doris-operator/internal/controller/broker"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/fe"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	resourceClient "github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	ctrl "sigs.k8s.io/controller-runtime"
)

var clusterLogger = ctrl.Log.WithName("cluster-reconciler")
var _ reconciler.Reconciler = &Reconciler{}

// Reconciler is the main reconciler for DorisCluster resources
type Reconciler struct {
	reconciler.BaseCluster[*dorisv1alpha1.DorisClusterSpec]
	ClusterConfig    *dorisv1alpha1.ClusterConfigSpec
	ClusterOperation *commonsv1alpha1.ClusterOperationSpec
}

// NewClusterReconciler creates a new cluster reconciler for DorisCluster resources
func NewClusterReconciler(
	client *resourceClient.Client,
	clusterInfo reconciler.ClusterInfo,
	spec *dorisv1alpha1.DorisClusterSpec,
) *Reconciler {
	return &Reconciler{
		BaseCluster: *reconciler.NewBaseCluster(
			client,
			clusterInfo,
			spec.ClusterOperationSpec,
			spec,
		),
		ClusterConfig:    spec.ClusterConfig,
		ClusterOperation: spec.ClusterOperationSpec,
	}
}

// GetImage returns the image configuration for Doris components.
func (r *Reconciler) GetImage(roleType constants.ComponentType) *util.Image {
	return common.GetImage(r.Spec.Image, roleType)
}

// RegisterResources registers all resources for the DorisCluster
func (r *Reconciler) RegisterResources(ctx context.Context) error {
	// Optional: Create service account for the cluster if needed
	// sa := createServiceAccount(r.Client, r.GetName())
	// if sa != nil {
	//     r.AddResource(sa)
	// }

	// FE role
	if r.Spec.Frontend != nil {
		feRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "fe",
		}

		// Create FE reconciler with base image
		feImage := r.GetImage(constants.ComponentTypeFE)
		feReconciler := fe.NewFEReconciler(
			r.Client,
			feRoleInfo,
			r.Spec.Frontend,
			feImage,
			&dorisv1alpha1.DorisCluster{
				Spec: *r.Spec,
			},
		)

		if err := feReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(feReconciler)
		clusterLogger.Info("Registered FE role")
	}

	// BE role
	if r.Spec.Backend != nil {
		beRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "be",
		}

		// Create BE reconciler with base image
		beImage := r.GetImage(constants.ComponentTypeBE)
		beReconciler := be.NewBEReconciler(
			r.Client,
			beRoleInfo,
			r.Spec.Backend,
			beImage,
			&dorisv1alpha1.DorisCluster{
				Spec: *r.Spec,
			},
		)

		if err := beReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(beReconciler)
		clusterLogger.Info("Registered BE role")
	}

	// Broker role
	if r.Spec.Broker != nil {
		brokerRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "broker",
		}

		// Create Broker reconciler with base image
		brokerImage := r.GetImage(constants.ComponentTypeBroker)
		brokerReconciler := broker.NewBrokerReconciler(
			r.Client,
			brokerRoleInfo,
			r.Spec.Broker,
			brokerImage,
			&dorisv1alpha1.DorisCluster{
				Spec: *r.Spec,
			},
		)

		if err := brokerReconciler.RegisterResources(ctx); err != nil {
			return err
		}
		r.AddResource(brokerReconciler)
		clusterLogger.Info("Registered Broker role")
	}
	return nil
}

package controller

import (
	"context"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/be"
	"github.com/zncdatadev/doris-operator/internal/controller/common"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/fe"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	resourceClient "github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
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

// GetImage returns the image configuration for Doris components
func (r *Reconciler) GetImage(roleType constants.ComponentType) *util.Image {
	image := &util.Image{
		Custom:          common.GetComponentImage(r.Spec.Image, roleType),
		Repo:            dorisv1alpha1.DefaultRepository,
		ProductName:     dorisv1alpha1.DefaultProductName,
		KubedoopVersion: dorisv1alpha1.DefaultKubedoopVersion,
		ProductVersion:  dorisv1alpha1.DefaultProductVersion,
		PullPolicy:      corev1.PullIfNotPresent,
	}

	if r.Spec.Image != nil {
		image.Custom = r.Spec.Image.Custom
		image.Repo = r.Spec.Image.Repo
		image.KubedoopVersion = r.Spec.Image.KubedoopVersion
		image.ProductVersion = r.Spec.Image.ProductVersion
		if r.Spec.Image.PullPolicy != nil {
			image.PullPolicy = *r.Spec.Image.PullPolicy
		}
		image.PullSecretName = r.Spec.Image.PullSecretName
	}
	return image
}

// RegisterResources registers all resources for the DorisCluster
func (r *Reconciler) RegisterResources(ctx context.Context) error {
	// Optional: Create service account for the cluster if needed
	// sa := createServiceAccount(r.Client, r.GetName())
	// if sa != nil {
	//     r.AddResource(sa)
	// }

	// FE role
	if r.Spec.FrontEnd != nil {
		feRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "fe",
		}

		// Create FE reconciler with base image
		feImage := r.GetImage(constants.ComponentTypeFE)
		feReconciler := fe.NewFEReconciler(
			r.Client,
			feRoleInfo,
			r.Spec.FrontEnd,
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
	if r.Spec.BackEnd != nil {
		beRoleInfo := reconciler.RoleInfo{
			ClusterInfo: r.ClusterInfo,
			RoleName:    "be",
		}

		// Create BE reconciler with base image
		beImage := r.GetImage(constants.ComponentTypeBE)
		beReconciler := be.NewBEReconciler(
			r.Client,
			beRoleInfo,
			r.Spec.BackEnd,
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

	// Optional: Register cluster-level ingress if configured
	if r.ClusterConfig != nil && r.ClusterConfig.IngressHost != "" {
		ingress := createIngressResource(r.Client, r.ClusterInfo, r.ClusterConfig.IngressHost)
		if ingress != nil {
			r.AddResource(ingress)
			clusterLogger.Info("Registered Ingress resource")
		}
	}

	return nil
}

// Helper function to create ingress resource (placeholder)
func createIngressResource(client *resourceClient.Client, clusterInfo reconciler.ClusterInfo, host string) reconciler.Reconciler {
	// Implementation for creating ingress goes here
	// For now, return nil as this is just a placeholder
	return nil
}

// Helper function to create service account (placeholder)
func createServiceAccount(client *resourceClient.Client, name string) reconciler.Reconciler {
	// Implementation for creating service account goes here
	// For now, return nil as this is just a placeholder
	return nil
}

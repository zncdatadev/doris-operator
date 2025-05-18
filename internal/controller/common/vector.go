package common

import (
	"context"

	"emperror.dev/errors"
	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/util"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var vectorLogger = ctrl.Log.WithName("vector")

const ContainerVector = "vector"

func IsVectorEnable(roleLoggingConfig *commonsv1alpha1.LoggingSpec) bool {
	if roleLoggingConfig != nil {
		return *roleLoggingConfig.EnableVectorAgent
	}
	return false
}

type VectorConfigParams struct {
	Client        client.Client
	ClusterConfig *dorisv1alpha1.ClusterConfigSpec
	Namespace     string
	InstanceName  string
	Role          string
	GroupName     string
}

func generateVectorYAML(ctx context.Context, params VectorConfigParams) (string, error) {
	aggregatorConfigMapName := params.ClusterConfig.VectorAggregatorConfigMapName
	if aggregatorConfigMapName == nil {
		return "", errors.New("vectorAggregatorConfigMapName is not set")
	}
	return productlogging.MakeVectorYaml(ctx, params.Client, params.Namespace, params.InstanceName, params.Role,
		params.GroupName, *aggregatorConfigMapName)
}

func ExtendConfigMapByVector(ctx context.Context, params VectorConfigParams, data map[string]string) {
	if data == nil {
		data = map[string]string{}
	}
	vectorYaml, err := generateVectorYAML(ctx, params)
	if err != nil {
		vectorLogger.Error(errors.Wrap(err, "error creating vector YAML"), "failed to create vector YAML")
	} else {
		data[builder.VectorConfigFileName] = vectorYaml
	}
}

// GetVectorFactory returns a new vector factory
// can provide vector container, volumes
func GetVectorFactory(
	image *util.Image,
) *builder.Vector {
	return builder.NewVector(
		constants.ConfigVolumeName,
		constants.LogVolumeName,
		image,
	)
}

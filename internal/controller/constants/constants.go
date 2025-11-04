package constants

// Component types
type ComponentType string

const (
	// Component type enum values
	ComponentTypeFE ComponentType = "fe"
	ComponentTypeBE ComponentType = "be"
)

// config file names
const (
	// Configuration file names
	FEConfigFilename       = "fe.conf"
	BEConfigFilename       = "be.conf"
	FELog4j2ConfigFilename = "log4j2.properties"
	LDAPConfigFilename     = "ldap.conf"
)

const FELogFileName = "fe.log4j2.xml"

// General constants
const (
	// Common values
	DefaultDorisVersion = "2.1.8"
	PodinfoVolumeName   = "podinfo"
	DefaultElectNumber  = "3"
)

// Service related constants
const (
	// Service naming patterns
	ServiceInternalSuffix = "-internal"
	ServiceAccessSuffix   = "-service"
)

// Image related constants
const (
	// Image repositories
	DorisRepository     = "apache/doris"
	InitImageRepository = "selectdb/alpine"
	DefaultInitImageTag = "latest"

	// Default image references
	DefaultFEImage   = DorisRepository + ":" + string(ComponentTypeFE) + "-" + DefaultDorisVersion
	DefaultBEImage   = DorisRepository + ":" + string(ComponentTypeBE) + "-" + DefaultDorisVersion
	DefaultInitImage = InitImageRepository + ":" + DefaultInitImageTag

	// Image format templates
	FEImageFormat = "%s:" + string(ComponentTypeFE) + "-%s"
	BEImageFormat = "%s:" + string(ComponentTypeBE) + "-%s"
)

// Container names
const (
	FEContainerName   = string(ComponentTypeFE)
	BEContainerName   = string(ComponentTypeBE)
	InitContainerName = "default-init"
)

// Path related constants
const (
	// Base paths
	BaseDorisPath = "/opt/apache-doris"

	// Container paths
	FEEntrypoint   = BaseDorisPath + "/" + string(ComponentTypeFE) + "_entrypoint.sh"
	BEEntrypoint   = BaseDorisPath + "/" + string(ComponentTypeBE) + "_entrypoint.sh"
	FEMetadataPath = BaseDorisPath + "/" + string(ComponentTypeFE) + "/doris-meta"
	BEStoragePath  = BaseDorisPath + "/" + string(ComponentTypeBE) + "/storage"

	// Config paths
	DefaultConfigMapPath = "/etc/doris/conf"
	PodinfoMountPath     = "/etc/podinfo"
)

// Network related constants
const (
	// FE ports
	FEHttpPort       = 8030
	FERpcPort        = 9020
	FEQueryPort      = 9030
	FEEditLogPort    = 9010
	DefaultQueryPort = "9030"

	// BE ports
	BERpcPort       = 9060
	BEHttpPort      = 8040
	BEHeartbeatPort = 9050
	BEBrpcPort      = 8060
)

// Port names
const (
	// FE port names
	FEHttpPortName    = string(ComponentTypeFE) + "-http"
	FERpcPortName     = string(ComponentTypeFE) + "-rpc"
	FEQueryPortName   = string(ComponentTypeFE) + "-query"
	FEEditLogPortName = string(ComponentTypeFE) + "-edit-log"

	// BE port names
	BERpcPortName       = string(ComponentTypeBE) + "-rpc"
	BEHttpPortName      = string(ComponentTypeBE) + "-http"
	BEHeartbeatPortName = string(ComponentTypeBE) + "-heartbeat"
	BEBrpcPortName      = string(ComponentTypeBE) + "-brpc"
)

// Volume related constants
const (
	// Volume names
	FEMetadataVolume = string(ComponentTypeFE) + "-meta"
	BEStorageVolume  = string(ComponentTypeBE) + "-storage"
	PodinfoVolume    = "podinfo"

	// ConfigVolumeName is the name of the configmap volume
	ConfigVolumeName = "doris-config"

	LogVolumeName = "log"
)

// Resource related constants
const (
	// Default CPU resources
	DefaultCPULimit   = "2"
	DefaultCPURequest = "2"

	// Memory limits
	FEMemoryLimit = "1Gi"
	BEMemoryLimit = "2Gi"

	// Storage sizes
	FEStorageSize = "10Gi"
	BEStorageSize = "20Gi"
)

// Health check related constants
const (
	HealthCheckPath            = "/api/health"
	DefaultInitialDelaySeconds = 30
	DefaultPeriodSeconds       = 10
)

// Environment variable related constants
const (
	// Environment variable names
	PodNameEnvVar       = "POD_NAME"
	PodIPEnvVar         = "POD_IP"
	HostIPEnvVar        = "HOST_IP"
	PodNamespaceEnvVar  = "POD_NAMESPACE"
	ConfigMapPathEnvVar = "CONFIGMAP_MOUNT_PATH"
	UserEnvVar          = "USER"
	DorisRootEnvVar     = "DORIS_ROOT"
	FEAddrEnvVar        = "ENV_FE_ADDR"
	FEQueryPortEnvVar   = "FE_QUERY_PORT"
	FEElectNumberEnvVar = "ELECT_NUMBER"

	// Default environment variable values
	DefaultUser      = "root"
	DefaultDorisRoot = BaseDorisPath
)

// Command related constants
const (
	// Init container command for BE
	BEInitCommand = "sysctl -w vm.max_map_count=2000000 && swapoff -a || true"
)

// Labels and annotations
const (
	OwnerReferenceLabelKey = "app.doris.ownerreference/name"
	ServiceRoleLabelKey    = "app.doris.service/role"
	ComponentLabelKey      = "app.kubernetes.io/component"
	HashAnnotationKey      = "app.doris.components/hash"
)

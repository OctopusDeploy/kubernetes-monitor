//nolint:golines // Golines because of the long tags on the structs causing issues with line parsing and validation with this linter
package config

import (
	"fmt"
	"time"

	"go.uber.org/multierr"
)

const (
	DefaultConfigDirectory     = "configuration"
	DefaultConfigFileName      = "config.json"
	DefaultSecretStoreFileName = "secret.json"

	ConfigurationStoreTypeFile       = "file"
	ConfigurationStoreTypeKubernetes = "kubernetes"

	// DefaultMaxGrpcSendMessageSizeBytes bounds the compressed size of messages sent to Octopus
	// Server. grpc-go applies this limit to the compressed payload.
	// Since Octopus Server has its own limits, we don't need to make this too low - just needs to be a finite number.
	DefaultMaxGrpcSendMessageSizeBytes = 50 * 1024 * 1024 // 52428800

	// DefaultMaxGrpcReceiveMessageSizeBytes bounds messages received from Octopus Server. grpc-go
	// applies this limit to the decompressed payload, so compression on the server side doesn't help this.
	// The list of desired resources is typically a lot smaller than all resources (Pods etc.), so 20MB is plenty.
	DefaultMaxGrpcReceiveMessageSizeBytes = 20 * 1024 * 1024 // 20971520

	// DefaultHealthCheckInterval and DefaultHealthCheckGiveUpAfter mirror
	// connection.DefaultHealthCheckInterval and DefaultHealthCheckGiveUpAfter. They
	// are declared here so the flag defaults and the tests asserting them read from
	// one place rather than three.
	DefaultHealthCheckInterval    = 30 * time.Second
	DefaultHealthCheckGiveUpAfter = 5 * time.Minute
)

// PersistentConfig holds the configuration that can be serialized and written to a file
type PersistentConfig struct {
	// Common
	ServerGrpcUrl string `mapstructure:"server-grpc-url" section:"common" usage:"Address of the gRPC server e.g. grpc://my.server.app:8443"`

	// Runtime
	PinGrpcCertificate    bool          `mapstructure:"pin-grpc-certificate" section:"run" usage:"Pin the gRPC server certificate to only trust the thumbprint obtained during registration. Will not validate against any CAs." default:"false"`
	ResourceMonitorPeriod time.Duration `mapstructure:"monitor-period"       section:"run" usage:"Send a complete snapshot of monitored resources to the server every period. Default: 5 minutes"                                default:"5m"`

	MaxGrpcSendMessageSizeBytes    int `mapstructure:"max-grpc-send-message-size-bytes"    section:"run" usage:"Largest gRPC message the monitor will send, in bytes. This limit applies to the compressed size. Default: 50MiB"     default:"52428800"`
	MaxGrpcReceiveMessageSizeBytes int `mapstructure:"max-grpc-receive-message-size-bytes" section:"run" usage:"Largest gRPC message the monitor will accept, in bytes. This limit applies to the decompressed size. Default: 20MiB" default:"20971520"`

	DisableGrpcCompression bool `mapstructure:"disable-grpc-compression" section:"run" usage:"Send gRPC messages uncompressed. Only use for troubleshooting." default:"false"`

	HealthCheckInterval    time.Duration `mapstructure:"health-check-interval"      section:"run" usage:"Send a gRPC health check to Octopus Server at this interval. Set to 0 to disable, which should only be for troubleshooting (error-recovery is limited when health checks are disabled). Default: 30 seconds" default:"30s"`
	HealthCheckGiveUpAfter time.Duration `mapstructure:"health-check-give-up-after" section:"run" usage:"Exit once Octopus Server has gone unanswered for this long, so the pod is restarted. Default: 5 minutes"                                                                                                     default:"5m"`
}

// SecretConfig holds the configuration that should be serialized and written to a secret location
type SecretConfig struct {
	InstallationId      string `mapstructure:"installation-id"      section:"common" usage:"ID used to uniquely identify this installation"`
	ServerThumbprint    string `mapstructure:"server-thumbprint"    section:"run"    usage:"Thumbprint of the gRPC server certificate"`
	AuthenticationToken string `mapstructure:"authentication-token" section:"common" usage:"Authentication token used for gRPC requests"`
}

// TransientConfig holds the configuration that is not serialized and is used for runtime operations
type TransientConfig struct {
	// Common
	ConfigPath               string `mapstructure:"config-path"                section:"common" usage:"Directory to store configuration files"`
	ConfigurationStoreType   string `mapstructure:"config-store"               section:"common" usage:"Method of storing authentication secrets.\n  - file\n  - kubernetes\n Default: file"    default:"file"`
	KubernetesStorePrefix    string `mapstructure:"kubernetes-store-prefix"    section:"common" usage:"Prefix for the Kubernetes store objects when using kubernetes store"`
	KubernetesStoreNamespace string `mapstructure:"kubernetes-store-namespace" section:"common" usage:"Namespace for the Kubernetes store objects when using kubernetes store"`
	CaCertificatePath        string `mapstructure:"ca-certificate-path"        section:"common" usage:"Absolute path to the CA certificate file used to verify the server's gRPC certificate."`

	// Registration
	ServerApiUrl      string `mapstructure:"server-api-url"      section:"register" usage:"Address of the server API eg. https://my.server.app"`
	ServerAccessToken string `mapstructure:"server-access-token" section:"register" usage:"Access token for the server API, used to register the monitor with the server"`
	MachineName       string `mapstructure:"machine-name"        section:"register" usage:"Name of the Kubernetes agent that the monitor is tied to"`
	SpaceId           string `mapstructure:"space-id"            section:"register" usage:"ID of the space where the agent will be registered with the server"`

	// Runtime
	UseKubeconfig          bool     `mapstructure:"use-kubeconfig"           section:"common" usage:"Use kubectl config client instead of in-cluster client"`
	KubeconfigPath         string   `mapstructure:"kubeconfig-path"          section:"common" usage:"Absolute path to the kubeconfig file"`
	TargetNamespaces       []string `mapstructure:"target-namespaces"        section:"run"    usage:"Comma-separated list of namespaces to monitor. When set, the monitor only watches these namespaces instead of cluster-wide."`
	ClusterScopedResources bool     `mapstructure:"cluster-scoped-resources" section:"run"    usage:"Include cluster-scoped resources (Nodes, Namespaces, PVs, etc.) in monitoring. Only effective when target-namespaces is set." default:"false"`

	// Debugging
	DebugEnabled   bool   `mapstructure:"debug"         section:"common" usage:"Enables debug mode, including logging and continuous profiling"`
	TracingEnabled bool   `mapstructure:"tracing"       section:"common" usage:"Enable tracing for the monitor. This will enable the default HTTP tracer to http://localhost:4318. You can set the OTEL_EXPORTER_OTLP_ENDPOINT environment variable to control the endpoint used."`
	PrintTraces    bool   `mapstructure:"print-traces"  section:"common" usage:"Enable stdout trace printing for the monitor. This requires tracing to be enabled. This will stop the monitor from sending traces via HTTP."`
	PyroscopeUrl   string `mapstructure:"pyroscope-url" section:"common" usage:"URL of the Pyroscope server to send profiling data to"`
}

// Config holds the configuration for the application including non-serializable components
type Config struct {
	Store            Store
	PersistentConfig `      mapstructure:",squash"`
	SecretConfig     `      mapstructure:",squash"`
	TransientConfig  `      mapstructure:",squash"`
}

// MonitorIsRegistered checks whether the monitor has been properly registered
func (c *Config) MonitorIsRegistered() error {
	var err error
	if c.InstallationId == "" {
		err = multierr.Append(err, fmt.Errorf("installation-id is required"))
	}

	if c.AuthenticationToken == "" {
		err = multierr.Append(err, fmt.Errorf("authentication-token is required"))
	}

	return err
}

type FailedToReadConfigError struct {
	err error
}

func (e FailedToReadConfigError) Error() string {
	return fmt.Sprintf("Failed to read configuration: %v", e.err)
}

type FailedToBindError struct {
	err error
}

func (e FailedToBindError) Error() string {
	return fmt.Sprintf("Failed to bind configuration: %v", e.err)
}

type FailedToCreateStoreError struct {
	err error
}

func (e FailedToCreateStoreError) Error() string {
	return fmt.Sprintf("Failed to create configuration store: %v", e.err)
}

package config

import (
	"os"
	"testing"
	"time"

	"dario.cat/mergo"
	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	"go.uber.org/multierr"
)

func setupFlags() *pflag.FlagSet {
	testFlagSet := pflag.NewFlagSet("test", pflag.ExitOnError)
	InitCommonFlags(testFlagSet)
	return testFlagSet
}

// Sets environment variables and returns a cleanup function to reset them
func envSetter(envs map[string]string) (closer func()) {
	originalEnvs := map[string]string{}

	for name, value := range envs {
		if originalValue, ok := os.LookupEnv(name); ok {
			originalEnvs[name] = originalValue
		}
		_ = os.Setenv(name, value)
	}

	return func() {
		for name := range envs {
			origValue, has := originalEnvs[name]
			if has {
				_ = os.Setenv(name, origValue)
			} else {
				_ = os.Unsetenv(name)
			}
		}
	}
}

// compareConfig creates a cmp.Option that ignores the Store field when comparing Config structs
func compareConfig() cmp.Option {
	return cmp.FilterPath(func(p cmp.Path) bool {
		return p.String() == "Store"
	}, cmp.Ignore())
}

var ExpectedDefaultRegisterConfig = Config{
	PersistentConfig: PersistentConfig{
		ResourceMonitorPeriod:          5 * time.Minute,
		MaxGrpcSendMessageSizeBytes:    DefaultMaxGrpcSendMessageSizeBytes,
		MaxGrpcReceiveMessageSizeBytes: DefaultMaxGrpcReceiveMessageSizeBytes,
	},
	TransientConfig: TransientConfig{
		ConfigurationStoreType: ConfigurationStoreTypeFile,
		TargetNamespaces:       []string{},
	},
}

var ExpectedDefaultRunConfig = Config{
	PersistentConfig: PersistentConfig{
		ResourceMonitorPeriod:          5 * time.Minute,
		MaxGrpcSendMessageSizeBytes:    DefaultMaxGrpcSendMessageSizeBytes,
		MaxGrpcReceiveMessageSizeBytes: DefaultMaxGrpcReceiveMessageSizeBytes,
		HealthCheckInterval:            DefaultHealthCheckInterval,
		HealthCheckGiveUpAfter:         DefaultHealthCheckGiveUpAfter,
	},
	TransientConfig: TransientConfig{
		ConfigurationStoreType: ConfigurationStoreTypeFile,
		TargetNamespaces:       []string{},
	},
}

// The chart sets these as environment variables, so a rename that does not reach
// the mapstructure tag leaves the value silently on its default.
func TestGetConfig_ReadsHealthCheckSettingsFromTheEnvironment(t *testing.T) {
	closer := envSetter(map[string]string{
		"HEALTH_CHECK_INTERVAL":      "45s",
		"HEALTH_CHECK_GIVE_UP_AFTER": "3m",
	})
	defer closer()

	flagSet := setupFlags()
	InitRunFlags(flagSet)

	config, _ := GetConfig(flagSet, "")

	if config.HealthCheckInterval != 45*time.Second {
		t.Errorf("Expected HealthCheckInterval to be 45s, got %v", config.HealthCheckInterval)
	}

	if config.HealthCheckGiveUpAfter != 3*time.Minute {
		t.Errorf("Expected HealthCheckGiveUpAfter to be 3m, got %v", config.HealthCheckGiveUpAfter)
	}
}

func TestGetConfig_Returns_SpecificErrors_WhenNoValuesAreSet(t *testing.T) {
	_, err := GetConfig(setupFlags(), "")
	for _, e := range multierr.Errors(err) {
		if _, ok := e.(FailedToReadConfigError); ok {
			continue
		}
		t.Errorf("Unexpected error type: %T", e)
	}
}

func TestGetConfig_ReturnsDefaultValues_WhenNoValuesAreSet_ForRuntime(t *testing.T) {
	// Set up a runtime flag set
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	// ignore the error here since we expect it to be a FailedToReadConfigError
	config, _ := GetConfig(flagSet, "")

	if diff := cmp.Diff(ExpectedDefaultRunConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_Returns_CombinedConfig_ForRegistrationCommand_WhenCommandLineFlagsAreSet(t *testing.T) {
	var err error
	flagSet := setupFlags()
	RegisterFlagSetter{flagSet}.InitFlags()

	err = multierr.Append(err, flagSet.Set("space-id", "Spaces-100"))
	err = multierr.Append(err, flagSet.Set("server-api-url", "https://octopus.com"))
	err = multierr.Append(err, flagSet.Set("server-access-token", "my-secret-token"))
	err = multierr.Append(err, flagSet.Set("machine-name", "my-machine-name"))
	if err != nil {
		t.Error(err)
	}

	expectedConfig := Config{
		TransientConfig: TransientConfig{
			SpaceId:           "Spaces-100",
			ServerApiUrl:      "https://octopus.com",
			ServerAccessToken: "my-secret-token",
			MachineName:       "my-machine-name",
			TargetNamespaces:  []string{},
		},
	}

	err = mergo.Merge(&expectedConfig, ExpectedDefaultRegisterConfig)
	if err != nil {
		t.Errorf("Error merging config: %v", err)
	}

	// ignore the error here since we expect it to be a FailedToReadConfigError
	config, _ := GetConfig(flagSet, "")

	if diff := cmp.Diff(expectedConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_Returns_CombinedConfig_ForRunCommand_WhenCommandLineFlagsAreSet(t *testing.T) {
	var err error
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	err = multierr.Append(err, flagSet.Set("server-grpc-url", "grpc://octopus.server.app:8443"))
	err = multierr.Append(err, flagSet.Set("server-thumbprint", "THISISATHUMBPRINT"))
	err = multierr.Append(err, flagSet.Set("monitor-period", "5m"))
	if err != nil {
		t.Error(err)
	}

	expectedConfig := Config{
		PersistentConfig: PersistentConfig{
			ServerGrpcUrl:         "octopus.server.app:8443",
			ResourceMonitorPeriod: time.Duration(300) * time.Second,
		},
		SecretConfig: SecretConfig{
			ServerThumbprint: "THISISATHUMBPRINT",
		},
		TransientConfig: TransientConfig{
			TargetNamespaces: []string{},
		},
	}

	err = mergo.Merge(&expectedConfig, ExpectedDefaultRunConfig)
	if err != nil {
		t.Errorf("Error merging config: %v", err)
	}

	// ignore the error here since we expect it to be a FailedToReadConfigError
	config, _ := GetConfig(flagSet, "")

	if diff := cmp.Diff(expectedConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_Returns_CombinedConfig_ForRegistration_WhenEnvironmentVariablesAreSet(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"SPACE_ID":            "Spaces-200",
		"SERVER_API_URL":      "https://octopus-env-123.com",
		"SERVER_ACCESS_TOKEN": "secret-token",
		"MACHINE_NAME":        "my-machine-name",
	}))
	flagSet := setupFlags()
	RegisterFlagSetter{flagSet}.InitFlags()

	expectedConfig := Config{
		TransientConfig: TransientConfig{
			ServerApiUrl:      "https://octopus-env-123.com",
			ServerAccessToken: "secret-token",
			MachineName:       "my-machine-name",
			SpaceId:           "Spaces-200",
			TargetNamespaces:  []string{},
		},
	}
	err := mergo.Merge(&expectedConfig, ExpectedDefaultRegisterConfig)
	if err != nil {
		t.Errorf("Error merging config: %v", err)
	}

	config, _ := GetConfig(flagSet, "")

	if diff := cmp.Diff(expectedConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_Returns_CombinedConfig_ForRun_WhenEnvironmentVariablesAreSet(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"SERVER_GRPC_URL":      "grpc://octopus-env-123.com:8443",
		"SERVER_THUMBPRINT":    "THUMBPRINT-123",
		"INSTALLATION_ID":      "INSTALLATION-123",
		"AUTHENTICATION_TOKEN": "AUTH-TOKEN-123",
		"USE_KUBECONFIG":       "true",
		"KUBECONFIG_PATH":      "/path/to/kubeconfig",
	}))
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	expectedConfig := Config{
		PersistentConfig: PersistentConfig{
			ServerGrpcUrl: "octopus-env-123.com:8443",
		},
		SecretConfig: SecretConfig{
			InstallationId:      "INSTALLATION-123",
			AuthenticationToken: "AUTH-TOKEN-123",
			ServerThumbprint:    "THUMBPRINT-123",
		},
		TransientConfig: TransientConfig{
			UseKubeconfig:    true,
			KubeconfigPath:   "/path/to/kubeconfig",
			TargetNamespaces: []string{},
		},
	}
	err := mergo.Merge(&expectedConfig, ExpectedDefaultRunConfig)
	if err != nil {
		t.Errorf("Error merging config: %v", err)
	}

	config, _ := GetConfig(flagSet, "")

	if diff := cmp.Diff(expectedConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

// TestGetConfig_DefaultsGrpcMessageSizes_WhenRunFlagsAreNotRegistered covers the path where the
// values come from GetConfig's own fallback rather than the flag defaults, which is what happens
// when the configuration is read from a store that predates these settings. Without the fallback
// the sizes would be zero and every gRPC call would fail.
func TestGetConfig_DefaultsGrpcMessageSizes_WhenRunFlagsAreNotRegistered(t *testing.T) {
	config, _ := GetConfig(setupFlags(), "")

	if config.MaxGrpcSendMessageSizeBytes != DefaultMaxGrpcSendMessageSizeBytes {
		t.Errorf(
			"MaxGrpcSendMessageSizeBytes = %d, want %d",
			config.MaxGrpcSendMessageSizeBytes, DefaultMaxGrpcSendMessageSizeBytes,
		)
	}
	if config.MaxGrpcReceiveMessageSizeBytes != DefaultMaxGrpcReceiveMessageSizeBytes {
		t.Errorf(
			"MaxGrpcReceiveMessageSizeBytes = %d, want %d",
			config.MaxGrpcReceiveMessageSizeBytes, DefaultMaxGrpcReceiveMessageSizeBytes,
		)
	}
}

// TestGetConfig_DefaultsGrpcMessageSizes_MatchTheFlagDefaults pins the struct tag defaults, which
// are string literals that cannot reference the constants, against the constants themselves. If
// the two ever drift, the value a user gets would depend on whether the run flags were registered.
func TestGetConfig_DefaultsGrpcMessageSizes_MatchTheFlagDefaults(t *testing.T) {
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	config, _ := GetConfig(flagSet, "")

	if config.MaxGrpcSendMessageSizeBytes != DefaultMaxGrpcSendMessageSizeBytes {
		t.Errorf(
			"max-grpc-send-message-size-bytes flag default = %d, but DefaultMaxGrpcSendMessageSizeBytes = %d",
			config.MaxGrpcSendMessageSizeBytes, DefaultMaxGrpcSendMessageSizeBytes,
		)
	}
	if config.MaxGrpcReceiveMessageSizeBytes != DefaultMaxGrpcReceiveMessageSizeBytes {
		t.Errorf(
			"max-grpc-receive-message-size-bytes flag default = %d, but DefaultMaxGrpcReceiveMessageSizeBytes = %d",
			config.MaxGrpcReceiveMessageSizeBytes, DefaultMaxGrpcReceiveMessageSizeBytes,
		)
	}
}

// TestGetConfig_Returns_GrpcMessageSizes_WhenEnvironmentVariablesAreSet is the path the Helm chart
// uses: it renders these settings as environment variables on the monitor container.
func TestGetConfig_Returns_GrpcMessageSizes_WhenEnvironmentVariablesAreSet(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"MAX_GRPC_SEND_MESSAGE_SIZE_BYTES":    "12345678",
		"MAX_GRPC_RECEIVE_MESSAGE_SIZE_BYTES": "87654321",
	}))
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	config, _ := GetConfig(flagSet, "")

	if config.MaxGrpcSendMessageSizeBytes != 12345678 {
		t.Errorf("MaxGrpcSendMessageSizeBytes = %d, want 12345678", config.MaxGrpcSendMessageSizeBytes)
	}
	if config.MaxGrpcReceiveMessageSizeBytes != 87654321 {
		t.Errorf("MaxGrpcReceiveMessageSizeBytes = %d, want 87654321", config.MaxGrpcReceiveMessageSizeBytes)
	}
}

// TestGetConfig_LeavesGrpcCompressionEnabled_ByDefault pins the direction of the compression
// setting. It is expressed as an opt-out so that "unset" can only ever mean compression is on:
// the zero value of the field is the safe one, whether the configuration came from the flag
// default, a store written before this setting existed, or a flag set that omits the run flags.
// Compression is what keeps large cluster snapshots under the gRPC send limit, so silently
// defaulting it off would reintroduce the dropped cluster updates it was added to fix.
func TestGetConfig_LeavesGrpcCompressionEnabled_ByDefault(t *testing.T) {
	t.Run("with the run flags registered", func(t *testing.T) {
		flagSet := setupFlags()
		RunFlagSetter{flagSet}.InitFlags()

		config, _ := GetConfig(flagSet, "")

		if config.DisableGrpcCompression {
			t.Error("DisableGrpcCompression = true, want false so that compression stays on by default")
		}
	})

	t.Run("without the run flags registered", func(t *testing.T) {
		config, _ := GetConfig(setupFlags(), "")

		if config.DisableGrpcCompression {
			t.Error("DisableGrpcCompression = true, want false so that compression stays on by default")
		}
	})
}

// TestGetConfig_Returns_DisableGrpcCompression_WhenEnvironmentVariableIsSet is the path the Helm
// chart uses: it renders this setting as an environment variable on the monitor container.
func TestGetConfig_Returns_DisableGrpcCompression_WhenEnvironmentVariableIsSet(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"DISABLE_GRPC_COMPRESSION": "true",
	}))
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	config, _ := GetConfig(flagSet, "")

	if !config.DisableGrpcCompression {
		t.Error("DisableGrpcCompression = false, want true")
	}
}

func TestGetConfig_ParsesTargetNamespaces_FromCommaSeparatedEnvVar(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"TARGET_NAMESPACES": "production, staging, dev",
	}))
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	config, _ := GetConfig(flagSet, "")

	expected := []string{"production", "staging", "dev"}
	if diff := cmp.Diff(expected, config.TargetNamespaces); diff != "" {
		t.Errorf("TargetNamespaces mismatch: %s", diff)
	}
}

func TestGetConfig_FiltersEmptyTargetNamespaces_FromEnvVar(t *testing.T) {
	t.Cleanup(envSetter(map[string]string{
		"TARGET_NAMESPACES": "production,",
	}))
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()

	config, _ := GetConfig(flagSet, "")

	expected := []string{"production"}
	if diff := cmp.Diff(expected, config.TargetNamespaces); diff != "" {
		t.Errorf("TargetNamespaces mismatch: %s", diff)
	}
}

func TestGetConfig_FiltersEmptyTargetNamespaces_FromFlags(t *testing.T) {
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()
	if err := flagSet.Set("target-namespaces", "production,"); err != nil {
		t.Fatal(err)
	}

	config, _ := GetConfig(flagSet, "")

	expected := []string{"production"}
	if diff := cmp.Diff(expected, config.TargetNamespaces); diff != "" {
		t.Errorf("TargetNamespaces mismatch: %s", diff)
	}
}

func TestGetConfig_Returns_CombinedConfigForRun_WhenConfigFileExists(t *testing.T) {
	// Set up a flag set for the run command
	flagSet := setupFlags()
	RunFlagSetter{flagSet}.InitFlags()
	testDataDirectory := "../../testdata/base/"

	// Values based on testdata/base/config.json
	expectedPersistent := PersistentConfig{
		ServerGrpcUrl:         "octopus-file.com:8443",
		ResourceMonitorPeriod: 5 * time.Minute,
	}

	// Values based on testdata/base/secret.json
	expectedSecret := SecretConfig{
		InstallationId:      "89e206c4-cd87-4d0d-96a8-03fdf0e2e2e8",
		AuthenticationToken: "test-only-authentication-token",
		ServerThumbprint:    "446E1F212852B8E414077A5122A803055DC5E171",
	}

	expectedConfig := Config{
		PersistentConfig: expectedPersistent,
		SecretConfig:     expectedSecret,
		TransientConfig: TransientConfig{
			ConfigPath:       testDataDirectory,
			TargetNamespaces: []string{},
		},
	}

	err := mergo.Merge(&expectedConfig, ExpectedDefaultRunConfig)
	if err != nil {
		t.Errorf("Error merging config: %v", err)
	}

	config, _ := GetConfig(flagSet, testDataDirectory)
	if diff := cmp.Diff(expectedConfig, *config, compareConfig()); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_FollowsPrecedence_ForRegister_WhenUsingMultipleSources(t *testing.T) {
	// Set up flags
	flagSet := setupFlags()
	InitRegisterFlags(flagSet)

	// Command line arguments should take precedence over everything else
	err := flagSet.Set("space-id", "Spaces-Flag") // Shouldn't be overridden by anything
	if err != nil {
		return
	}
	err = os.Setenv("SPACE_ID", "Spaces-Env") // Should be overridden by flag
	if err != nil {
		return
	}

	// Then, environment variables should be applied
	err = os.Setenv("SERVER_API_URL", "https://octopus-env.com") // Shouldn't be overridden by anything
	if err != nil {
		return
	}

	// Get the config
	config, _ := GetConfig(flagSet, "")

	// Create the expected config based on the precedence rules
	// Note that the registration command does not use persistent config
	expectedTransientConfig := TransientConfig{
		SpaceId:                "Spaces-Flag",             // From command line flag
		ServerApiUrl:           "https://octopus-env.com", // From the environment variable
		ConfigurationStoreType: ConfigurationStoreTypeFile,
		TargetNamespaces:       []string{},
	}

	if diff := cmp.Diff(expectedTransientConfig, config.TransientConfig); diff != "" {
		t.Error(diff)
	}
}

func TestGetConfig_FollowsPrecedence_ForRun_WhenUsingMultipleSources(t *testing.T) {
	// Set up flags
	flagSet := setupFlags()
	InitRunFlags(flagSet)

	// Command line arguments should take precedence over everything else
	err := flagSet.Set("server-thumbprint", "THUMBPRINT-FLAG") // Shouldn't be overridden by anything
	if err != nil {
		return
	}
	err = os.Setenv("SERVER_THUMBPRINT", "THUMBPRINT-ENV") // Should be overridden by flag
	if err != nil {
		return
	}

	// Then, environment variables should be applied
	err = os.Setenv("SERVER_GRPC_URL", "grpc://octopus-env.com:8443") // Shouldn't be overridden by anything
	if err != nil {
		return
	}

	// Finally, the config file should be used
	// Get the config
	testdataDir := "../../testdata/precedence/"

	config, _ := GetConfig(flagSet, testdataDir)

	// Create the expected config based on the precedence rules
	expectedPersistentConfig := PersistentConfig{
		ServerGrpcUrl:                  "octopus-env.com:8443", // grpc:// should be stripped
		ResourceMonitorPeriod:          5 * time.Minute,        // Default value
		MaxGrpcSendMessageSizeBytes:    DefaultMaxGrpcSendMessageSizeBytes,
		MaxGrpcReceiveMessageSizeBytes: DefaultMaxGrpcReceiveMessageSizeBytes,
		HealthCheckInterval:            DefaultHealthCheckInterval,
		HealthCheckGiveUpAfter:         DefaultHealthCheckGiveUpAfter,
	}

	expectedSecretConfig := SecretConfig{
		ServerThumbprint:    "THUMBPRINT-FLAG",
		InstallationId:      "FILE-INSTALLATION-ID", // From the config files
		AuthenticationToken: "FILE-AUTH-TOKEN",      // From the config files
	}

	// Test the persistent config
	if diff := cmp.Diff(expectedPersistentConfig, config.PersistentConfig); diff != "" {
		t.Error(diff)
	}

	// Test the secret config
	if diff := cmp.Diff(expectedSecretConfig, config.SecretConfig); diff != "" {
		t.Error(diff)
	}
}

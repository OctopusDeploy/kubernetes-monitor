package config

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"k8s.io/client-go/util/homedir"
)

func UseEnv(v *viper.Viper) {
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
}

func UseFlags(v *viper.Viper, flagSet *pflag.FlagSet) error {
	if flagSet != nil {
		if err := v.BindPFlags(flagSet); err != nil {
			return err
		}
	}
	return nil
}

// GetConfig constructs a configuration for the application using Viper
// It first creates an initial configuration from the provided flag set and environment variables
// Then it reads the configuration from the appropriate store based on that initial configuration
// This resolves the configuration in the following order of precedence:
// 1. Command line arguments
// 2. Environment variables
// 3. Configuration file
// 4. Default values
func GetConfig(flagSet *pflag.FlagSet, overrideConfigPath string) (*Config, error) {
	var returnErr error

	// Get initial Viper instance to determine the config details and store type
	v := viper.New()
	UseEnv(v)
	err := UseFlags(v, flagSet)
	if err != nil {
		returnErr = multierr.Append(returnErr, FailedToBindError{err})
	}
	if overrideConfigPath != "" {
		v.Set("config-path", overrideConfigPath)
	}

	store, err := NewStore(v)
	if err != nil {
		returnErr = multierr.Append(returnErr, FailedToCreateStoreError{err})
		return nil, returnErr
	}

	config, err := store.Read()
	if err != nil {
		returnErr = multierr.Append(returnErr, FailedToReadConfigError{err})
	}

	// If we failed so hard that we have no configuration, we should just bail out now
	if config == nil {
		return nil, returnErr
	}

	// Handle specific transformations
	if config.ServerGrpcUrl != "" {
		config.ServerGrpcUrl = strings.TrimPrefix(config.ServerGrpcUrl, "grpc://")
	}

	// Viper doesn't auto-split comma-separated env vars into slices,
	// so we handle TARGET_NAMESPACES manually when it comes as a string.
	if len(config.TargetNamespaces) == 0 {
		if raw := v.GetString("target-namespaces"); raw != "" {
			config.TargetNamespaces = strings.Split(raw, ",")
		}
	}

	// Trim and drop empty namespace entries regardless of how they were parsed.
	// Empty items can come from trailing commas (e.g. "ns1,") and would otherwise
	// be interpreted as all namespaces by Kubernetes client namespace scoping.
	cleanedNamespaces := make([]string, 0, len(config.TargetNamespaces))
	for _, ns := range config.TargetNamespaces {
		ns = strings.TrimSpace(ns)
		if ns != "" {
			cleanedNamespaces = append(cleanedNamespaces, ns)
		}
	}
	config.TargetNamespaces = cleanedNamespaces

	// Computed configuration values
	if config.UseKubeconfig && config.KubeconfigPath == "" {
		if home := homedir.HomeDir(); home != "" {
			config.KubeconfigPath = filepath.Join(home, ".kube", "config")
		}
	}

	// If the monitor period is not set, default it to 5 minutes
	if config.ResourceMonitorPeriod <= 0 {
		config.ResourceMonitorPeriod = 5 * time.Minute // Default to 5 minutes
	}

	// gRPC rejects a zero or negative message size limit, so fall back to the defaults when they
	// are unset.
	if config.MaxGrpcSendMessageSizeBytes <= 0 {
		config.MaxGrpcSendMessageSizeBytes = DefaultMaxGrpcSendMessageSizeBytes
	}
	if config.MaxGrpcReceiveMessageSizeBytes <= 0 {
		config.MaxGrpcReceiveMessageSizeBytes = DefaultMaxGrpcReceiveMessageSizeBytes
	}

	// Add the store to the configuration
	config.Store = store

	return config, returnErr
}

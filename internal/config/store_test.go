package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/viper"
)

// ---- FileStore ----

func TestNewFileStore_FallsBackToReadOnlyStore_WhenDirectoryCannotBeCreated(t *testing.T) {
	dir := t.TempDir()

	// A regular file where the config directory needs to go makes os.MkdirAll fail.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("failed to set up blocking file: %v", err)
	}

	v := viper.New()
	v.Set("config-path", filepath.Join(blocker, "config.json"))

	store, err := NewFileStore(v)
	if err != nil {
		t.Fatalf("NewFileStore returned an error: %v", err)
	}

	if _, ok := store.(*ReadOnlyStore); !ok {
		t.Errorf("NewFileStore() = %T, want *ReadOnlyStore fallback", store)
	}
}

func TestFileStore_WriteSecrets_RestrictsFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file permissions are not supported on Windows")
	}

	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "previously-readable"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "configuration")
			v := viper.New()
			v.Set("config-path", filepath.Join(dir, DefaultConfigFileName))
			store, err := NewFileStore(v)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, DefaultSecretStoreFileName)
			if existing {
				if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			config := &Config{SecretConfig: SecretConfig{AuthenticationToken: "test-only-token"}}
			if err := store.WriteSecrets(config); err != nil {
				t.Fatal(err)
			}
			for _, filename := range []string{dir, path} {
				info, err := os.Stat(filename)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm()&0o077 != 0 {
					t.Errorf("%s grants group/other access: %o", filename, info.Mode().Perm())
				}
			}
		})
	}
}

func TestFileStore_WriteConfig_RoundTripsAllFieldTypes(t *testing.T) {
	dir := t.TempDir()
	v := viper.New()
	v.Set("config-path", filepath.Join(dir, DefaultConfigFileName))

	store, err := NewFileStore(v)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	written := &Config{
		PersistentConfig: PersistentConfig{
			ServerGrpcUrl:                  "grpc://octopus.example.com:8443",
			PinGrpcCertificate:             true,
			ResourceMonitorPeriod:          10 * time.Minute,
			MaxGrpcSendMessageSizeBytes:    1234567890,
			MaxGrpcReceiveMessageSizeBytes: 9876543210,
			DisableGrpcCompression:         true,
		},
	}
	if err := store.WriteConfig(written); err != nil {
		t.Fatalf("WriteConfig returned an error: %v", err)
	}

	// A fresh store forces parsing what was actually persisted to disk.
	readBackViper := viper.New()
	readBackViper.Set("config-path", filepath.Join(dir, DefaultConfigFileName))
	readBackStore, err := NewFileStore(readBackViper)
	if err != nil {
		t.Fatalf("failed to create file store for read-back: %v", err)
	}

	// Read errors on the missing secret file; that doesn't affect the fields under test.
	readConfig, _ := readBackStore.Read()

	if diff := cmp.Diff(written.PersistentConfig, readConfig.PersistentConfig); diff != "" {
		t.Errorf("PersistentConfig round-trip mismatch (-written +read):\n%s", diff)
	}
}

func TestFileStore_WriteSecrets_RoundTripsFields(t *testing.T) {
	dir := t.TempDir()
	v := viper.New()
	v.Set("config-path", filepath.Join(dir, DefaultConfigFileName))

	store, err := NewFileStore(v)
	if err != nil {
		t.Fatalf("failed to create file store: %v", err)
	}

	written := &Config{
		SecretConfig: SecretConfig{
			InstallationId:      "89e206c4-cd87-4d0d-96a8-03fdf0e2e2e8",
			ServerThumbprint:    "446E1F212852B8E414077A5122A803055DC5E171",
			AuthenticationToken: "test-only-authentication-token",
		},
	}
	if err := store.WriteSecrets(written); err != nil {
		t.Fatalf("WriteSecrets returned an error: %v", err)
	}

	readBackViper := viper.New()
	readBackViper.Set("config-path", filepath.Join(dir, DefaultConfigFileName))
	readBackStore, err := NewFileStore(readBackViper)
	if err != nil {
		t.Fatalf("failed to create file store for read-back: %v", err)
	}

	// Read errors on the missing config file; that doesn't affect the fields under test.
	readConfig, _ := readBackStore.Read()

	if diff := cmp.Diff(written.SecretConfig, readConfig.SecretConfig); diff != "" {
		t.Errorf("SecretConfig round-trip mismatch (-written +read):\n%s", diff)
	}
}

// ---- NewStore dispatch ----

func TestNewStore_DefaultsToFileStore_WhenTypeNotSpecified(t *testing.T) {
	v := viper.New()
	v.Set("config-path", filepath.Join(t.TempDir(), DefaultConfigFileName))

	store, err := NewStore(v)
	if err != nil {
		t.Fatalf("NewStore returned an error: %v", err)
	}

	if _, ok := store.(*FileStore); !ok {
		t.Errorf("NewStore() = %T, want *FileStore", store)
	}
}

func TestNewStore_ReturnsKubernetesStore_WhenConfigured(t *testing.T) {
	v := viper.New()
	v.Set("config-store", ConfigurationStoreTypeKubernetes)
	v.Set("kubernetes-store-prefix", "my-agent-kubernetesmonitor")
	v.Set("kubernetes-store-namespace", "octopus")

	store, err := NewStore(v)
	if err != nil {
		t.Fatalf("NewStore returned an error: %v", err)
	}

	if _, ok := store.(*KubernetesStore); !ok {
		t.Errorf("NewStore() = %T, want *KubernetesStore", store)
	}
}

func TestNewStore_ReturnsError_ForUnknownType(t *testing.T) {
	v := viper.New()
	v.Set("config-store", "not-a-real-store-type")

	_, err := NewStore(v)
	if err == nil {
		t.Fatal("NewStore() error = nil, want an error for an unknown store type")
	}
	if !strings.Contains(err.Error(), "unknown store type") {
		t.Errorf("NewStore() error = %q, want it to mention the unknown store type", err.Error())
	}
}

// ---- KubernetesStore ----

func TestNewKubernetesStore_UsesProvidedPrefixAndNamespace(t *testing.T) {
	v := viper.New()
	v.Set("kubernetes-store-prefix", "my-agent-kubernetesmonitor")
	v.Set("kubernetes-store-namespace", "octopus")

	store, err := NewKubernetesStore(v)
	if err != nil {
		t.Fatalf("NewKubernetesStore returned an error: %v", err)
	}

	if store.namePrefix != "my-agent-kubernetesmonitor" {
		t.Errorf("namePrefix = %q, want %q", store.namePrefix, "my-agent-kubernetesmonitor")
	}
	if store.namespace != "octopus" {
		t.Errorf("namespace = %q, want %q", store.namespace, "octopus")
	}
}

func TestNewKubernetesStore_RequiresNamespace(t *testing.T) {
	v := viper.New()
	v.Set("kubernetes-store-prefix", "my-agent-kubernetesmonitor")

	_, err := NewKubernetesStore(v)
	if err == nil {
		t.Fatal("NewKubernetesStore() error = nil, want an error when namespace is not provided")
	}
	if !strings.Contains(err.Error(), "namespace") {
		t.Errorf("NewKubernetesStore() error = %q, want it to mention the missing namespace", err.Error())
	}
}

func TestNewKubernetesStore_ErrorsWhenHostnameDoesNotMatchNamingConvention(t *testing.T) {
	// Without a prefix, the store derives one from os.Hostname(), which here doesn't
	// match the expected "<prefix>-kubernetesmonitor-<suffix>" convention.
	v := viper.New()
	v.Set("kubernetes-store-namespace", "octopus")

	_, err := NewKubernetesStore(v)
	if err == nil {
		t.Fatal("NewKubernetesStore() error = nil, want an error when the hostname doesn't match the naming convention")
	}
	if !strings.Contains(err.Error(), "failed to extract name prefix from hostname") {
		t.Errorf("NewKubernetesStore() error = %q, want it to mention the hostname extraction failure", err.Error())
	}
}

func TestKubernetesStore_Read_ReturnsConfigFromDefaultViper(t *testing.T) {
	v := viper.New()
	v.Set("server-grpc-url", "grpc://octopus.example.com:8443")

	store := &KubernetesStore{defaultViper: v}

	config, err := store.Read()
	if err != nil {
		t.Fatalf("Read returned an error: %v", err)
	}
	if config.ServerGrpcUrl != "grpc://octopus.example.com:8443" {
		t.Errorf("ServerGrpcUrl = %q, want %q", config.ServerGrpcUrl, "grpc://octopus.example.com:8443")
	}
	if config.Store != store {
		t.Errorf("Store = %v, want the KubernetesStore itself", config.Store)
	}
}

func TestKubernetesStore_WriteConfig_ReturnsNotImplementedError(t *testing.T) {
	store := &KubernetesStore{}

	err := store.WriteConfig(&Config{})
	if err == nil {
		t.Fatal("WriteConfig() error = nil, want a not-implemented error")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("WriteConfig() error = %q, want it to say it's not implemented", err.Error())
	}
}

func TestKubernetesStore_WriteSecrets_ReturnsErrorWhenRestConfigIsUnavailable(t *testing.T) {
	store := &KubernetesStore{namespace: "octopus", namePrefix: "my-agent"}

	config := &Config{
		TransientConfig: TransientConfig{
			UseKubeconfig:  true,
			KubeconfigPath: filepath.Join(t.TempDir(), "does-not-exist"),
		},
	}

	err := store.WriteSecrets(config)
	if err == nil {
		t.Fatal("WriteSecrets() error = nil, want an error when the kubeconfig can't be loaded")
	}
	if !strings.Contains(err.Error(), "failed to get Kubernetes REST config") {
		t.Errorf("WriteSecrets() error = %q, want it to mention the REST config failure", err.Error())
	}
}

// ---- ReadOnlyStore ----

func TestReadOnlyStore_Read_ReturnsConfigFromDefaultViper(t *testing.T) {
	v := viper.New()
	v.Set("server-grpc-url", "grpc://octopus.example.com:8443")

	store := &ReadOnlyStore{defaultViper: v}

	config, err := store.Read()
	if err != nil {
		t.Fatalf("Read returned an error: %v", err)
	}
	if config.ServerGrpcUrl != "grpc://octopus.example.com:8443" {
		t.Errorf("ServerGrpcUrl = %q, want %q", config.ServerGrpcUrl, "grpc://octopus.example.com:8443")
	}
	if config.Store != store {
		t.Errorf("Store = %v, want the ReadOnlyStore itself", config.Store)
	}
}

func TestReadOnlyStore_WriteConfig_ReturnsNotImplementedError(t *testing.T) {
	store := &ReadOnlyStore{}

	err := store.WriteConfig(&Config{})
	if err == nil {
		t.Fatal("WriteConfig() error = nil, want a not-implemented error")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("WriteConfig() error = %q, want it to say it's not implemented", err.Error())
	}
}

func TestReadOnlyStore_WriteSecrets_ReturnsNotImplementedError(t *testing.T) {
	store := &ReadOnlyStore{}

	err := store.WriteSecrets(&Config{})
	if err == nil {
		t.Fatal("WriteSecrets() error = nil, want a not-implemented error")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("WriteSecrets() error = %q, want it to say it's not implemented", err.Error())
	}
}

package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"k8s.io/client-go/kubernetes"

	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Store defines an interface for configuration storage
type Store interface {
	// Read retrieves configuration from the store
	Read() (*Config, error)

	// WriteSecrets persists secret configuration to the store
	WriteSecrets(config *Config) error

	// WriteConfig persists persistent configuration to the store
	WriteConfig(config *Config) error
}

// FileStore implements the Store interface for file-based configuration
type FileStore struct {
	configDirectory string
	defaultViper    *viper.Viper
	configViper     *viper.Viper
	secretViper     *viper.Viper
}

// KubernetesStore implements the Store interface for Kubernetes secret-based configuration
type KubernetesStore struct {
	namespace    string
	namePrefix   string
	defaultViper *viper.Viper
}

// ReadOnlyStore is a store used as a fallback to allow read-only environment variable operation
type ReadOnlyStore struct {
	defaultViper *viper.Viper
}

// NewStore creates an appropriate configuration store based on the provided type
func NewStore(v *viper.Viper) (Store, error) {
	storeType, ok := v.Get("config-store").(string)
	if !ok || storeType == "" {
		storeType = ConfigurationStoreTypeFile // Default to file store if not specified
	}
	switch storeType {
	case ConfigurationStoreTypeFile:
		return NewFileStore(v)
	case ConfigurationStoreTypeKubernetes:
		return NewKubernetesStore(v)
	default:
		return nil, fmt.Errorf("unknown store type: %s", storeType)
	}
}

// NewFileStore creates a new file-based configuration store
func NewFileStore(v *viper.Viper) (Store, error) {
	configDirectory := DefaultConfigDirectory
	pathKey := v.Get("config-path")
	path, ok := pathKey.(string)
	if ok && path != "" {
		configDirectory = filepath.Dir(path)
	}

	// New configuration directories may contain authentication credentials.
	if err := os.MkdirAll(configDirectory, 0o700); err != nil {
		// We can't write, so we'll fall back to a read-only store
		return NewReadOnlyStore(v)
	}

	configFilePath := filepath.Join(configDirectory, DefaultConfigFileName)
	secretFilePath := filepath.Join(configDirectory, DefaultSecretStoreFileName)

	configViper := viper.New()
	configViper.SetConfigFile(configFilePath)

	secretViper := viper.New()
	secretViper.SetConfigFile(secretFilePath)
	secretViper.SetConfigPermissions(0o600)

	return &FileStore{
		configDirectory: configDirectory,
		defaultViper:    v,
		configViper:     configViper,
		secretViper:     secretViper,
	}, nil
}

// Read retrieves configuration from a file
func (s *FileStore) Read() (*Config, error) {
	// Initialize the returns
	var config Config
	var aggregatedErrors error

	// Read the main configuration file
	if err := s.configViper.ReadInConfig(); err != nil {
		aggregatedErrors = multierr.Append(aggregatedErrors, err)
	}

	// Read the secret configuration file
	if err := s.secretViper.ReadInConfig(); err != nil {
		aggregatedErrors = multierr.Append(aggregatedErrors, err)
	}

	// Get all the config settings currently loaded in to the viper instances
	configSettings := s.configViper.AllSettings()
	secretSettings := s.secretViper.AllSettings()

	// Get the field mappings for converting between field names and mapstructure tags
	mapper := getFieldMapper()

	// Convert the settings to the mapstructure tags used in the Config struct
	mappedSettings := mapper.convertToMapstructureTag(configSettings)
	// Convert the secret settings to the mapstructure tags used in the Config struct
	mappedSecretSettings := mapper.convertToMapstructureTag(secretSettings)

	mergeConfig(s.defaultViper, configSettings, &aggregatedErrors)
	mergeConfig(s.defaultViper, secretSettings, &aggregatedErrors)
	mergeConfig(s.defaultViper, mappedSettings, &aggregatedErrors)
	mergeConfig(s.defaultViper, mappedSecretSettings, &aggregatedErrors)

	// Bind the configuration to the Config struct
	if bindErr := s.defaultViper.Unmarshal(&config); bindErr != nil {
		config = Config{}
		aggregatedErrors = multierr.Append(aggregatedErrors, bindErr)
	}

	// Attach the store to the config
	config.Store = s

	return &config, aggregatedErrors
}

// WriteConfig persists configuration to a file
func (s *FileStore) WriteConfig(config *Config) error {
	// Get any persistent configuration in the config struct
	configFields := reflect.VisibleFields(reflect.TypeFor[PersistentConfig]())
	cfg := config.PersistentConfig
	reflectedValue := reflect.ValueOf(cfg)

	// Add the persistent configuration to the configViper (in case it was an env var)
	for _, f := range configFields {
		key := strings.Split(f.Tag.Get("mapstructure"), ",")[0]
		if f.Type.Kind() == reflect.Bool {
			// For boolean values, we need to ensure they are set as true/false strings
			if reflectedValue.FieldByName(f.Name).Bool() {
				s.configViper.Set(key, "true")
			} else {
				s.configViper.Set(key, "false")
			}
			continue
		}
		s.configViper.Set(key, fmt.Sprintf("%v", reflectedValue.FieldByName(f.Name)))
	}

	// Write the configViper to the config file
	if err := s.configViper.WriteConfig(); err != nil {
		// If the file doesn't exist, try creating it
		if os.IsNotExist(err) {
			return s.configViper.SafeWriteConfig()
		}
		return fmt.Errorf("failed to write config: %w", err)
	}
	return nil
}

// WriteSecrets persists configuration to a file
func (s *FileStore) WriteSecrets(config *Config) error {
	// Tighten existing files before writing: the create mode does not change their permissions.
	secretFilePath := filepath.Join(s.configDirectory, DefaultSecretStoreFileName)
	if err := os.Chmod(secretFilePath, 0o600); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to restrict secret file permissions: %w", err)
	}

	// Get any secret configuration in the config struct
	configFields := reflect.VisibleFields(reflect.TypeFor[SecretConfig]())
	cfg := config.SecretConfig
	reflectedValue := reflect.ValueOf(cfg)

	// Add the secret configuration to the secretViper (in case it was an env var)
	for _, f := range configFields {
		key := strings.Split(f.Tag.Get("mapstructure"), ",")[0]
		s.secretViper.Set(key, fmt.Sprintf("%s", reflectedValue.FieldByName(f.Name)))
	}

	// Write the secretViper to the config file
	if err := s.secretViper.WriteConfig(); err != nil {
		// If the file doesn't exist, try creating it
		if os.IsNotExist(err) {
			return s.secretViper.SafeWriteConfig()
		}
		return fmt.Errorf("failed to write config: %w", err)
	}
	return nil
}

// NewKubernetesStore creates a new Kubernetes secret-based configuration store
func NewKubernetesStore(v *viper.Viper) (*KubernetesStore, error) {
	// Name prefix must be consistent with our existing naming convention
	// The default is the Octopus machine name
	// To get this dynamically, we use the hostname of the machine

	var namePrefix string

	prefixConfig, ok := v.Get("kubernetes-store-prefix").(string)
	if ok && prefixConfig != "" {
		namePrefix = prefixConfig
	} else {
		hostname, err := os.Hostname()
		if hostname == "" || err != nil {
			// If we can't get the hostname, we need to bail out
			return nil, fmt.Errorf("failed to get hostname: %w", err)
		}
		regex, err := regexp.Compile("^(.+?-kubernetesmonitor)-.+$")
		if err != nil {
			return nil, fmt.Errorf("failed to compile regex for hostname: %w", err)
		}
		matches := regex.FindStringSubmatch(hostname)
		if len(matches) < 2 {
			return nil, fmt.Errorf("failed to extract name prefix from hostname: %s", hostname)
		}
		namePrefix = matches[1]
	}

	namespace, ok := v.Get("kubernetes-store-namespace").(string)
	if !ok || namespace == "" {
		return nil, fmt.Errorf("kubernetes store namespace must be provided")
	}

	return &KubernetesStore{
		defaultViper: v,
		namespace:    namespace,
		namePrefix:   namePrefix,
	}, nil
}

// Read is currently not implemented for KubernetesStore
// It currently just returns the default configuration's values
func (s *KubernetesStore) Read() (*Config, error) {
	// Initialize the returns
	var config Config
	var aggregatedErrors error

	// Bind the configuration to the Config struct
	if bindErr := s.defaultViper.Unmarshal(&config); bindErr != nil {
		config = Config{}
		aggregatedErrors = multierr.Append(aggregatedErrors, bindErr)
	}

	// Attach the store to the config
	config.Store = s

	return &config, aggregatedErrors
}

// WriteSecrets persists configuration to a Kubernetes secret
func (s *KubernetesStore) WriteSecrets(config *Config) error {
	secretConfigFields := reflect.VisibleFields(reflect.TypeFor[SecretConfig]())
	cfg := config.SecretConfig
	reflectedValue := reflect.ValueOf(cfg)

	secret := &v1.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Type: v1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.namePrefix + "-authentication",
			Namespace: s.namespace,
		},
		StringData: map[string]string{},
	}

	for _, f := range secretConfigFields {
		if f.Tag.Get("mapstructure") != "" {
			value := reflectedValue.FieldByName(f.Name)
			key := strings.ToUpper(strings.Split(f.Tag.Get("mapstructure"), ",")[0])
			key = strings.Replace(key, "-", "_", -1)
			secret.StringData[key] = fmt.Sprintf("%s", value)
		}
	}

	restConfig, err := KubernetesRestConfig(config)
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes REST config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}

	_, err = clientset.CoreV1().Secrets(s.namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
	if err != nil {
		var decodedError *kerrors.StatusError
		ok := errors.As(err, &decodedError)
		if ok && decodedError.ErrStatus.Reason == metav1.StatusReasonAlreadyExists {
			_, err = clientset.CoreV1().Secrets(s.namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
		} else {
			return fmt.Errorf("failed to create or update Kubernetes secret: %w", err)
		}
		return err
	}
	return nil
}

// WriteConfig persists persistent configuration to a Kubernetes configmap
func (s *KubernetesStore) WriteConfig(config *Config) error {
	return fmt.Errorf("WriteConfig is not implemented for KubernetesStore")
}

// NewReadOnlyStore creates a new read-only configuration store
func NewReadOnlyStore(v *viper.Viper) (*ReadOnlyStore, error) {
	return &ReadOnlyStore{
		defaultViper: v,
	}, nil
}

func (s *ReadOnlyStore) Read() (*Config, error) {
	// Initialize the returns
	var config Config
	var aggregatedErrors error

	// Bind the configuration to the Config struct
	if bindErr := s.defaultViper.Unmarshal(&config); bindErr != nil {
		config = Config{}
		aggregatedErrors = multierr.Append(aggregatedErrors, bindErr)
	}

	// Attach the store to the config
	config.Store = s

	return &config, aggregatedErrors
}

// WriteSecrets is currently not-implemented for the ReadOnlyStore
func (s *ReadOnlyStore) WriteSecrets(config *Config) error {
	return fmt.Errorf("WriteSecrets is not implemented for ReadOnlyStore")
}

// WriteConfig is currently not-implemented for the ReadOnlyStore
func (s *ReadOnlyStore) WriteConfig(config *Config) error {
	return fmt.Errorf("WriteConfig is not implemented for ReadOnlyStore")
}

// mergeConfigMaps merges maps, appending any errors to the provided multierror
func mergeConfig(baseViper *viper.Viper, cfg map[string]any, err *error) {
	newErr := baseViper.MergeConfigMap(cfg)
	if newErr != nil {
		multierr.AppendInto(err, newErr)
	}
}

type fieldMapper struct {
	NameToMapstructureTag map[string]string
}

func getFieldMapper() fieldMapper {
	mapper := fieldMapper{
		NameToMapstructureTag: make(map[string]string),
	}

	t := reflect.TypeFor[Config]()
	configFields := collectEmbeddedTypeFields(t)
	for _, field := range configFields {
		if tag, ok := field.Tag.Lookup("mapstructure"); ok {
			mapName := strings.Split(tag, ",")[0]
			mapper.NameToMapstructureTag[strings.ToLower(field.Name)] = mapName
		}
	}

	return mapper
}

func collectEmbeddedTypeFields(t reflect.Type) []reflect.StructField {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var allFields []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.Anonymous {
			fields := collectEmbeddedTypeFields(sf.Type)
			allFields = append(allFields, fields...)
		} else {
			allFields = append(allFields, sf)
		}
	}
	return allFields
}

func (f *fieldMapper) convertToMapstructureTag(s map[string]any) map[string]any {
	newSettings := make(map[string]any, len(s))
	for k, v := range s {
		if mappedName, ok := f.NameToMapstructureTag[k]; ok {
			newSettings[mappedName] = v
		} else {
			newSettings[k] = v
		}
	}
	return newSettings
}

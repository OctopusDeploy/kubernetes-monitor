package register

import (
	"context"
	"log/slog"
	"net/url"

	"github.com/google/uuid"

	"github.com/octopusdeploy/kubernetes-monitor/internal/config"
	"github.com/octopusdeploy/kubernetes-monitor/internal/octopusdeploy"
)

func Run(ctx context.Context, c *config.Config, logger *slog.Logger) error {
	if c.MonitorIsRegistered() == nil {
		logger.Info("Monitor already registered, skipping")
		return nil
	}

	// Allow users defined installation IDs
	if c.InstallationId == "" {
		logger.Info("No installation ID provided, generating new installation ID")
		c.InstallationId = uuid.New().String()
	}

	logger.Info("Registering monitor with Octopus Server")
	kubernetesMonitor, err := registerWithOctopusServer(c)
	if err != nil {
		logger.Error("Failed to register monitor", slog.Any("error", err))
		return err
	}

	c.AuthenticationToken = kubernetesMonitor.AuthenticationToken
	c.ServerThumbprint = kubernetesMonitor.CertificateThumbprint
	err = c.Store.WriteSecrets(c)
	if err != nil {
		logger.Error(
			"Failed to save secrets to store",
			slog.Any("error", err),
		)
		return err
	}

	if c.ConfigurationStoreType == config.ConfigurationStoreTypeFile {
		logger.Info("Saving configuration to file store")
		err = c.Store.WriteConfig(c)
		if err != nil {
			logger.Error(
				"Failed to save secrets to store",
				slog.Any("error", err),
			)
			return err
		}
	}

	logger.Info("Authentication information saved, registration complete")
	return nil
}

func registerWithOctopusServer(c *config.Config) (octopusdeploy.RegisterKubernetesMonitorResponse, error) {
	apiUrl, err := url.Parse(c.ServerApiUrl)
	if err != nil {
		return octopusdeploy.RegisterKubernetesMonitorResponse{}, err
	}

	octopusClient, err := octopusdeploy.NewOctopusApiClient(c.ServerAccessToken, apiUrl, c.CaCertificatePath)
	if err != nil {
		return octopusdeploy.RegisterKubernetesMonitorResponse{}, err
	}

	kubernetesMonitor, err := octopusClient.RegisterKubernetesMonitor(
		c.InstallationId,
		c.SpaceId,
		c.MachineName,
	)
	if err != nil {
		return octopusdeploy.RegisterKubernetesMonitorResponse{}, err
	}

	return kubernetesMonitor, nil
}

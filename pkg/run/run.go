package run

import (
	"context"
	"errors"
	"log/slog"

	"github.com/octopusdeploy/kubernetes-monitor/internal/config"
	"github.com/octopusdeploy/kubernetes-monitor/internal/watcher"
	"github.com/octopusdeploy/kubernetes-monitor/pkg/register"
)

func Run(ctx context.Context, c *config.Config, logger *slog.Logger, otelConfig OtelConfig) error {
	// Set up OpenTelemetry
	otelShutdown, err := setupOTelSDK(ctx, otelConfig)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	// Temporary while we tease apart registration and running
	// In the future this will be done in a completely separate command
	err = register.Run(ctx, c, logger)
	if err != nil {
		return err
	}

	if err = c.MonitorIsRegistered(); err != nil {
		logger.Error("Kubernetes monitor not registered")
		return CommandLaunchError{err}
	}

	logger.Info("Starting monitor service")
	w, err := watcher.NewWatcher(ctx, c, logger)
	if err != nil {
		logger.With(slog.Any("error", err)).Error("Failed to start service")
		return errors.New("failed to start service")
	}

	err = w.Start(ctx)
	if err != nil {
		logger.With(slog.Any("error", err)).Error("Failed to start service")
		return errors.New("failed to start service")
	}

	select {
	case <-ctx.Done():
		logger.InfoContext(ctx, "Shutting Down...")
		return nil

	case chanErr := <-*w.ErrorCh:
		logger.With(slog.Any("error", chanErr)).Error("Error starting up")
		return errors.New("error from child service")
	}
}

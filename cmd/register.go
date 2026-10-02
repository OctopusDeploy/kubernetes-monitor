package cmd

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/octopusdeploy/kubernetes-monitor/internal/config"
	"github.com/octopusdeploy/kubernetes-monitor/internal/logger"
	"github.com/octopusdeploy/kubernetes-monitor/pkg/register"
)

var registerCmd = &cobra.Command{
	Use:   "register",
	Short: "Register a new instance",
	Long: `Registers a new instance with the server.
					
The server will generate a new installation ID and authentication token
and register will save the values in the specified config store.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Handle SIGINT (CTRL+C) gracefully.
		ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

		cfg, err := config.GetConfig(cmd.Flags(), "")
		logger := logger.New(cfg != nil && cfg.DebugEnabled)
		slog.SetDefault(logger)
		if err != nil {
			var failedToReadError config.FailedToReadConfigError
			ok := errors.As(err, &failedToReadError)
			if !ok {
				return err
			}
		}
		return register.Run(ctx, cfg, logger)
	},
}

func init() {
	config.InitRegisterFlags(registerCmd.Flags())
	rootCmd.AddCommand(registerCmd)
}

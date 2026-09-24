package cmd

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	_ "net/http/pprof"

	"github.com/octopusdeploy/kubernetes-monitor/internal/config"
	"github.com/octopusdeploy/kubernetes-monitor/internal/logger"
	"github.com/octopusdeploy/kubernetes-monitor/internal/profiling"
	"github.com/octopusdeploy/kubernetes-monitor/pkg/run"
)

var rootCmd = &cobra.Command{
	Use:   "kubernetes-monitor",
	Short: "Start monitoring Kubernetes resources",
	Long: `Start monitoring Kubernetes resources.

Makes a persistent connection with the server to receive resources to monitor and
report on resources that exist in the cluster.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Handle SIGINT (CTRL+C) gracefully.
		ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

		debug, _ := cmd.Flags().GetBool("debug")
		logger := logger.New(debug)
		slog.SetDefault(logger)
		cfg, err := config.GetConfig(cmd.Flags(), "")
		// We can fail to read the config if the user has not provided a config file
		// We shouldn't return an error in this case, but we should for any others
		if err != nil {
			var failedToReadError config.FailedToReadConfigError
			ok := errors.As(err, &failedToReadError)
			if !ok {
				logger.Error(err.Error())
			}
		}
		if debug {
			go func() {
				_ = http.ListenAndServe("localhost:5054", nil)
			}()
			pyroscopeUrl := "http://localhost:5053"
			if cfg.PyroscopeUrl != "" {
				pyroscopeUrl = cfg.PyroscopeUrl
			}
			err := profiling.UsePyroscope(
				profiling.WithLogger(logger),
				profiling.WithoutDebugLogs(),
				profiling.WithPyroscopeUrl(pyroscopeUrl),
			)
			if err != nil {
				logger.Error(err.Error())
			}
		}

		tracingEnabled, _ := cmd.Flags().GetBool("tracing")
		printTraces, _ := cmd.Flags().GetBool("print-traces")
		otelConfig := run.OtelConfig{
			TracingEnabled: tracingEnabled,
			PrintTraces:    printTraces,
		}

		err = run.Run(ctx, cfg, logger, otelConfig)
		if errors.As(err, &run.CommandLaunchError{}) {
			return err
		}
		return nil
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	config.InitCommonFlags(rootCmd.PersistentFlags())
	config.InitRunFlags(rootCmd.Flags())

	// Temporary - remove this once registration is no longer part of the root command
	config.InitRegisterFlags(rootCmd.Flags())
}

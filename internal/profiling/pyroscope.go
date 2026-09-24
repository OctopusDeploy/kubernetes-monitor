package profiling

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime"

	"github.com/grafana/pyroscope-go"
	"go.uber.org/multierr"
)

type config struct {
	// Pyroscope server address
	ServerAddress string
	// Application name
	ApplicationName string
	// Custom tags
	Tags map[string]string
	// Profile types to collect
	ProfileTypes []pyroscope.ProfileType
	// Logger
	Logger pyroscope.Logger
	// Mutex profile fraction
	MutexProfileFraction int
	// Block profile rate
	BlockProfileRate int
}

type Option interface {
	apply(config) config
}

func WithLogger(l *slog.Logger) Option {
	return loggerOption{l}
}

func WithoutDebugLogs() Option {
	return noDebugLoggerOption{}
}

func WithPyroscopeUrl(serverAddress string) Option {
	return serverAddressOption{serverAddress}
}

type slogWrapper struct {
	l              *slog.Logger
	WriteDebugLogs bool
}

func (s slogWrapper) Infof(format string, args ...interface{}) {
	s.l.Info(fmt.Sprintf(format, args...))
}

func (s slogWrapper) Debugf(format string, args ...interface{}) {
	if !s.WriteDebugLogs {
		// If debug logs are not enabled, we skip logging.
		return
	}
	s.l.Debug(fmt.Sprintf(format, args...))
}

func (s slogWrapper) Errorf(format string, args ...interface{}) {
	s.l.Error(fmt.Sprintf(format, args...))
}

type loggerOption struct {
	l *slog.Logger
}

func (o loggerOption) apply(cfg config) config {
	cfg.Logger = slogWrapper{o.l, true}
	return cfg
}

type noDebugLoggerOption struct{}

func (o noDebugLoggerOption) apply(cfg config) config {
	if cfg.Logger != nil {
		wrappedSlog, ok := cfg.Logger.(slogWrapper)
		if !ok {
			// If the logger is not a slog wrapper, we can't modify it.
			return cfg
		}
		wrappedSlog.WriteDebugLogs = false
		cfg.Logger = wrappedSlog
	}
	return cfg
}

type serverAddressOption struct {
	serverAddress string
}

func (o serverAddressOption) apply(cfg config) config {
	cfg.ServerAddress = o.serverAddress
	return cfg
}

// newConfig creates a validated Config configured with options.
func newConfig(options ...Option) config {
	cfg := config{
		ServerAddress:   "http://localhost:5053",
		ApplicationName: "kubernetes-monitor",
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
		MutexProfileFraction: 5,
		BlockProfileRate:     5,
	}
	for _, opt := range options {
		cfg = opt.apply(cfg)
	}
	return cfg
}

func UsePyroscope(options ...Option) error {
	cfg := newConfig(options...)

	healthCheckAddress := fmt.Sprintf("%s/ready", cfg.ServerAddress)
	res, err := http.Get(healthCheckAddress)
	if err != nil || res.StatusCode != 200 {
		return multierr.Combine(
			fmt.Errorf(
				"pyroscope server is not reachable at %s, skipping continuous profiling initialization",
				cfg.ServerAddress,
			),
			err,
		)
	}

	runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
	runtime.SetBlockProfileRate(cfg.BlockProfileRate)

	_, err = pyroscope.Start(pyroscope.Config{
		ApplicationName: cfg.ApplicationName,
		ServerAddress:   cfg.ServerAddress,
		Logger:          cfg.Logger,
		Tags:            cfg.Tags,
		ProfileTypes:    cfg.ProfileTypes,
	})
	return err
}

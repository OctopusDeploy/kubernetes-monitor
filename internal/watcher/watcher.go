package watcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/connection"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	certpool "github.com/octopusdeploy/kubernetes-monitor/internal/certificate"
	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/commands"
	"github.com/octopusdeploy/kubernetes-monitor/internal/communication"
	"github.com/octopusdeploy/kubernetes-monitor/internal/config"
	"github.com/octopusdeploy/kubernetes-monitor/internal/events"
	"github.com/octopusdeploy/kubernetes-monitor/internal/logs"
	"github.com/octopusdeploy/kubernetes-monitor/internal/octopusdeploy"
)

type Watcher struct {
	ResourceMonitorPeriod  time.Duration
	DiscoveryPeriod        time.Duration
	HealthCheckInterval    time.Duration
	HealthCheckGiveUpAfter time.Duration
	InstallationId         string
	ErrorCh                *chan error
	Logger                 *slog.Logger
	Connection             connection.Connection
	Clusters               cluster.ClusterList

	// subscribers tracks the goroutines started for one run, so a health-driven
	// stop can wait for them before the next run starts them again.
	subscribers sync.WaitGroup
}

// NewWatcher Creates a new watcher service with the provided configuration.
func NewWatcher(ctx context.Context, c *config.Config, l *slog.Logger) (*Watcher, error) {
	rootCertificates, err := certpool.GetRootCertificatePool(c.CaCertificatePath)
	if err != nil {
		l.Warn("Failed to load custom root CA bundle, using system CAs only", slog.Any("error", err))
	}

	if c.DisableGrpcCompression {
		l.Warn("gRPC compression is disabled.")
	}

	conn, err := connection.New(connection.Config{
		ServerURL: c.ServerGrpcUrl,
		Credentials: octopusdeploy.RpcAuth{
			InstallationId:      c.InstallationId,
			AuthenticationToken: c.AuthenticationToken,
		},
		TLS: connection.TLSConfig{
			Thumbprint:     c.ServerThumbprint,
			PinCertificate: c.PinGrpcCertificate,
			RootCAs:        rootCertificates,
		},
		CallOptions: communication.GetGrpcCallOptions(
			c.MaxGrpcSendMessageSizeBytes,
			c.MaxGrpcReceiveMessageSizeBytes,
			!c.DisableGrpcCompression,
		),
		DialOptions: []grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		},
	}, l)
	if err != nil {
		return nil, err
	}

	k8sConfig, err := config.KubernetesRestConfig(c)
	if err != nil {
		return nil, err
	}

	defaultMonitoredResourcesUpdater := NewDefaultMonitoredResourcesUpdater(conn, l, 10)

	if len(c.TargetNamespaces) > 0 {
		l.Info("Namespace-scoped monitoring enabled",
			slog.Any("targetNamespaces", c.TargetNamespaces),
			slog.Bool("clusterScopedResources", c.ClusterScopedResources))
	}

	errCh := make(chan error)
	watcher := &Watcher{
		ResourceMonitorPeriod: c.ResourceMonitorPeriod,
		InstallationId:        c.InstallationId,
		Clusters: cluster.NewClusterList(
			ctx,
			k8sConfig,
			l,
			defaultMonitoredResourcesUpdater,
			c.TargetNamespaces,
			c.ClusterScopedResources,
		),
		Logger:                 l,
		Connection:             conn,
		ErrorCh:                &errCh,
		HealthCheckInterval:    c.HealthCheckInterval,
		HealthCheckGiveUpAfter: c.HealthCheckGiveUpAfter,
	}

	return watcher, nil
}

// Start starts the watcher service.
func (w *Watcher) Start(ctx context.Context) error {
	runCtx, cancelRun := context.WithCancel(ctx)

	// The first start is synchronous so a bad connection or a rejected stream is
	// returned to the caller rather than surfacing later as a restart failure.
	if err := w.startSubscribers(runCtx); err != nil {
		cancelRun()
		return err
	}

	if w.HealthCheckInterval <= 0 {
		w.Logger.Warn("Health check is disabled, nothing will restart this pod if Octopus Server stops answering")
		context.AfterFunc(ctx, cancelRun)

		return nil
	}

	healthCheck := w.newHealthCheck(ctx)
	go healthCheck.Start()
	go w.superviseSubscribers(ctx, cancelRun, healthCheck, w.startSubscribers)

	return nil
}

func (w *Watcher) newHealthCheck(ctx context.Context) *connection.HealthCheck {
	return connection.NewHealthCheck(
		ctx,
		w.Connection,
		w.healthCheckConfig(),
		w.Logger.With(slog.String("component", "HealthCheck")),
	)
}

func (w *Watcher) healthCheckConfig() connection.HealthCheckConfig {
	return connection.HealthCheckConfig{
		Interval:    w.HealthCheckInterval,
		GiveUpAfter: w.HealthCheckGiveUpAfter,
	}
}

// superviseSubscribers stops the subscribers while Octopus Server is not answering
// and starts them again once it is, because a subscriber's own retries cannot tell a
// broken stream from an absent server.
//
// This must consume Events(). The channel is buffered at two and its sends block, so
// a supervisor that ignored transitions would wedge the probe loop on the third one
// and no fatal error would ever arrive.
func (w *Watcher) superviseSubscribers(
	ctx context.Context,
	cancelRun context.CancelFunc,
	healthCheck *connection.HealthCheck,
	startSubscribers func(context.Context) error,
) {
	for {
		select {
		case <-ctx.Done():
			cancelRun()
			return

		case err := <-healthCheck.Errors():
			cancelRun()
			w.report(ctx, err)

			return

		case transition := <-healthCheck.Events():
			if transition == connection.Up {
				continue
			}

			w.Logger.Warn("Octopus Server stopped answering, stopping subscribers")
			cancelRun()
			w.waitForSubscribers()

			if !healthCheck.AwaitRecovery(ctx) {
				// AwaitRecovery consumes the fatal error rather than passing it on, so
				// there is nothing to forward here beyond the fact that it gave up.
				w.report(ctx, errors.New("Octopus Server health did not recover, stopping so the pod is restarted"))

				return
			}

			w.Logger.Info("Octopus Server is answering again, restarting subscribers")

			var runCtx context.Context
			runCtx, cancelRun = context.WithCancel(ctx)

			if err := startSubscribers(runCtx); err != nil {
				cancelRun()
				w.report(ctx, err)

				return
			}
		}
	}
}

func (w *Watcher) startSubscribers(ctx context.Context) error {
	w.Logger.With(slog.Duration("interval", w.ResourceMonitorPeriod)).Info("Resource monitor loop started")
	w.runSubscriber(func() {
		ticker := time.NewTicker(w.ResourceMonitorPeriod)
		defer ticker.Stop()
		w.StartResourceMonitorLoop(ctx, ticker, w.Connection.Get())
	})

	var err error
	err = multierr.Append(err, w.SubscribeToConfigurationUpdates(ctx))
	err = multierr.Append(err, w.SubscribeToLogRequests(ctx))
	err = multierr.Append(err, w.SubscribeToEventRequests(ctx))

	return err
}

func (w *Watcher) runSubscriber(start func()) {
	w.subscribers.Add(1)

	go func() {
		defer w.subscribers.Done()
		start()
	}()
}

func (w *Watcher) waitForSubscribers() {
	stopped := make(chan struct{})

	go func() {
		w.subscribers.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(subscriberStopTimeout):
		w.Logger.Warn("Timed out waiting for subscribers to stop, restarting them anyway")
	}
}

// forwardErrors moves a handler's first error onto the watcher's channel. It stops
// with the run context, so restarting subscribers does not leave one of these
// parked on a dead handler for the life of the process.
func (w *Watcher) forwardErrors(ctx context.Context, errCh <-chan error) {
	go func() {
		select {
		case <-ctx.Done():
		case err := <-errCh:
			w.report(ctx, err)
		}
	}()
}

// report sends an error to the watcher's channel unless the run is already over,
// so a subscriber erroring on its way down does not look like a fresh failure.
func (w *Watcher) report(ctx context.Context, err error) {
	select {
	case *w.ErrorCh <- err:
	case <-ctx.Done():
	}
}

func (w *Watcher) SubscribeToConfigurationUpdates(parentCtx context.Context) error {
	ctx := context.WithValue(parentCtx, ContextKey("component"), "CommandHandler")
	logger := w.Logger.With(slog.String("component", "CommandHandler"))
	cs := commands.NewCommandHandler(&w.Clusters, w.Connection.Get(), ctx, logger)

	w.forwardErrors(ctx, cs.ErrCh)

	err := cs.Connect()
	if err != nil {
		return err
	}

	w.runSubscriber(cs.StartSubscriber)
	return nil
}

func (w *Watcher) SubscribeToLogRequests(parentCtx context.Context) error {
	ctx := context.WithValue(parentCtx, ContextKey("component"), "LogsHandler")
	logger := w.Logger.With(slog.String("component", "LogsHandler"))

	logHandler := logs.NewHandler(&w.Clusters, logger, ctx, w.Connection.Get())

	w.forwardErrors(ctx, logHandler.ErrCh)

	err := logHandler.Connect()
	if err != nil {
		return err
	}

	w.runSubscriber(logHandler.StartSubscriber)
	return nil
}

func (w *Watcher) SubscribeToEventRequests(parentCtx context.Context) error {
	ctx := context.WithValue(parentCtx, ContextKey("component"), "EventsHandler")
	logger := w.Logger.With(slog.String("component", "EventsHandler"))

	eventHandler := events.NewHandler(&w.Clusters, logger, ctx, w.Connection.Get())

	w.forwardErrors(ctx, eventHandler.ErrCh)

	err := eventHandler.Connect()
	if err != nil {
		return err
	}

	w.runSubscriber(eventHandler.StartSubscriber)
	return nil
}

const subscriberStopTimeout = 30 * time.Second

type ContextKey string

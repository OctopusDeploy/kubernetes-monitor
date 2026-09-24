package watcher

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"

	log "github.com/octopusdeploy/kubernetes-monitor/internal/logger"
	"github.com/octopusdeploy/kubernetes-monitor/internal/octopusdeploy"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/watcher"

var tracer = otel.Tracer(name)

func (w *Watcher) StartResourceMonitorLoop(ctx context.Context, ticker *time.Ticker, conn *grpc.ClientConn) {
	logger := w.Logger.With(slog.String("component", "ResourceMonitor"))

	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "Shutting down resource monitor service")
			return
		case <-ticker.C:
			tickerContext := context.WithValue(ctx, ContextKey("component"), "tickerTick")
			w.UpdateMonitoredResources(tickerContext, logger, conn)
		}
	}
}

func (w *Watcher) UpdateMonitoredResources(_ context.Context, logger *slog.Logger, conn *grpc.ClientConn) {
	localContext, correlationId := octopusdeploy.AppendCorrelationId(context.TODO())
	localContext, span := tracer.Start(localContext, "UpdateMonitoredResourcesLoopInvocation")
	defer span.End()
	logger = log.AddCorrelationId(logger, correlationId)

	logger.Info("Running resource monitor loop")
	client := pb.NewLiveStatusServiceClient(conn)

	for _, cluster := range w.Clusters.GetAll() {
		cluster.InvalidateDiscoveryClient()
		for applicationInstanceUpdate := range cluster.GetApplicationInstanceUpdates(localContext) {
			replaceResourcesRequest, errors := pb.ToReplaceMonitoredResourceRequest(*applicationInstanceUpdate)

			if len(errors) > 0 {
				for _, err := range errors {
					logger.Error("Error converting resource to update", slog.Any("error", err))
				}
			}

			logger.Info("Sending resource replacement for desired state",
				slog.Int("presentMonitoredResources", len(replaceResourcesRequest.PresentMonitoredResources)),
				slog.Int("childMonitoredResources", len(replaceResourcesRequest.ChildMonitoredResources)),
				slog.Int("missingMonitoredResources", len(replaceResourcesRequest.MissingMonitoredResources)),
				slog.Int("unknownMonitoredResources", len(replaceResourcesRequest.UnknownMonitoredResources)),
				slog.Any("key", replaceResourcesRequest.ApplicationInstanceId),
			)

			replaceMonitoredResourcesStart := time.Now()
			_, err := client.ReplaceMonitoredResources(localContext, replaceResourcesRequest)
			logger.Info(
				"ReplaceMonitoredResources completed",
				slog.Duration("duration", time.Now().Sub(replaceMonitoredResourcesStart)),
			)

			if err != nil {
				logger.Error("Error sending resource updates", slog.Any("error", err))
			}
		}
	}
}

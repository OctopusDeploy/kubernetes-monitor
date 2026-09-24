package watcher

import (
	"context"
	"log/slog"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/connection"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	log "github.com/octopusdeploy/kubernetes-monitor/internal/logger"
	"github.com/octopusdeploy/kubernetes-monitor/internal/octopusdeploy"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

type sendQueue struct {
	updateQueue chan queuedItem
}

type queuedItem interface {
	sendFunc(client pb.LiveStatusServiceClient, logger *slog.Logger)
}

type basicUpdate struct {
	ctx                    context.Context
	updateResourcesRequest *pb.UpdateMonitoredResourcesRequest
}

func (u *basicUpdate) sendFunc(client pb.LiveStatusServiceClient, logger *slog.Logger) {
	ctx, span := tracer.Start(u.ctx, "basicUpdate.sendFunc")
	defer span.End()
	_, err := client.UpdateMonitoredResources(ctx, u.updateResourcesRequest)
	if err != nil {
		logger.Error("could not send update", slog.Any("Error", err.Error()))
	}
}

type basicDelete struct {
	ctx                    context.Context
	deleteResourcesRequest *pb.DeleteChildMonitoredResourcesRequest
}

func (u *basicDelete) sendFunc(client pb.LiveStatusServiceClient, logger *slog.Logger) {
	ctx, span := tracer.Start(u.ctx, "basicDelete.sendFunc")
	defer span.End()
	_, err := client.DeleteChildMonitoredResources(ctx, u.deleteResourcesRequest)
	if err != nil {
		logger.Error("Error sending resource deletions", slog.Any("error", err.Error()))
	}
}

type basicReplace struct {
	ctx                     context.Context
	replaceResourcesRequest *pb.ReplaceMonitoredResourcesRequest
}

func (u *basicReplace) sendFunc(client pb.LiveStatusServiceClient, logger *slog.Logger) {
	ctx, span := tracer.Start(u.ctx, "basicReplace.sendFunc")
	defer span.End()
	_, err := client.ReplaceMonitoredResources(ctx, u.replaceResourcesRequest)
	if err != nil {
		logger.Error("Error sending resource replacement", slog.Any("error", err.Error()))
	}
}

// DefaultMonitoredResourcesUpdater sends monitored resource changes to Server. Updates, deletes and
// replacements all share one queue, which the workers drain in order — but they then send
// concurrently, so nothing guarantees the order they arrive at Server in.
type DefaultMonitoredResourcesUpdater struct {
	logger    *slog.Logger
	sendQueue sendQueue
}

// runWorker resolves the client per item rather than holding one. These workers
// outlive a connection rebuild, and a stub captures the ClientConn it was built
// with, so one built once would go on sending to a connection already closed.
func (q *sendQueue) runWorker(conn connection.Connection, logger *slog.Logger) {
	for item := range q.updateQueue {
		item.sendFunc(pb.NewLiveStatusServiceClient(conn.Get()), logger)
	}
}

func NewDefaultMonitoredResourcesUpdater(
	conn connection.Connection, logger *slog.Logger, concurrentConnections int,
) *DefaultMonitoredResourcesUpdater {
	obj := &DefaultMonitoredResourcesUpdater{
		logger:    logger,
		sendQueue: sendQueue{make(chan queuedItem, 100)},
	}
	for i := 0; i < concurrentConnections; i++ {
		go obj.sendQueue.runWorker(conn, logger)
	}
	return obj
}

func (d *DefaultMonitoredResourcesUpdater) setupCorrelationIdAndSpan(
	ctx context.Context, spanName string,
) (context.Context, *slog.Logger, func()) {
	localContext, correlationId := octopusdeploy.AppendCorrelationId(ctx)
	localLogger := log.AddCorrelationId(d.logger, correlationId)
	localContext, span := tracer.Start(localContext, spanName)
	return localContext, localLogger, func() { span.End() }
}

func (d *DefaultMonitoredResourcesUpdater) logConversionErrors(
	logger *slog.Logger, errors []error, message string,
) {
	if len(errors) > 0 {
		for _, err := range errors {
			logger.Error(message, slog.Any("error", err))
		}
	}
}

func (d *DefaultMonitoredResourcesUpdater) Replace(
	ctx context.Context, replacement *cluster.ApplicationInstanceChanges,
) {
	localContext, localLogger, endSpan := d.setupCorrelationIdAndSpan(ctx, "DefaultMonitoredResourcesUpdater.Replace")
	defer endSpan()

	replaceResourcesRequest, errors := pb.ToReplaceMonitoredResourceRequest(*replacement)
	d.logConversionErrors(localLogger, errors, "Error converting resource to replacement")

	reqContext, reqSpan := tracer.Start(localContext, "DefaultMonitoredResourcesUpdater.Send.Replace")
	queuedReplace := basicReplace{ctx: reqContext, replaceResourcesRequest: replaceResourcesRequest}
	d.sendQueue.updateQueue <- &queuedReplace
	reqSpan.End()
}

func (d *DefaultMonitoredResourcesUpdater) Update(ctx context.Context, update *cluster.ApplicationInstanceChanges) {
	localContext, localLogger, endSpan := d.setupCorrelationIdAndSpan(ctx, "DefaultMonitoredResourcesUpdater.Update")
	defer endSpan()

	updateResourcesRequest, deleteResourcesRequest, errors := pb.ToUpdateMonitoredResourceRequest(localContext, *update)
	d.logConversionErrors(localLogger, errors, "Error converting resource to update")

	localLogger.Debug("Received request to update monitored resources",
		slog.Any("UpdateResourcesRequest", updateResourcesRequest != nil),
		slog.Any("DeleteResourcesRequest", deleteResourcesRequest != nil),
	)

	if updateResourcesRequest != nil {
		reqContext, reqSpan := tracer.Start(localContext, "DefaultMonitoredResourcesUpdater.Send.Update")
		queuedUpdate := basicUpdate{ctx: reqContext, updateResourcesRequest: updateResourcesRequest}
		d.sendQueue.updateQueue <- &queuedUpdate
		reqSpan.End()
	}

	if deleteResourcesRequest != nil {
		reqContext, reqSpan := tracer.Start(localContext, "DefaultMonitoredResourcesUpdater.Send.Delete")
		queuedDelete := basicDelete{ctx: reqContext, deleteResourcesRequest: deleteResourcesRequest}
		d.sendQueue.updateQueue <- &queuedDelete
		reqSpan.End()
	}
}

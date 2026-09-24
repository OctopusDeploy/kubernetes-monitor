package logs

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"

	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/communication"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

type (
	sessionIdContextKey struct{}
	handlerContextKey   struct{}
	Handler             struct {
		Logger     *slog.Logger
		Clusters   *cluster.ClusterList
		client     pb.FetchContainerLogsServiceClient
		stream     pb.FetchContainerLogsService_FetchContainerLogsClient
		conn       *grpc.ClientConn
		context    context.Context
		retryCount int
		ErrCh      chan error
	}
)

const name = "github.com/octopusdeploy/kubernetes-monitor/internal/logs"

var tracer = otel.Tracer(name)

func NewHandler(
	clusters *cluster.ClusterList, logger *slog.Logger, parentContext context.Context, grpcConn *grpc.ClientConn,
) *Handler {
	ctx := context.WithValue(parentContext, handlerContextKey{}, "LogsHandler")
	return &Handler{
		Logger:   logger,
		Clusters: clusters,
		conn:     grpcConn,
		context:  ctx,
		ErrCh:    make(chan error),
	}
}

func (h *Handler) Connect() error {
	h.Logger.Debug("gRPC connection state before connect", slog.String("connState", h.conn.GetState().String()))
	h.client = pb.NewFetchContainerLogsServiceClient(h.conn)
	stream, err := h.client.FetchContainerLogs(h.context)
	if err != nil {
		return err
	}
	h.Logger.Debug("gRPC connection state after connect", slog.String("connState", h.conn.GetState().String()))
	h.stream = stream
	return nil
}

// RestartSubscriber backs off and reconnects the stream, returning false if the subscriber should
// stop. It deliberately does not call StartSubscriber itself; the caller drives reconnection
// iteratively so the goroutine stack does not grow with every reconnect.
func (h *Handler) RestartSubscriber() bool {
	h.retryCount++

	backoff := communication.CalculateBackoff(h.retryCount)
	h.Logger.Info("Attempting to restart log subscriber",
		slog.Int("retryCount", h.retryCount),
		slog.Duration("backoff", backoff),
		slog.String("connState", h.conn.GetState().String()))

	if !communication.WaitFor(h.context, backoff) {
		return false
	}

	if err := h.Connect(); err != nil {
		h.ErrCh <- err

		return false
	}

	return true
}

func (h *Handler) StartSubscriber() {
	h.Logger.Info("Starting log subscriber")

	// Supervisor loop: reconnection is driven iteratively here rather than by recursing through
	// RestartSubscriber -> StartSubscriber, so the stack does not grow with each reconnect.
	for h.receive() {
	}
}

// receive reads from the stream until it errors. It returns true if the stream was restarted and
// receiving should continue, or false if the subscriber should stop.
func (h *Handler) receive() bool {
	for {
		logRequest, err := h.stream.Recv()
		if err != nil {
			return communication.HandleStreamRecvErr(h.context, err, h.Logger, h.RestartSubscriber, h.ErrCh)
		}

		if h.retryCount > 0 {
			h.Logger.Debug("Stream recovered, resetting the retry counter",
				slog.Int("retryCount", h.retryCount))
			h.retryCount = 0
		}

		if !h.processRequest(logRequest) {
			return false
		}
	}
}

// processRequest handles a single log request. It returns true if the subscriber should keep
// receiving, or false if it should stop (an unrecoverable send error was reported on ErrCh).
func (h *Handler) processRequest(logRequest *pb.FetchContainerLogsRequest) bool {
	ctx := context.WithValue(h.context, sessionIdContextKey{}, logRequest.SessionId.Value)
	ctx, span := tracer.Start(ctx, "Handler.processFetchContainerLogsRequest")
	defer span.End()

	requestLogger := h.Logger.With("sessionId", logRequest.SessionId.Value)
	sessionAttribute := attribute.String("sessionId", logRequest.SessionId.Value)
	span.SetAttributes(sessionAttribute)
	requestLogger.InfoContext(ctx, "Received log request")

	clusterId := logRequest.ClusterId.FromProto()
	reqCluster, err := h.Clusters.GetCluster(clusterId)
	if err != nil {
		requestLogger.ErrorContext(ctx, "Error getting cluster", slog.Any("error", err))
		return true
	}

	span.AddEvent("clusterId")
	logLines, err := reqCluster.GetContainerLogs(
		logRequest.Namespace,
		logRequest.PodName,
		logRequest.ContainerName,
		logRequest.ShowPreviousContainer,
		ctx,
	)
	if err != nil {
		errCode := pb.ErrorCode_ERROR_CODE_UNEXPECTED
		if kerrors.IsForbidden(err) {
			errCode = pb.ErrorCode_ERROR_CODE_PERMISSION_DENIED
		}
		requestLogger.ErrorContext(ctx, "Error processing logs", slog.Any("error", err))
		msgErr := h.stream.SendMsg(&pb.FetchContainerLogsResponse{
			SessionId: logRequest.SessionId,
			LogLines:  make([]*pb.LogLine, 0),
			Error:     &pb.Error{Code: errCode, Message: err.Error()},
		})

		if msgErr != nil {
			return communication.HandleStreamSendErr(h.context, msgErr, h.Logger, h.RestartSubscriber, h.ErrCh)
		}

		return true
	}

	requestLogger.InfoContext(ctx, "sending logs response", slog.Any("logCount", len(logLines)))
	err = h.stream.SendMsg(&pb.FetchContainerLogsResponse{
		SessionId: logRequest.SessionId,
		LogLines:  pb.ToLogLines(logLines),
	})
	if err != nil {
		return communication.HandleStreamSendErr(h.context, err, h.Logger, h.RestartSubscriber, h.ErrCh)
	}
	requestLogger.DebugContext(ctx, "sent logs response")
	return true
}

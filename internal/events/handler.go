package events

import (
	"context"
	"log/slog"

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
		client     pb.FetchEventsServiceClient
		stream     pb.FetchEventsService_FetchEventsClient
		conn       *grpc.ClientConn
		context    context.Context
		retryCount int
		ErrCh      chan error
	}
)

func NewHandler(
	clusters *cluster.ClusterList,
	logger *slog.Logger,
	parentContext context.Context,
	grpcConn *grpc.ClientConn,
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
	h.client = pb.NewFetchEventsServiceClient(h.conn)
	stream, err := h.client.FetchEvents(h.context)
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
	h.Logger.Info("Attempting to restart event subscriber",
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
	h.Logger.Info("Starting event subscriber")

	// Supervisor loop: reconnection is driven iteratively here rather than by recursing through
	// RestartSubscriber -> StartSubscriber, so the stack does not grow with each reconnect.
	for h.receive() {
	}
}

// receive reads from the stream until it errors. It returns true if the stream was restarted and
// receiving should continue, or false if the subscriber should stop.
func (h *Handler) receive() bool {
	for {
		eventRequest, err := h.stream.Recv()
		if err != nil {
			return communication.HandleStreamRecvErr(h.context, err, h.Logger, h.RestartSubscriber, h.ErrCh)
		}

		if h.retryCount > 0 {
			h.Logger.Debug("Stream recovered, resetting the retry counter",
				slog.Int("retryCount", h.retryCount))
			h.retryCount = 0
		}

		ctx := context.WithValue(h.context, sessionIdContextKey{}, eventRequest.SessionId.Value)
		requestLogger := h.Logger.With("sessionId", eventRequest.SessionId.Value)
		requestLogger.InfoContext(ctx, "Received event request")

		clusterId := eventRequest.ClusterId.FromProto()
		reqCluster, err := h.Clusters.GetCluster(clusterId)
		if err != nil {
			requestLogger.ErrorContext(ctx, "Error getting cluster", slog.Any("error", err))
			continue
		}

		events, err := reqCluster.GetEvents(
			eventRequest.Namespace,
			eventRequest.Name,
			eventRequest.Kind,
			ctx,
		)
		if err != nil {
			requestLogger.ErrorContext(ctx, "Error processing events", slog.Any("error", err))
			errCode := pb.ErrorCode_ERROR_CODE_UNEXPECTED
			if kerrors.IsForbidden(err) {
				errCode = pb.ErrorCode_ERROR_CODE_PERMISSION_DENIED
			}
			msgErr := h.stream.SendMsg(&pb.FetchEventsResponse{
				SessionId: eventRequest.SessionId,
				Events:    make([]*pb.Event, 0),
				Error:     &pb.Error{Code: errCode, Message: err.Error()},
			})
			if msgErr != nil {
				if !communication.HandleStreamSendErr(h.context, msgErr, h.Logger, h.RestartSubscriber, h.ErrCh) {
					return false
				}
			}
			continue
		}

		requestLogger.InfoContext(ctx, "sending events response", slog.Any("eventCount", len(events)))
		err = h.stream.SendMsg(&pb.FetchEventsResponse{
			SessionId: eventRequest.SessionId,
			Events:    pb.ToEvents(events),
		})
		if err != nil {
			if !communication.HandleStreamSendErr(h.context, err, h.Logger, h.RestartSubscriber, h.ErrCh) {
				return false
			}
			continue
		}
		requestLogger.DebugContext(ctx, "sent events response")
	}
}

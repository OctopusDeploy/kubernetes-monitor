package commands

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/octopusdeploy/kubernetes-monitor/internal/cluster"
	"github.com/octopusdeploy/kubernetes-monitor/internal/communication"
	"github.com/octopusdeploy/kubernetes-monitor/internal/logger"
	"github.com/octopusdeploy/kubernetes-monitor/internal/octopusdeploy"
	pb "github.com/octopusdeploy/kubernetes-monitor/internal/protos"
)

type CommandHandler struct {
	Logger     *slog.Logger
	Clusters   *cluster.ClusterList
	client     pb.CommandsServiceClient
	stream     pb.CommandsService_SubscribeToCommandsClient
	conn       *grpc.ClientConn
	context    context.Context
	retryCount int
	ErrCh      chan error
}

func NewCommandHandler(
	clusters *cluster.ClusterList,
	grpcConn *grpc.ClientConn,
	ctx context.Context,
	log *slog.Logger,
) *CommandHandler {
	localContext, correlationId := octopusdeploy.AppendCorrelationId(ctx)
	return &CommandHandler{
		Logger:   logger.AddCorrelationId(log, correlationId),
		Clusters: clusters,
		ErrCh:    make(chan error),
		conn:     grpcConn,
		context:  localContext,
	}
}

func (c *CommandHandler) Connect() error {
	c.Logger.Info("Attempting to connect to Octopus server...")
	c.Logger.Debug("gRPC connection state before connect", slog.String("connState", c.conn.GetState().String()))
	c.client = pb.NewCommandsServiceClient(c.conn)
	stream, err := c.client.SubscribeToCommands(c.context, grpc.WaitForReady(false))
	if err != nil {
		c.Logger.Error(
			"Failed to connect to Octopus server, waiting for connection to be ready...",
			slog.Any("error", err.Error()),
			slog.String("connState", c.conn.GetState().String()),
		)

		stream, err = c.client.SubscribeToCommands(c.context)
		if err != nil {
			return err
		}
	}

	c.Logger.Info("Successfully connected to Octopus server")
	c.Logger.Debug("gRPC connection state after connect", slog.String("connState", c.conn.GetState().String()))
	c.stream = stream
	return nil
}

// RestartSubscriber backs off and reconnects the stream, returning false if the subscriber should
// stop. It deliberately does not call StartSubscriber itself; the caller drives reconnection
// iteratively so the goroutine stack does not grow with every reconnect.
func (c *CommandHandler) RestartSubscriber() bool {
	c.retryCount++

	backoff := communication.CalculateBackoff(c.retryCount)
	c.Logger.Info("Attempting to restart command subscriber",
		slog.Int("retryCount", c.retryCount),
		slog.Duration("backoff", backoff),
		slog.String("connState", c.conn.GetState().String()))

	if !communication.WaitFor(c.context, backoff) {
		return false
	}

	if err := c.Connect(); err != nil {
		c.ErrCh <- err

		return false
	}

	return true
}

func (c *CommandHandler) StartSubscriber() {
	c.Logger.Info("Starting command subscriber")

	// Supervisor loop: reconnection is driven iteratively here rather than by recursing through
	// RestartSubscriber -> StartSubscriber, so the stack does not grow with each reconnect.
	for c.receive() {
	}
}

// receive reads from the stream until it errors. It returns true if the stream was restarted and
// receiving should continue, or false if the subscriber should stop.
func (c *CommandHandler) receive() bool {
	for {
		commandResponse, err := c.stream.Recv()
		if err != nil {
			return communication.HandleStreamRecvErr(c.context, err, c.Logger, c.RestartSubscriber, c.ErrCh)
		}

		if c.retryCount > 0 {
			c.Logger.Debug("Stream recovered, resetting the retry counter",
				slog.Int("retryCount", c.retryCount))
			c.retryCount = 0
		}

		err = c.handle(commandResponse)
		if err != nil {
			c.Logger.Error("Error handling command", slog.Any("error", err))
		}
	}
}

func (c *CommandHandler) handle(streamFromServer *pb.ServerToClientStream) error {
	switch msg := streamFromServer.Command.(type) {
	case *pb.ServerToClientStream_UpdateDesiredResourcesCommand:
		return c.handleUpdateDesiredResourcesCommand(msg.UpdateDesiredResourcesCommand)
	case *pb.ServerToClientStream_PruneOtherVersionsCommand:
		return c.handlePruneOtherVersionsCommand(msg.PruneOtherVersionsCommand)
	case *pb.ServerToClientStream_DeleteDesiredResourcesCommand:
		return c.handleDeleteDesiredResourcesCommand(msg.DeleteDesiredResourcesCommand)
	case *pb.ServerToClientStream_ReplaceDesiredResourcesCommand:
		return c.handleReplaceDesiredResourcesCommand(msg.ReplaceDesiredResourcesCommand)
	default:
		c.Logger.Warn("Received unrecognised command from server")
	}

	return nil
}

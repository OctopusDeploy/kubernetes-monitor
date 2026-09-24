package communication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/grpcerror"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	stdgzip "compress/gzip"
)

func init() {
	// BestSpeed uses the least CPU but still gives a good compression ratio. This mutates the
	// process-wide "gzip" compressor, so keep this the only call to gzip.SetLevel. On failure gRPC
	// keeps the default level, which is a fair compromise to crashing.
	//
	// We still set this even when compression is disabled
	if err := gzip.SetLevel(stdgzip.BestSpeed); err != nil {
		slog.Warn("failed to set gzip compression level, falling back to the default level",
			slog.Any("error", err))
	}
}

const (
	// BaseBackoffSeconds Base backoff duration for exponential backoff (in seconds)
	BaseBackoffSeconds = 2
	// MaxBackoffMinutes Maximum backoff duration to prevent excessive delays (in minutes)
	MaxBackoffMinutes = 3
)

// WaitFor sleeps for d unless ctx is cancelled first, returning false if the wait
// was cut short. Callers treat that as "stop" rather than "retry now": a plain
// time.Sleep here would hold a subscriber in backoff for up to MaxBackoffMinutes
// after it had been told to stop.
func WaitFor(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func CalculateBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0
	}

	// Exponential backoff: base * 2^(retryCount-1)
	backoffSeconds := float64(BaseBackoffSeconds) * math.Pow(2, float64(retryCount-1))

	maxBackoffSeconds := float64(MaxBackoffMinutes * 60)
	if backoffSeconds > maxBackoffSeconds {
		backoffSeconds = maxBackoffSeconds
	}

	return time.Duration(backoffSeconds) * time.Second
}

// HandleStreamRecvErr acts on an error from stream.Recv. It returns true if the
// subscriber was restarted and receiving should continue, or false if it should stop
// -- the stream ended on purpose, or the error was reported on errCh.
func HandleStreamRecvErr(
	ctx context.Context,
	recvErr error,
	logger *slog.Logger,
	restartFunc func() bool,
	errCh chan error,
) bool {
	if ctx.Err() != nil {
		logger.Info("Subscriber stopping, its context was cancelled")
		return false
	}

	switch classify(recvErr, logger) {
	case grpcerror.Retry:
		return restartFunc()
	case grpcerror.Stop:
		logger.Info("Subscriber stopping, the stream ended and will not be retried",
			slog.Any("error", recvErr))

		return false
	}

	logger.Error("unrecoverable error encountered, consider adding to recoverable error list if this is unintended",
		slog.Any("error", recvErr))
	_, file, no, ok := runtime.Caller(1)
	if ok {
		callerError := errors.New(fmt.Sprintf("%s#%d failed to receive stream message", file, no))
		err := multierr.Append(recvErr, callerError)
		errCh <- err
		return false
	}
	errCh <- recvErr
	return false
}

// HandleStreamSendErr acts on an error from stream.SendMsg, on the same terms as
// HandleStreamRecvErr.
func HandleStreamSendErr(
	ctx context.Context,
	sendErr error,
	logger *slog.Logger,
	restartFunc func() bool,
	errCh chan error,
) bool {
	if ctx.Err() != nil {
		logger.Info("Subscriber stopping, its context was cancelled")
		return false
	}

	switch classify(sendErr, logger) {
	case grpcerror.Retry:
		return restartFunc()
	case grpcerror.Stop:
		logger.Info("Subscriber stopping, the stream ended and will not be retried",
			slog.Any("error", sendErr))

		return false
	}

	logger.Error(
		"unrecoverable error encountered while sending stream message, consider adding to recoverable error list if this is unintended",
		slog.Any("error", sendErr),
	)
	_, file, no, ok := runtime.Caller(1)
	if ok {
		callerError := errors.New(fmt.Sprintf("%s#%d failed to send stream message", file, no))
		errCh <- multierr.Append(sendErr, callerError)
		return false
	}
	errCh <- sendErr
	return false
}

func GetGrpcCallOptions(
	maxSendMessageSizeBytes, maxReceiveMessageSizeBytes int,
	compressionEnabled bool,
) []grpc.CallOption {
	callOptions := []grpc.CallOption{
		grpc.MaxCallSendMsgSize(maxSendMessageSizeBytes),
		grpc.MaxCallRecvMsgSize(maxReceiveMessageSizeBytes),
	}

	if compressionEnabled {
		callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
	}

	return callOptions
}

func classify(err error, logger *slog.Logger) grpcerror.Action {
	action := grpcerror.Classify(err)

	logConnectionLost(err, logger)
	logger.Debug("Classified stream error",
		slog.String("grpcCode", status.Code(err).String()),
		slog.String("grpcErrorMessage", status.Convert(err).Message()),
		slog.String("action", action.String()),
	)

	return action
}

func logConnectionLost(err error, logger *slog.Logger) {
	switch {
	case errors.Is(err, io.EOF):
		logger.Error("connection lost to Octopus Server (IO EOF error)", slog.Any("error", err))
	case isInternalContaining(err, "EOF"):
		logger.Error("connection lost to Octopus Server (gRPC EOF error)", slog.Any("error", err))
	}
}

// isInternalContaining matches on the message because gRPC reports both a broken
// connection and a broken server as Internal, and only the message tells them apart.
func isInternalContaining(err error, substring string) bool {
	grpcError := status.Convert(err)

	return grpcError.Code() == codes.Internal && strings.Contains(grpcError.Message(), substring)
}

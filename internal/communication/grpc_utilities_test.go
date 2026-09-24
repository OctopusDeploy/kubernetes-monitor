package communication

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		expected   time.Duration
	}{
		{
			name:       "retry count 0 should return 0",
			retryCount: 0,
			expected:   0,
		},
		{
			name:       "retry count 1 should return 2 seconds",
			retryCount: 1,
			expected:   2 * time.Second,
		},
		{
			name:       "retry count 8 should return 256 seconds (capped at max)",
			retryCount: 8,
			expected:   180 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateBackoff(tt.retryCount)
			if result != tt.expected {
				t.Errorf("calculateBackoff(%d) = %v, want %v", tt.retryCount, result, tt.expected)
			}
		})
	}
}

func TestHandleStreamErr(t *testing.T) {
	// Create a logger that discards output for testing
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handlers := map[string]func(context.Context, error, *slog.Logger, func() bool, chan error) bool{
		"recv": HandleStreamRecvErr,
		"send": HandleStreamSendErr,
	}

	tests := []struct {
		name         string
		err          error
		wantRestart  bool
		wantReported bool
	}{
		{
			name:         "unrecoverable error should send error to channel",
			err:          status.Error(codes.NotFound, "not found"),
			wantRestart:  false,
			wantReported: true,
		},
		{
			name:        "recoverable error should restart",
			err:         status.Error(codes.Unavailable, "service unavailable"),
			wantRestart: true,
		},
		{
			name:        "EOF error should restart",
			err:         io.EOF,
			wantRestart: true,
		},
		{
			name: "RST_STREAM reset should restart",
			err: status.Error(
				codes.Internal,
				"stream terminated by RST_STREAM with error code: INTERNAL_ERROR",
			),
			wantRestart: true,
		},
		{
			name:         "Internal error without RST_STREAM or EOF should send error to channel",
			err:          status.Error(codes.Internal, "something broke server side"),
			wantRestart:  false,
			wantReported: true,
		},
		{
			// Reporting these would take the pod down for a stop somebody asked for.
			name:        "cancelled stream should stop without reporting",
			err:         status.Error(codes.Canceled, "context canceled"),
			wantRestart: false,
		},
		{
			name:        "unimplemented RPC should stop without reporting",
			err:         status.Error(codes.Unimplemented, "unknown method"),
			wantRestart: false,
		},
	}

	for handlerName, handler := range handlers {
		for _, tt := range tests {
			t.Run(handlerName+": "+tt.name, func(t *testing.T) {
				errCh := make(chan error, 1)
				restartCalled := false

				cont := handler(t.Context(), tt.err, logger, func() bool {
					restartCalled = true
					return true
				}, errCh)

				if restartCalled != tt.wantRestart {
					t.Errorf("restart called = %v, want %v", restartCalled, tt.wantRestart)
				}
				if cont != tt.wantRestart {
					t.Errorf("continue = %v, want %v", cont, tt.wantRestart)
				}
				if tt.wantReported && len(errCh) == 0 {
					t.Error("expected unrecoverable error to be sent to channel")
				}
				if !tt.wantReported && len(errCh) > 0 {
					t.Errorf("expected nothing on the error channel, got %v", <-errCh)
				}
			})
		}
	}
}

// A cancelled subscriber is being stopped on purpose, not failing. Reporting the
// resulting Canceled error would send it up to the watcher's ErrorCh and take the
// pod down, which is the opposite of pausing while the server is unreachable.
func TestHandleStreamRecvErr_CancelledContextStopsQuietly(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	restarted := false
	keepGoing := HandleStreamRecvErr(ctx, status.Error(codes.Canceled, "context canceled"), logger,
		func() bool { restarted = true; return true }, errCh)

	if keepGoing {
		t.Error("Expected the subscriber to stop, it was told to continue")
	}
	if restarted {
		t.Error("Expected no restart attempt on a cancelled context")
	}
	select {
	case err := <-errCh:
		t.Errorf("Expected nothing on the error channel, got %v", err)
	default:
	}
}

func TestWaitFor_ReturnsEarlyWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	started := time.Now()
	if WaitFor(ctx, time.Minute) {
		t.Error("Expected WaitFor to report the wait was cut short")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("Expected WaitFor to return immediately, it took %s", elapsed)
	}
}

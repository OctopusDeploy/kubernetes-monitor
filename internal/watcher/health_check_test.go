package watcher

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OctopusDeploy/octopus-grpc/go/pkg/connection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestSuperviseSubscribers_StopsSubscribersWhenHealthGoesDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	w, health, runs := supervisedWatcher(t, ctx, time.Second)

	first := runs.await(t, 1)
	if first.Err() != nil {
		t.Fatal("Expected the first run to start with a live context")
	}

	health.fail(true)

	if !eventually(4*time.Second, func() bool { return first.Err() != nil }) {
		t.Fatal("Expected the run context to be cancelled once health went down")
	}
	_ = w
}

func TestSuperviseSubscribers_RestartsSubscribersWhenHealthRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, health, runs := supervisedWatcher(t, ctx, time.Second)

	first := runs.await(t, 1)
	health.fail(true)

	if !eventually(4*time.Second, func() bool { return first.Err() != nil }) {
		t.Fatal("Expected the first run to be cancelled")
	}

	health.fail(false)

	second := runs.await(t, 2)
	if second.Err() != nil {
		t.Error("Expected the restarted run to have a live context")
	}
}

func TestSuperviseSubscribers_ReportsFatalErrorsOnTheWatcherErrorChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	w, health, _ := supervisedWatcher(t, ctx, 20*time.Millisecond)
	health.fail(true)

	select {
	case err := <-*w.ErrorCh:
		if err == nil {
			t.Fatal("Expected a non-nil error")
		}
	case <-ctx.Done():
		t.Fatal("Expected a fatal error on ErrorCh, got none")
	}
}

// The health check's events channel is buffered at two and its sends block, so a
// supervisor that stopped consuming transitions would wedge the probe loop and no
// fatal error would ever arrive. This flaps health well past the buffer.
func TestSuperviseSubscribers_SurvivesMoreTransitionsThanTheEventBufferHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()

	_, health, runs := supervisedWatcher(t, ctx, time.Second)

	for round := 1; round <= 4; round++ {
		current := runs.await(t, round)

		health.fail(true)
		if !eventually(2*time.Second, func() bool { return current.Err() != nil }) {
			t.Fatalf("round %d: expected the run to be cancelled, probe made %d checks",
				round, health.checks.Load())
		}

		health.fail(false)
		if next := runs.await(t, round+1); next.Err() != nil {
			t.Fatalf("round %d: expected a live context for the restarted run", round)
		}
	}
}

func supervisedWatcher(
	t *testing.T,
	ctx context.Context,
	giveUpAfter time.Duration,
) (*Watcher, *switchableHealth, *runRecorder) {
	t.Helper()

	errCh := make(chan error, 1)
	w := &Watcher{
		Logger:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		ErrorCh:                &errCh,
		HealthCheckInterval:    10 * time.Millisecond,
		HealthCheckGiveUpAfter: giveUpAfter,
	}

	health := newSwitchableHealth(t)
	w.Connection = health.conn
	runs := &runRecorder{}

	runCtx, cancelRun := context.WithCancel(ctx)
	if err := runs.start(runCtx); err != nil {
		t.Fatalf("Expected the first start to succeed, got %v", err)
	}

	healthCheck := w.newHealthCheck(ctx)
	go healthCheck.Start()
	go w.superviseSubscribers(ctx, cancelRun, healthCheck, runs.start)

	return w, health, runs
}

type runRecorder struct {
	mu       sync.Mutex
	contexts []context.Context
}

func (r *runRecorder) start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contexts = append(r.contexts, ctx)

	return nil
}

func (r *runRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.contexts)
}

func (r *runRecorder) await(t *testing.T, n int) context.Context {
	t.Helper()

	if !eventually(4*time.Second, func() bool { return r.count() >= n }) {
		t.Fatalf("Expected %d subscriber start(s), got %d", n, r.count())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.contexts[n-1]
}

// switchableHealth is a real in-process health server reached through a real
// connection.Connection. The health check builds its own stub from the
// connection, so a test drives it by answering real RPCs rather than by
// injecting a client.
type switchableHealth struct {
	conn   connection.Connection
	health *health.Server
	checks atomic.Int32
}

// fail flips what the server answers. A NOT_SERVING response is a failed probe as
// far as the health check is concerned.
func (s *switchableHealth) fail(failing bool) {
	status := grpc_health_v1.HealthCheckResponse_SERVING
	if failing {
		status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}

	s.health.SetServingStatus("", status)
}

// awaitReady connects before the first probe. grpc.NewClient is lazy and a probe
// is bounded by the check interval, so a test that let the dial happen under the
// first probe would be racing it.
func (s *switchableHealth) awaitReady(t *testing.T) {
	t.Helper()

	client := s.conn.Get()
	client.Connect()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	for client.GetState() != connectivity.Ready {
		if !client.WaitForStateChange(ctx, client.GetState()) {
			t.Fatal("Expected the connection to become ready")
		}
	}
}

func newSwitchableHealth(t *testing.T) *switchableHealth {
	t.Helper()

	s := &switchableHealth{health: health.NewServer()}
	s.fail(false)

	count := func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		s.checks.Add(1)

		return handler(ctx, req)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Expected to listen, got %v", err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(count))
	grpc_health_v1.RegisterHealthServer(server, s.health)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	s.conn, err = connection.New(connection.Config{
		ServerURL: listener.Addr().String(),
		TLS:       connection.TLSConfig{Plaintext: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Expected to build a connection, got %v", err)
	}

	t.Cleanup(func() { _ = s.conn.Close() })
	s.awaitReady(t)

	return s
}

func eventually(within time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}

	return condition()
}

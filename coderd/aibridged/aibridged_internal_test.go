package aibridged

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/xerrors"
	"storj.io/drpc"

	"cdr.dev/slog/v3"
	"cdr.dev/slog/v3/sloggers/slogtest"
	"github.com/coder/coder/v2/aibridge"
	"github.com/coder/coder/v2/aibridge/config"
	aibcontext "github.com/coder/coder/v2/aibridge/context"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/x/proxy"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/testutil"
)

func TestNewRecorder(t *testing.T) {
	t.Parallel()
	for _, options := range []PoolOptions{
		{},
		{StructuredLogging: true},
		{DisableContentRecording: true},
		{StructuredLogging: true, DisableContentRecording: true},
	} {
		t.Run(fmt.Sprintf("Structured=%t/DisableContent=%t", options.StructuredLogging, options.DisableContentRecording), func(t *testing.T) {
			t.Parallel()
			apiKeyID, id := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC()
			clientErr := xerrors.New("client unavailable")
			contentErr, contentCalls := clientErr, 1
			if options.DisableContentRecording {
				contentErr, contentCalls = nil, 0
			}
			type contextKey struct{}
			sink := testutil.NewFakeSink(t)
			clientCalls := 0
			rec := newRecorder(sink.Logger(), noop.NewTracerProvider().Tracer(t.Name()), apiKeyID, options, func(ctx context.Context) (DRPCClient, error) {
				clientCalls++
				require.Equal(t, "record context", ctx.Value(contextKey{}))
				return nil, clientErr
			})
			require.Zero(t, clientCalls, "creating a recorder must not acquire a client")
			ctx := context.WithValue(t.Context(), contextKey{}, "record context")
			require.ErrorIs(t, rec.RecordInterception(ctx, &recorder.InterceptionRecord{ID: id, StartedAt: now}), clientErr)
			require.ErrorIs(t, rec.RecordInterceptionEnded(ctx, &recorder.InterceptionRecordEnded{ID: id, EndedAt: now}), clientErr)
			require.ErrorIs(t, rec.RecordTokenUsage(ctx, &recorder.TokenUsageRecord{InterceptionID: id, CreatedAt: now, Input: 1}), clientErr)
			require.ErrorIs(t, rec.RecordPromptUsage(ctx, &recorder.PromptUsageRecord{InterceptionID: id, CreatedAt: now, Prompt: "prompt"}), contentErr)
			require.ErrorIs(t, rec.RecordToolUsage(ctx, &recorder.ToolUsageRecord{InterceptionID: id, CreatedAt: now, Tool: "tool"}), contentErr)
			require.ErrorIs(t, rec.RecordModelThought(ctx, &recorder.ModelThoughtRecord{InterceptionID: id, CreatedAt: now, Content: "thought"}), contentErr)
			require.Equal(t, 3+3*contentCalls, clientCalls, "each forwarded record acquires its own client")
			logs := sink.Entries(func(entry slog.SinkEntry) bool { return entry.Message == recorder.InterceptionLogMarker })
			if options.StructuredLogging {
				require.Len(t, logs, 6, "logging must precede content filtering")
			} else {
				require.Empty(t, logs)
			}
			require.ErrorIs(t, rec.RecordPromptUsage(ctx, &recorder.PromptUsageRecord{}), recorder.ErrInvalidRecord, "validation must precede content filtering")
			require.Equal(t, 3+3*contentCalls, clientCalls)
		})
	}
}

// blockingHandler returns a handler that signals started, then blocks until
// release is closed or the request context is canceled.
func blockingHandler() (h http.Handler, started <-chan struct{}, release func()) {
	startedCh := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedCh <- struct{}{}
		select {
		case <-releaseCh:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	return h, startedCh, func() { close(releaseCh) }
}

// serveAsync invokes an acquired handler on a passthrough route and reports
// the response status.
func serveAsync(handler http.Handler) <-chan int {
	done := make(chan int, 1)
	go func() {
		defer close(done)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/openai/v1/models", nil))
		done <- rec.Code
	}()
	return done
}

type proxyTestConn struct {
	drpc.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *proxyTestConn) Closed() <-chan struct{} { return c.closed }
func (c *proxyTestConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// DRPC stays available, including reconnects, until requests finish or shutdown
// is canceled. An in-progress reload must not block reconnecting or closing DRPC.
func TestServerProxy_Shutdown(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		cancel     bool
		wantStatus int
		wantErr    error
	}{
		{name: "Graceful", wantStatus: http.StatusNoContent},
		// Canceling the request aborts the upstream round trip.
		{name: "Canceled", cancel: true, wantStatus: http.StatusBadGateway, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			upstreamHandler, started, release := blockingHandler()
			tracker := newProxyTestServer(t, upstreamHandler)
			release = sync.OnceFunc(release)
			t.Cleanup(release)
			testCtx := testutil.Context(t, testutil.WaitShort)
			first := &Client{Conn: &proxyTestConn{closed: make(chan struct{})}}
			second := &Client{Conn: &proxyTestConn{closed: make(chan struct{})}}
			reconnected := make(chan struct{})
			dials := 0
			tracker.clientCh = make(chan DRPCClient)
			tracker.clientDialer = func(context.Context) (DRPCClient, error) {
				dials++
				switch dials {
				case 1:
					return first, nil
				case 2:
					return nil, xerrors.New("transient dial failure")
				default:
					close(reconnected)
					return second, nil
				}
			}
			tracker.wg.Add(1)
			go tracker.connect()
			client, err := tracker.Client(testCtx)
			require.NoError(t, err)
			require.Same(t, first, client)

			handler, err := tracker.GetRequestHandler(testCtx, Request{})
			require.NoError(t, err)
			inflightReqDone := serveAsync(handler)
			testutil.RequireReceive(testCtx, t, started)

			shutdownCtx, cancel := context.WithCancel(testCtx)
			defer cancel()
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- tracker.Shutdown(shutdownCtx) }()
			require.Eventually(t, tracker.shuttingDown.Load, testutil.WaitShort, testutil.IntervalFast)

			refused, err := tracker.GetRequestHandler(testCtx, Request{})
			require.NoError(t, err)
			rec := httptest.NewRecorder()
			refused.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, "AI Gateway is shutting down\n", rec.Body.String())
			select {
			case err := <-shutdownDone:
				t.Fatalf("shutdown returned before the request finished: %v", err)
			default:
			}

			client, err = tracker.Client(testCtx)
			require.NoError(t, err)
			require.Same(t, first, client, "DRPC must remain available during drainage")
			tracker.backendMu.Lock()
			defer tracker.backendMu.Unlock()
			require.NoError(t, first.DRPCConn().Close())
			testutil.TryReceive(testCtx, t, reconnected)
			client, err = tracker.Client(testCtx)
			require.NoError(t, err)
			require.Same(t, second, client, "DRPC must reconnect during drainage")
			require.NoError(t, tracker.lifecycleCtx.Err())

			if tc.cancel {
				cancel()
			} else {
				release()
			}
			require.Equal(t, tc.wantStatus, testutil.RequireReceive(testCtx, t, inflightReqDone))
			require.ErrorIs(t, testutil.RequireReceive(testCtx, t, shutdownDone), tc.wantErr)
			require.ErrorIs(t, context.Cause(tracker.lifecycleCtx), ErrShutdown)
			testutil.TryReceive(testCtx, t, second.DRPCConn().Closed())
			require.False(t, tracker.Ready())
		})
	}
}

func TestServerProxy_CancelsWithLifecycle(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tracker, err := New(ctx, func(ctx context.Context) (DRPCClient, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, slogtest.Make(t, nil), nil, codersdk.Experiments{codersdk.ExperimentAIGatewayReverseProxy}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tracker.Close() })
	handler, started, release := blockingHandler()
	tracker.backend.Store(&backend{router: newProxyTestRouter(t, handler, tracker.inflight)})
	t.Cleanup(release)
	acquired, err := tracker.GetRequestHandler(t.Context(), Request{})
	require.NoError(t, err)
	done := serveAsync(acquired)
	testCtx := testutil.Context(t, testutil.WaitShort)
	testutil.RequireReceive(testCtx, t, started)

	cancel()
	require.Equal(t, http.StatusBadGateway, testutil.RequireReceive(testCtx, t, done))
	require.ErrorIs(t, tracker.Err(), context.Canceled)
	refused, err := tracker.GetRequestHandler(testCtx, Request{})
	require.NoError(t, err)
	for _, h := range []http.Handler{acquired, refused} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "AI Gateway is shutting down\n", rec.Body.String())
	}
}

// blockingWriter blocks the first response write until release is closed,
// ignoring request cancellation.
type blockingWriter struct {
	*httptest.ResponseRecorder
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.ResponseRecorder.Write(p)
}

// TestServerProxy_DeadlineDoesNotWaitForHandler ensures shutdown returns
// even if an admitted handler cannot observe cancellation, such as a blocked
// response write.
func TestServerProxy_DeadlineDoesNotWaitForHandler(t *testing.T) {
	t.Parallel()

	upstreamCanceled := make(chan struct{})
	upstreamDone := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	tracker := newProxyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("chunk"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(upstreamCanceled)
		case <-upstreamDone:
		}
	}))
	handler, err := tracker.GetRequestHandler(t.Context(), Request{})
	require.NoError(t, err)
	writer := &blockingWriter{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: release}
	inflightReqDone := make(chan int, 1)
	go func() {
		defer close(inflightReqDone)
		handler.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/openai/v1/models", nil))
		inflightReqDone <- writer.Code
	}()
	t.Cleanup(func() {
		unblock()
		close(upstreamDone)
		select {
		case <-inflightReqDone:
		case <-time.After(testutil.WaitLong):
			t.Error("handler did not finish after release")
		}
	})
	testCtx := testutil.Context(t, testutil.WaitLong)
	testutil.TryReceive(testCtx, t, writer.started)

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- tracker.Shutdown(ctx) }()
	require.ErrorIs(t, testutil.RequireReceive(testCtx, t, shutdownDone), context.DeadlineExceeded)
	// Forced shutdown cancels the request, which aborts the upstream connection.
	testutil.TryReceive(testCtx, t, upstreamCanceled)

	select {
	case <-inflightReqDone:
		t.Fatal("handler returned before release")
	default:
	}

	// Release the test handler so incorrect re-admission fails rather than hangs.
	unblock()
	require.Equal(t, http.StatusOK, testutil.RequireReceive(testCtx, t, inflightReqDone))
	refused, err := tracker.GetRequestHandler(testCtx, Request{})
	require.NoError(t, err)
	for _, h := range []http.Handler{handler, refused} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "AI Gateway is shutting down\n", rec.Body.String())
	}
}

// TestServerProxy_RefusesAfterShutdown asserts no request is admitted once
// shutdown has begun.
func TestServerProxy_RefusesAfterShutdown(t *testing.T) {
	t.Parallel()

	tracker := newProxyTestServer(t, http.NotFoundHandler())
	acquired, err := tracker.GetRequestHandler(t.Context(), Request{})
	require.NoError(t, err)
	require.NoError(t, tracker.Shutdown(t.Context()))
	refused, err := tracker.GetRequestHandler(t.Context(), Request{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		handler http.Handler
	}{
		{name: "Retained", handler: acquired},
		{name: "AcquiredAfterShutdown", handler: refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			tc.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", nil))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, "AI Gateway is shutting down\n", rec.Body.String())
		})
	}
}

// newProxyTestRouter creates an OpenAI proxy router admitting requests through
// gate and forwarding to an httptest upstream served by upstreamHandler.
func newProxyTestRouter(t *testing.T, upstreamHandler http.Handler, gate *aibridge.InflightGate) *proxy.Router {
	t.Helper()
	upstream := httptest.NewServer(upstreamHandler)
	t.Cleanup(upstream.Close)
	router, err := proxy.NewRouter([]aibridge.Provider{aibridge.NewOpenAIProvider(config.OpenAI{BaseURL: upstream.URL})},
		slogtest.Make(t, nil), nil, noop.NewTracerProvider().Tracer(t.Name()), gate)
	require.NoError(t, err)
	t.Cleanup(router.CloseIdleConnections)
	return router
}

// newProxyTestServer publishes a proxy router sharing the server's admission
// gate. It registers cleanup but leaves DRPC setup and the connection loop to
// the test.
func newProxyTestServer(t *testing.T, upstreamHandler http.Handler) *Server {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	s := &Server{
		lifecycleCtx: ctx,
		cancelFn:     cancel,
		logger:       slogtest.Make(t, nil),
		tracer:       noop.NewTracerProvider().Tracer(t.Name()),
		inflight:     aibridge.NewInflightGate(slogtest.Make(t, nil)),
	}
	s.backend.Store(&backend{router: newProxyTestRouter(t, upstreamHandler, s.inflight)})
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// blockingRecorder blocks lifecycle records until release is closed or the
// record context ends, reporting the context of the first record.
type blockingRecorder struct {
	recorder.Recorder
	started chan<- context.Context
	release <-chan struct{}
}

func (r *blockingRecorder) RecordInterception(ctx context.Context, _ *recorder.InterceptionRecord) error {
	r.started <- ctx
	return r.wait(ctx)
}

func (r *blockingRecorder) RecordInterceptionEnded(ctx context.Context, _ *recorder.InterceptionRecordEnded) error {
	return r.wait(ctx)
}

func (r *blockingRecorder) wait(ctx context.Context) error {
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// failingWriter fails response body writes, which aborts forwarding.
type failingWriter struct{ *httptest.ResponseRecorder }

func (failingWriter) Write([]byte) (int, error) { return 0, xerrors.New("client write failed") }

// TestServerProxy_ProcessingOutlivesHandler asserts processing tasks do not
// delay the response, keep request values, and hold shutdown drainage while
// DRPC stays live. Forced shutdown cancels their context.
func TestServerProxy_ProcessingOutlivesHandler(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	for _, tc := range []struct {
		name   string
		panics bool
		forced bool
	}{
		{name: "Graceful"},
		{name: "Panic", panics: true},
		{name: "Forced", forced: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			testCtx := testutil.Context(t, testutil.WaitShort)
			taskStarted := make(chan context.Context, 1)
			releaseTask := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseTask) })
			t.Cleanup(release)
			tracker := newProxyTestServer(t, http.NotFoundHandler())
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = w.Write([]byte("{}"))
			}))
			t.Cleanup(upstream.Close)
			// Processing failures are logged by the router, which the server builds
			// with its own logger and admission gate.
			sink := testutil.NewFakeSink(t)
			tracker.logger = sink.Logger()
			require.NoError(t, tracker.ReplaceProviders(testCtx, []aibridge.Provider{aibridge.NewOpenAIProvider(config.OpenAI{BaseURL: upstream.URL})}))
			router := tracker.backend.Load().router
			blocking := &blockingRecorder{started: taskStarted, release: releaseTask}

			// The handler returns while its processing is still blocked.
			reqCtx, cancelReq := context.WithCancel(aibcontext.AsActor(context.WithValue(testCtx, ctxKey{}, "attribution"), "actor", "", nil))
			rec := httptest.NewRecorder()
			var w http.ResponseWriter = rec
			if tc.panics {
				w = failingWriter{rec}
			}
			served := make(chan any, 1)
			go func() {
				defer func() { served <- recover() }()
				req := httptest.NewRequest(http.MethodPost, "/openai/v1/chat/completions", strings.NewReader("{}")).WithContext(reqCtx)
				req.Header.Set("Authorization", "Bearer request-secret")
				router.Serve(w, req, blocking)
			}()
			if panicValue := testutil.RequireReceive(testCtx, t, served); tc.panics {
				require.Equal(t, http.ErrAbortHandler, panicValue)
			} else {
				require.Nil(t, panicValue)
				require.Equal(t, http.StatusOK, rec.Code)
			}
			cancelReq()
			processCtx := testutil.RequireReceive(testCtx, t, taskStarted)
			require.Equal(t, "attribution", processCtx.Value(ctxKey{}))
			require.NoError(t, processCtx.Err(), "request cancellation must not cancel processing")

			shutdownCtx, cancelShutdown := context.WithCancel(testCtx)
			defer cancelShutdown()
			shutdownDone := make(chan error, 1)
			go func() { shutdownDone <- tracker.Shutdown(shutdownCtx) }()
			require.Eventually(t, tracker.shuttingDown.Load, testutil.WaitShort, testutil.IntervalFast)
			select {
			case err := <-shutdownDone:
				t.Fatalf("shutdown returned before processing finished: %v", err)
			default:
			}
			require.NoError(t, tracker.lifecycleCtx.Err(), "DRPC must remain available while processing drains")

			if tc.forced {
				cancelShutdown()
				require.ErrorIs(t, testutil.RequireReceive(testCtx, t, shutdownDone), context.Canceled)
				testutil.TryReceive(testCtx, t, processCtx.Done())
				// The canceled task's error is logged with bounded request fields;
				// correlation fields may accompany them, but never credentials.
				var entry slog.SinkEntry
				require.Eventually(t, func() bool {
					entries := sink.Entries(func(e slog.SinkEntry) bool { return e.Message == "proxy request processing failed" })
					if len(entries) != 1 {
						return false
					}
					entry = entries[0]
					return true
				}, testutil.WaitShort, testutil.IntervalFast)
				names := make([]string, 0, len(entry.Fields))
				for _, field := range entry.Fields {
					names = append(names, field.Name)
					require.NotContains(t, fmt.Sprint(field.Value), "request-secret")
				}
				require.Subset(t, names, []string{"method", "path", "error"})
			} else {
				release()
				require.NoError(t, testutil.RequireReceive(testCtx, t, shutdownDone))
			}
			require.ErrorIs(t, context.Cause(tracker.lifecycleCtx), ErrShutdown)
		})
	}
}

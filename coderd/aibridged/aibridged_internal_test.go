package aibridged

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
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
	"github.com/coder/coder/v2/coderd/aibridged/proto"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/testutil"
)

func TestNewRecorder(t *testing.T) {
	t.Parallel()
	clientErr := xerrors.New("client unavailable")
	for _, tc := range []struct {
		structuredLogging       bool
		disableContentRecording bool
		wantClientCalls         int
		wantLogs                int
		wantContentErr          error
	}{
		{
			wantClientCalls: 6,
			wantContentErr:  clientErr,
		},
		{
			structuredLogging: true,
			wantClientCalls:   6,
			wantLogs:          6,
			wantContentErr:    clientErr,
		},
		{
			disableContentRecording: true,
			wantClientCalls:         3,
		},
		{
			structuredLogging:       true,
			disableContentRecording: true,
			wantClientCalls:         3,
			wantLogs:                6,
		},
	} {
		t.Run(fmt.Sprintf("Structured=%t/DisableContent=%t", tc.structuredLogging, tc.disableContentRecording), func(t *testing.T) {
			t.Parallel()
			request := newIdentityRequest("policy")
			now := time.Now().UTC()
			sink := testutil.NewFakeSink(t)
			clientCalls := 0
			rec := newRecorder(sink.Logger(), noop.NewTracerProvider().Tracer(t.Name()), tc.structuredLogging, tc.disableContentRecording, func(ctx context.Context) (DRPCClient, error) {
				clientCalls++
				require.Equal(t, request.interceptionID, ctx.Value(recordContextKey{}), "the client must be acquired with the record call's context")
				return nil, clientErr
			})
			require.Zero(t, clientCalls, "creating a recorder must not acquire a client")
			ctx := request.withActor(t.Context())
			id := request.interceptionID
			require.ErrorIs(t, rec.RecordInterception(ctx, request.record(now)), clientErr)
			require.ErrorIs(t, rec.RecordInterceptionEnded(ctx, &recorder.InterceptionRecordEnded{ID: id, EndedAt: now}), clientErr)
			require.ErrorIs(t, rec.RecordTokenUsage(ctx, &recorder.TokenUsageRecord{InterceptionID: id, CreatedAt: now, Input: 1}), clientErr)
			require.ErrorIs(t, rec.RecordPromptUsage(ctx, &recorder.PromptUsageRecord{InterceptionID: id, CreatedAt: now, Prompt: "prompt"}), tc.wantContentErr)
			require.ErrorIs(t, rec.RecordToolUsage(ctx, &recorder.ToolUsageRecord{InterceptionID: id, CreatedAt: now, Tool: "tool"}), tc.wantContentErr)
			require.ErrorIs(t, rec.RecordModelThought(ctx, &recorder.ModelThoughtRecord{InterceptionID: id, CreatedAt: now, Content: "thought"}), tc.wantContentErr)
			require.Equal(t, tc.wantClientCalls, clientCalls, "each forwarded record acquires its own client")
			logs := structuredRecordLogs(sink)
			require.Len(t, logs, tc.wantLogs, "logging must precede content filtering")
			for _, fields := range logs {
				if fields["record_type"] == recorder.RecordTypeInterceptionStart {
					require.Equal(t, request.actor.APIKeyID, fields["api_key_id"])
				}
			}
			require.ErrorIs(t, rec.RecordPromptUsage(ctx, &recorder.PromptUsageRecord{}), recorder.ErrInvalidRecord, "validation must precede content filtering")
			require.Equal(t, tc.wantClientCalls, clientCalls)
		})
	}

	// One recorder serves every request, so each interception start must carry
	// its own request's identity, including under concurrency.
	t.Run("SharedAcrossRequests", func(t *testing.T) {
		t.Parallel()
		sink := testutil.NewFakeSink(t)
		client := &recordingClient{}
		rec := newRecorder(sink.Logger(), noop.NewTracerProvider().Tracer(t.Name()), true, false, client.acquire)
		requests := make([]identityRequest, 24)
		for i := range requests {
			requests[i] = newIdentityRequest(fmt.Sprintf("user-%d", i))
		}
		const sequential = 4
		for _, request := range requests[:sequential] {
			require.NoError(t, rec.RecordInterception(request.withActor(t.Context()), request.record(time.Now().UTC())))
		}
		start := make(chan struct{})
		errs := make(chan error, len(requests)-sequential)
		var wg sync.WaitGroup
		for _, request := range requests[sequential:] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs <- rec.RecordInterception(request.withActor(t.Context()), request.record(time.Now().UTC()))
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		requireRecordedIdentities(t, sink, client, requests...)
	})

	t.Run("MissingIdentity", func(t *testing.T) {
		t.Parallel()
		now := time.Now().UTC()
		sink := testutil.NewFakeSink(t)
		client := &recordingClient{}
		rec := newRecorder(sink.Logger(), noop.NewTracerProvider().Tracer(t.Name()), true, false, client.acquire)
		valid := newIdentityRequest("valid")
		require.NoError(t, rec.RecordInterception(valid.withActor(t.Context()), valid.record(now)))

		noActor := newIdentityRequest("no-actor")
		emptyKey := newIdentityRequest("empty-key")
		emptyKey.actor.APIKeyID = ""
		for name, ctx := range map[string]context.Context{
			"NoActor":       context.WithValue(t.Context(), recordContextKey{}, noActor.interceptionID),
			"EmptyAPIKeyID": emptyKey.withActor(t.Context()),
		} {
			// A previously recorded identity must not be reused as a fallback.
			err := rec.RecordInterception(ctx, &recorder.InterceptionRecord{ID: uuid.NewString(), InitiatorID: uuid.NewString(), StartedAt: now})
			require.ErrorIs(t, err, aibcontext.ErrMissingAPIKeyID, name)
		}
		requireRecordedIdentities(t, sink, client, valid)

		// Only interception starts carry an API-key ID; other records do not
		// require an identity.
		require.NoError(t, rec.RecordInterceptionEnded(t.Context(), &recorder.InterceptionRecordEnded{ID: valid.interceptionID, EndedAt: now}))
		snapshot := client.snapshot()
		require.Len(t, snapshot.ended, 1)
		require.Equal(t, valid.interceptionID, snapshot.ended[0].GetId())
	})

	t.Run("DetachedContext", func(t *testing.T) {
		t.Parallel()
		sink := testutil.NewFakeSink(t)
		client := &recordingClient{}
		rec := newRecorder(sink.Logger(), noop.NewTracerProvider().Tracer(t.Name()), true, false, client.acquire)
		request := newIdentityRequest("detached")
		ctx, cancel := context.WithCancel(request.withActor(t.Context()))
		cancel()
		require.NoError(t, rec.RecordInterception(context.WithoutCancel(ctx), request.record(time.Now().UTC())))
		requireRecordedIdentities(t, sink, client, request)
		require.NoError(t, client.snapshot().acquisitions[0].Err(), "a detached record context must not carry the request's cancellation")
	})
}

type recordContextKey struct{}

// identityRequest is one authenticated request's identity and interception.
type identityRequest struct {
	actor          aibridge.Actor
	interceptionID string
}

func newIdentityRequest(username string) identityRequest {
	return identityRequest{
		actor: aibridge.Actor{
			ID:       uuid.New(),
			APIKeyID: uuid.NewString(),
			Username: username,
			Email:    username + "@example.test",
		},
		interceptionID: uuid.NewString(),
	}
}

// withActor attaches the actor and tags the context with the interception ID,
// so client acquisitions can be traced back to their record call.
func (r identityRequest) withActor(parent context.Context) context.Context {
	return aibridge.AsActor(context.WithValue(parent, recordContextKey{}, r.interceptionID), r.actor)
}

func (r identityRequest) record(startedAt time.Time) *recorder.InterceptionRecord {
	return &recorder.InterceptionRecord{ID: r.interceptionID, InitiatorID: r.actor.ID.String(), StartedAt: startedAt}
}

// requireRecordedIdentities asserts that exactly the given interception starts
// were acquired, sent and logged, each with its own request's identity.
func requireRecordedIdentities(t *testing.T, sink *testutil.FakeSink, client *recordingClient, requests ...identityRequest) {
	t.Helper()
	want := make(map[string]aibridge.Actor, len(requests))
	for _, request := range requests {
		want[request.interceptionID] = request.actor
	}
	snapshot := client.snapshot()

	require.Len(t, snapshot.acquisitions, len(want))
	for _, ctx := range snapshot.acquisitions {
		interceptionID, ok := ctx.Value(recordContextKey{}).(string)
		require.True(t, ok, "the client must be acquired with the record call's context")
		actor := aibcontext.ActorFromContext(ctx)
		require.NotNil(t, actor)
		require.Equal(t, want[interceptionID], *actor)
	}

	require.Len(t, snapshot.interceptions, len(want))
	for _, in := range snapshot.interceptions {
		actor, ok := want[in.GetId()]
		require.True(t, ok, "unexpected interception %q", in.GetId())
		require.Equal(t, actor.APIKeyID, in.GetApiKeyId())
		require.Equal(t, actor.ID.String(), in.GetInitiatorId())
	}

	var starts int
	for _, fields := range structuredRecordLogs(sink) {
		if fields["record_type"] != recorder.RecordTypeInterceptionStart {
			continue
		}
		starts++
		interceptionID, _ := fields["interception_id"].(string)
		actor, ok := want[interceptionID]
		require.True(t, ok, "unexpected interception log %q", interceptionID)
		require.Equal(t, actor.APIKeyID, fields["api_key_id"])
		require.Equal(t, actor.ID.String(), fields["initiator_id"])
	}
	require.Equal(t, len(want), starts)
}

// structuredRecordLogs returns the fields of each structured record log.
func structuredRecordLogs(sink *testutil.FakeSink) []map[string]any {
	entries := sink.Entries(func(entry slog.SinkEntry) bool { return entry.Message == recorder.InterceptionLogMarker })
	logs := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		fields := make(map[string]any, len(entry.Fields))
		for _, field := range entry.Fields {
			fields[field.Name] = field.Value
		}
		logs = append(logs, fields)
	}
	return logs
}

// recordingClient captures client acquisitions and interception RPCs. It is
// safe for concurrent use, and its nil embedded client makes any other call
// fail loudly.
type recordingClient struct {
	DRPCClient
	mu            sync.Mutex
	acquisitions  []context.Context
	interceptions []*proto.RecordInterceptionRequest
	ended         []*proto.RecordInterceptionEndedRequest
}

type recordingClientSnapshot struct {
	acquisitions  []context.Context
	interceptions []*proto.RecordInterceptionRequest
	ended         []*proto.RecordInterceptionEndedRequest
}

func (c *recordingClient) acquire(ctx context.Context) (DRPCClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acquisitions = append(c.acquisitions, ctx)
	return c, nil
}

func (c *recordingClient) snapshot() recordingClientSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return recordingClientSnapshot{
		acquisitions:  slices.Clone(c.acquisitions),
		interceptions: slices.Clone(c.interceptions),
		ended:         slices.Clone(c.ended),
	}
}

func (c *recordingClient) RecordInterception(_ context.Context, in *proto.RecordInterceptionRequest) (*proto.RecordInterceptionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interceptions = append(c.interceptions, in)
	return &proto.RecordInterceptionResponse{}, nil
}

func (c *recordingClient) RecordInterceptionEnded(_ context.Context, in *proto.RecordInterceptionEndedRequest) (*proto.RecordInterceptionEndedResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ended = append(c.ended, in)
	return &proto.RecordInterceptionEndedResponse{}, nil
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

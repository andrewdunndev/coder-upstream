package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/coder/coder/v2/aibridge/circuitbreaker"
	"github.com/coder/coder/v2/aibridge/client"
	"github.com/coder/coder/v2/aibridge/config"
	aibcontext "github.com/coder/coder/v2/aibridge/context"
	"github.com/coder/coder/v2/aibridge/credential"
	"github.com/coder/coder/v2/aibridge/keypool"
	"github.com/coder/coder/v2/aibridge/metrics"
	"github.com/coder/coder/v2/aibridge/provider"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
	"github.com/coder/coder/v2/aibridge/utils"
	codertestutil "github.com/coder/coder/v2/testutil"
	"github.com/coder/quartz"
)

// Embedding a nil Recorder makes any unexpected content recording fail loudly.
type lifecycleRecorder struct {
	recorder.Recorder
	starts           []recorder.InterceptionRecord
	ends             []recorder.InterceptionRecordEnded
	startErr, endErr error
	onStart, onEnd   func(context.Context)
	initDone         sync.Once
	doneCh           chan struct{}
}

func (r *lifecycleRecorder) done() <-chan struct{} {
	r.initDone.Do(func() { r.doneCh = make(chan struct{}) })
	return r.doneCh
}

func (r *lifecycleRecorder) RecordInterception(ctx context.Context, record *recorder.InterceptionRecord) error {
	r.starts = append(r.starts, *record)
	if r.onStart != nil {
		r.onStart(ctx)
	}
	if r.startErr != nil {
		r.done()
		close(r.doneCh)
	}
	return r.startErr
}

func (r *lifecycleRecorder) RecordInterceptionEnded(ctx context.Context, record *recorder.InterceptionRecordEnded) error {
	r.done()
	defer close(r.doneCh)
	r.ends = append(r.ends, *record)
	if r.onEnd != nil {
		r.onEnd(ctx)
	}
	return r.endErr
}

// serveForwardingRequest applies the router's body limit before serving.
func serveForwardingRequest(h *forwardingHandler, w http.ResponseWriter, r *http.Request, rec recorder.Recorder) {
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(w, r.Body, routing.MaxRequestBodyBytes)
		defer r.Body.Close()
	}
	h.serve(w, r, rec)
}

func TestRouterRecorderDispatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path   string
		status int
	}{
		{path: "/openai/v1/responses", status: http.StatusOK},
		{path: "/openai/v1/%72esponses", status: http.StatusOK},
		{path: "/openai/v1%2Fresponses", status: http.StatusNotFound},
		{path: "/openai/v1/responses/", status: http.StatusNotFound},
		{path: "/openai/v1//responses", status: http.StatusTemporaryRedirect},
		{path: "/unknown", status: http.StatusNotFound},
		{path: "*", status: http.StatusBadRequest},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			expected := &struct{ recorder.Recorder }{}
			called := false
			router := &Router{fallback: http.NotFoundHandler(), routes: []route{{
				pattern: "/openai/v1/responses",
				serve: func(w http.ResponseWriter, r *http.Request, rec recorder.Recorder) {
					called = true
					require.Same(t, expected, rec)
					require.Equal(t, "/openai/v1/responses", r.Pattern)
					w.WriteHeader(http.StatusOK)
				},
			}}}
			req := httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
			if tc.path == "*" {
				req.URL.Path, req.RequestURI = tc.path, tc.path
			} else {
				req = httptest.NewRequest(http.MethodPost, tc.path, nil)
			}
			response := httptest.NewRecorder()
			router.Serve(response, req, expected)
			require.Equal(t, tc.status, response.Code)
			require.Equal(t, tc.status == http.StatusOK, called)
		})
	}
}

func TestRouterRecordedDispatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path   string
		status int
	}{
		{path: "/openai/v1/responses", status: http.StatusOK},
		{path: "/openai/v1/%72esponses", status: http.StatusOK},
		{path: "/openai/v1%2Fresponses", status: http.StatusNotFound},
		{path: "/openai/v1/responses/", status: http.StatusNotFound},
		{path: "/openai/v1//responses", status: http.StatusTemporaryRedirect},
		{path: "/unknown", status: http.StatusNotFound},
		{path: "*", status: http.StatusBadRequest},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			rec := &lifecycleRecorder{}
			h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
			h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, http.StatusOK, tc.status, "redirects and routing errors must not forward")
				require.Equal(t, "/openai/v1/responses", r.Pattern)
				require.Equal(t, "/responses", r.URL.Path)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody}, nil
			})
			router := &Router{fallback: http.NotFoundHandler(), routes: []route{{pattern: "/openai/v1/responses", serve: h.serve}}}
			req := recordedRequest(t, nil)
			if tc.path == "*" {
				req.URL.Path, req.RequestURI = tc.path, tc.path
			} else {
				urlRequest := httptest.NewRequest(http.MethodPost, tc.path, nil)
				req.URL, req.RequestURI = urlRequest.URL, urlRequest.RequestURI
			}
			response := httptest.NewRecorder()
			router.Serve(response, req, rec)
			require.Equal(t, tc.status, response.Code)
			if tc.status == http.StatusOK {
				codertestutil.TryReceive(codertestutil.Context(t, codertestutil.WaitLong), t, rec.done())
				require.Len(t, rec.starts, 1)
				require.Len(t, rec.ends, 1)
			} else {
				require.Empty(t, rec.starts)
				require.Empty(t, rec.ends)
			}
		})
	}
}

func recordedRequest(t *testing.T, body io.Reader) *http.Request {
	t.Helper()
	ctx := aibcontext.AsActor(t.Context(), t.Name(), "actor@example.test", recorder.Metadata{"team": "engineering"})
	req := httptest.NewRequest(http.MethodPost, "/openai/v1/chat/completions", body).WithContext(ctx)
	req.Pattern = "/openai/v1/chat/completions"
	req.Header.Set("Authorization", "Bearer user-secret-key")
	return req
}

func TestForwardingHandlerRejectsRequest(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"MissingActor", "MissingRecorder", "WebSocket", "Firewall", "DeclaredOversize", "MissingCredential", "UnsupportedSigning"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var rec recorder.Recorder = &struct{ recorder.Recorder }{}
			body := &unreadBody{}
			req := recordedRequest(t, body)
			var prov provider.Provider = provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"})
			status := http.StatusBadRequest
			switch name {
			case "MissingActor":
				req = req.WithContext(t.Context())
			case "MissingRecorder":
				rec = nil
				status = http.StatusInternalServerError
			case "WebSocket":
				req.Method = http.MethodGet
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "websocket")
				status = http.StatusNotImplemented
			case "Firewall":
				req.Header.Set("X-Coder-Agent-Firewall-Session-Id", "invalid")
			case "DeclaredOversize":
				req.ContentLength = routing.MaxRequestBodyBytes + 1
				status = http.StatusRequestEntityTooLarge
			case "MissingCredential":
				req.Header.Del("Authorization")
				status = http.StatusBadGateway
			case "UnsupportedSigning":
				var err error
				prov, err = provider.NewAnthropic(t.Context(), config.Anthropic{BaseURL: "https://upstream.example.test"}, nil, &config.AWSClaudePlatform{Region: "us-west-2", WorkspaceID: "wrkspc_test"})
				require.NoError(t, err)
				req.Header.Del("Authorization")
				status = http.StatusNotImplemented
			}
			h := &forwardingHandler{provider: prov}
			response := httptest.NewRecorder()
			record, cred := h.checkRequest(response, req, rec)
			require.Nil(t, record)
			require.Nil(t, cred)
			require.Equal(t, status, response.Code)
			require.False(t, body.read, "preflight must not read the request body")
			require.Equal(t, name == "DeclaredOversize", body.closed)
		})
	}
}

func TestForwardingHandlerRequestMetadata(t *testing.T) {
	t.Parallel()
	for _, pooled := range []bool{false, true} {
		name := "BYOK"
		if pooled {
			name = "Pool"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body := &unreadBody{}
			req := recordedRequest(t, body)
			req.Header.Set("User-Agent", "claude-code/1.0")
			req.Header.Set("X-Coder-Agent-Firewall-Session-Id", "e5f6a7b8-1234-5678-9abc-def012345678")
			req.Header.Set("X-Coder-Agent-Firewall-Sequence-Number", "42")
			var pool *keypool.Pool
			kind, hint := credential.KindBYOK, utils.MaskSecret("user-secret-key")
			if pooled {
				var err error
				pool, err = keypool.New("openai", []string{"pool-key"}, quartz.NewMock(t), nil)
				require.NoError(t, err)
				req.Header.Del("Authorization")
				kind, hint = credential.KindCentralized, credential.HintFailoverKey
			}
			h := &forwardingHandler{provider: provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test", KeyPool: pool})}
			response := httptest.NewRecorder()
			record, cred := h.checkRequest(response, req, &struct{ recorder.Recorder }{})
			require.NotNil(t, record)
			require.NotNil(t, cred)
			require.Equal(t, kind, cred.Kind())
			require.Equal(t, kind, record.CredentialKind)
			require.Equal(t, hint, record.CredentialHint)
			require.Equal(t, t.Name(), record.InitiatorID)
			require.Equal(t, recorder.Metadata{"team": "engineering"}, record.Metadata)
			require.Equal(t, "openai", record.ProviderName)
			require.Equal(t, string(client.ClaudeCode), record.Client)
			require.Equal(t, req.UserAgent(), record.UserAgent)
			require.Equal(t, new("e5f6a7b8-1234-5678-9abc-def012345678"), record.AgentFirewallSessionID)
			require.Equal(t, new(int32(42)), record.AgentFirewallSequenceNumber)
			require.Empty(t, record.Model)
			require.Empty(t, response.Body.String())
			require.False(t, body.read)
			require.False(t, body.closed)
		})
	}
}

func TestForwardingHandlerLifecycle(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Success", "BytesWithEOF", "StartFailure", "EndFailure", "TransportFailure", "Upstream503", "ReadFailure", "WriteFailure", "FlushFailure", "Panic", "Cancellation", "PoolFailure", "CircuitOpen"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &lifecycleRecorder{onEnd: func(ctx context.Context) {
				require.NoError(t, ctx.Err(), "finalization must detach request cancellation")
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Positive(t, time.Until(deadline))
				require.LessOrEqual(t, time.Until(deadline), recorder.DefaultAsyncTimeout)
			}}
			body := &trackedBody{Reader: strings.NewReader("chunk")}
			input := &trackedBody{Reader: strings.NewReader("not JSON")}
			req := recordedRequest(t, input)
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			response := httptest.NewRecorder()
			var writer http.ResponseWriter = response
			status, wantStatus := http.StatusOK, http.StatusOK
			var transportErr error
			var wantPanic any
			switch name {
			case "BytesWithEOF":
				body.Reader = readerFunc(func(p []byte) (int, error) { return copy(p, "chunk"), io.EOF })
			case "StartFailure":
				rec.startErr = xerrors.New("start unavailable")
			case "EndFailure":
				rec.endErr = xerrors.New("end unavailable")
			case "TransportFailure":
				transportErr = io.ErrClosedPipe
				wantStatus = http.StatusBadGateway
			case "Upstream503":
				status = http.StatusServiceUnavailable
				wantStatus = status
			case "ReadFailure":
				body.Reader = io.MultiReader(body.Reader, readerFunc(func([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }))
				wantPanic = http.ErrAbortHandler
			case "WriteFailure":
				writer = &failingWriter{response}
				wantPanic = http.ErrAbortHandler
			case "FlushFailure":
				writer = &failingFlushWriter{response}
				wantPanic = http.ErrAbortHandler
			case "Panic":
				body.Reader = readerFunc(func([]byte) (int, error) { panic("copy panic") })
				wantPanic = "copy panic"
			case "Cancellation":
				body.Reader = io.MultiReader(body.Reader, readerFunc(func([]byte) (int, error) { cancel(); return 0, context.Canceled }))
				wantPanic = http.ErrAbortHandler
			case "PoolFailure":
				transportErr = &keypool.Error{Kind: keypool.ErrorKindRateLimited}
				wantStatus = http.StatusBadGateway
			case "CircuitOpen":
				transportErr = circuitbreaker.ErrCircuitOpen
				wantStatus = http.StatusBadGateway
			}
			m := metrics.NewMetrics(prometheus.NewRegistry())
			h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), m)
			calls := 0
			h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				if transportErr != nil {
					return nil, transportErr
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: body}, nil
			})
			serve := func() { serveForwardingRequest(h, writer, req, rec) }
			if wantPanic != nil {
				require.PanicsWithValue(t, wantPanic, serve)
			} else {
				serve()
				require.Equal(t, wantStatus, response.Code)
			}
			codertestutil.TryReceive(codertestutil.Context(t, codertestutil.WaitLong), t, rec.done())
			require.Equal(t, 1, input.closes)
			require.Equal(t, 1, input.eofs)
			require.Equal(t, 1, calls)
			require.Len(t, rec.starts, 1)
			if name == "StartFailure" {
				require.Equal(t, "chunk", response.Body.String())
				require.Empty(t, rec.ends)
				return
			}
			require.Len(t, rec.ends, 1)
			require.Equal(t, rec.starts[0].ID, rec.ends[0].ID)
			require.False(t, rec.starts[0].StartedAt.IsZero())
			require.False(t, rec.ends[0].EndedAt.Before(rec.starts[0].StartedAt))
			require.Empty(t, rec.starts[0].Model)
			require.Equal(t, credential.KindBYOK, rec.starts[0].CredentialKind)
			require.Equal(t, utils.MaskSecret("user-secret-key"), rec.starts[0].CredentialHint)
			metricStatus := metrics.InterceptionCountStatusFailed
			if name == "Success" || name == "BytesWithEOF" || name == "EndFailure" {
				require.Empty(t, rec.ends[0].ErrorType)
				require.Empty(t, rec.ends[0].ErrorMessage)
				require.Equal(t, "chunk", response.Body.String())
				metricStatus = metrics.InterceptionCountStatusCompleted
			} else {
				require.NotEmpty(t, rec.ends[0].ErrorType)
				require.NotEmpty(t, rec.ends[0].ErrorMessage)
			}
			if name == "Upstream503" {
				require.Equal(t, recorder.ErrorTypeServerError, rec.ends[0].ErrorType)
			}
			if name == "FlushFailure" {
				require.Equal(t, io.ErrClosedPipe.Error(), rec.ends[0].ErrorMessage)
			}
			if transportErr == nil {
				require.Equal(t, 1, body.closes)
			}
			require.Equal(t, 1.0, promtest.ToFloat64(m.InterceptionCount.WithLabelValues("openai", "", metricStatus, "/v1/chat/completions", http.MethodPost, t.Name(), string(client.Unknown))))
			require.Zero(t, promtest.CollectAndCount(m.PassthroughCount))
		})
	}
}

// TestForwardingHandlerRecordsCredentials asserts the recorded request
// metadata and the credential hints before and after key failover.
func TestForwardingHandlerRecordsCredentials(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"OpenAIBYOK", "AnthropicBYOK", "CopilotBYOK", "Pool", "ExhaustedPool"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pooled := name == "Pool" || name == "ExhaustedPool"
			pool, err := keypool.New("openai", []string{"first-secret-key", "second-secret-key"}, quartz.NewMock(t), nil)
			require.NoError(t, err)
			var prov provider.Provider = provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test", KeyPool: pool})
			req := recordedRequest(t, strings.NewReader("body"))
			req.Header.Set("X-Coder-Agent-Firewall-Session-Id", "e5f6a7b8-1234-5678-9abc-def012345678")
			req.Header.Set("X-Coder-Agent-Firewall-Sequence-Number", "42")
			kind, startHint, endHint := credential.KindBYOK, utils.MaskSecret("user-secret-key"), utils.MaskSecret("user-secret-key")
			wantStatus, wantErrType := http.StatusUnauthorized, recorder.ErrorTypeUnauthorized
			switch name {
			case "AnthropicBYOK":
				prov, err = provider.NewAnthropic(t.Context(), config.Anthropic{BaseURL: "https://upstream.example.test", KeyPool: pool}, nil, nil)
				require.NoError(t, err)
				req.Header.Del("Authorization")
				req.Header.Set("X-Api-Key", "anthropic-secret-key")
				startHint, endHint = utils.MaskSecret("anthropic-secret-key"), utils.MaskSecret("anthropic-secret-key")
			case "CopilotBYOK":
				prov = provider.NewCopilot(config.Copilot{BaseURL: "https://upstream.example.test"})
			case "Pool", "ExhaustedPool":
				req.Header.Del("Authorization")
				req.Header.Set("User-Agent", "claude-code/1.0")
				req.Header.Set("X-Claude-Code-Session-Id", "session-one")
				kind, startHint, endHint = credential.KindCentralized, credential.HintFailoverKey, utils.MaskSecret("second-secret-key")
				if name == "Pool" {
					wantStatus, wantErrType = http.StatusOK, ""
				} else {
					wantStatus = http.StatusBadGateway
				}
			}
			h := newTestForwardingHandler(t, prov, nil)
			attempts := 0
			h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return nil, err
				}
				status := http.StatusUnauthorized
				if name == "Pool" && attempts == 2 {
					status = http.StatusOK
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: http.NoBody}, nil
			})
			original := req.Header.Clone()
			rec := &lifecycleRecorder{}
			response := httptest.NewRecorder()
			serveForwardingRequest(h, response, req, rec)
			codertestutil.TryReceive(codertestutil.Context(t, codertestutil.WaitLong), t, rec.done())
			require.Equal(t, wantStatus, response.Code)
			require.Equal(t, original, req.Header)
			require.Len(t, rec.starts, 1)
			require.Len(t, rec.ends, 1)
			start, end := rec.starts[0], rec.ends[0]
			require.Equal(t, start.ID, end.ID)
			require.Equal(t, t.Name(), start.InitiatorID)
			require.Equal(t, recorder.Metadata{"team": "engineering"}, start.Metadata)
			require.Equal(t, new("e5f6a7b8-1234-5678-9abc-def012345678"), start.AgentFirewallSessionID)
			require.Equal(t, new(int32(42)), start.AgentFirewallSequenceNumber)
			require.Empty(t, start.Model)
			require.Equal(t, kind, start.CredentialKind)
			require.Equal(t, startHint, start.CredentialHint)
			require.Equal(t, wantErrType, end.ErrorType)
			if pooled {
				require.Equal(t, 2, attempts)
				require.Equal(t, endHint, end.CredentialHint, "the end record must report the key that served the final attempt")
				require.Equal(t, new("session-one"), start.ClientSessionID)
				require.Equal(t, string(client.ClaudeCode), start.Client)
			} else {
				require.Equal(t, 1, attempts, "BYOK failures must not fall back to pooled credentials")
				require.Equal(t, endHint, end.CredentialHint)
			}
			if name == "ExhaustedPool" {
				require.Contains(t, end.ErrorMessage, "key")
			}
		})
	}
}

func TestForwardingHandlerStreamsAndCancels(t *testing.T) {
	t.Parallel()
	canceled, finalized := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		assert.NoError(t, http.NewResponseController(w).Flush())
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-release:
		}
	}))
	t.Cleanup(upstream.Close)
	h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: upstream.URL}), nil)
	startedRecord := make(chan struct{})
	rec := &lifecycleRecorder{onStart: func(context.Context) { close(startedRecord) }, onEnd: func(context.Context) { close(finalized) }}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := aibcontext.AsActor(r.Context(), t.Name(), "", nil)
		h.serve(w, r.WithContext(ctx), rec)
	}))
	t.Cleanup(gateway.Close)
	ctx, cancel := context.WithCancel(codertestutil.Context(t, codertestutil.WaitLong))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.URL+"/openai/v1/chat/completions", strings.NewReader("request"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer user-secret-key")
	resp, err := gateway.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	first := make([]byte, len("data: first\n\n"))
	_, err = io.ReadFull(resp.Body, first)
	require.NoError(t, err)
	require.Equal(t, "data: first\n\n", string(first))
	codertestutil.TryReceive(ctx, t, startedRecord)
	select {
	case <-finalized:
		t.Fatal("end recording ran before response delivery completed")
	default:
	}
	cancel()
	waitCtx := codertestutil.Context(t, codertestutil.WaitLong)
	codertestutil.TryReceive(waitCtx, t, canceled)
	codertestutil.TryReceive(waitCtx, t, finalized)
	codertestutil.TryReceive(waitCtx, t, rec.done())
	require.Len(t, rec.ends, 1)
	require.NotEmpty(t, rec.ends[0].ErrorType)
}

func TestForwardingHandlerAsyncRecording(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"Start", "End"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ctx := codertestutil.Context(t, codertestutil.WaitShort)
			recording, release := make(chan struct{}), make(chan struct{})
			rec := &lifecycleRecorder{}
			defer func() {
				close(release)
				codertestutil.TryReceive(ctx, t, rec.done())
				require.Len(t, rec.starts, 1)
				require.Len(t, rec.ends, 1)
				require.False(t, rec.ends[0].EndedAt.Before(rec.starts[0].StartedAt))
			}()
			block := func(ctx context.Context) {
				close(recording)
				select {
				case <-release:
				case <-ctx.Done():
					t.Error("recording timed out before the HTTP handler returned")
				}
			}
			if stage == "Start" {
				rec.onStart = block
			} else {
				rec.onEnd = block
			}
			h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
			h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				_, err := io.Copy(io.Discard, r.Body)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("response"))}, nil
			})
			response := httptest.NewRecorder()
			req := recordedRequest(t, strings.NewReader("request"))
			served := make(chan struct{})
			go func() {
				defer close(served)
				h.serve(response, req, rec)
			}()
			codertestutil.TryReceive(ctx, t, recording)
			codertestutil.TryReceive(ctx, t, served)
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "response", response.Body.String())
		})
	}
}

func TestForwardingHandlerAbandonsCapture(t *testing.T) {
	t.Parallel()
	ctx := codertestutil.Context(t, codertestutil.WaitShort)
	input, output := io.Pipe()
	defer input.Close()
	defer output.Close()
	h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
	h.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("upstream error"))}, nil
	})
	req := recordedRequest(t, input)
	req.Header.Set("User-Agent", "claude-code/1.0")
	rec := &lifecycleRecorder{}
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.serve(response, req, rec)
	}()
	codertestutil.TryReceive(ctx, t, done)
	codertestutil.TryReceive(ctx, t, rec.done())
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Equal(t, "upstream error", response.Body.String())
	require.Len(t, rec.starts, 1)
	require.Nil(t, rec.starts[0].ClientSessionID)
	require.Len(t, rec.ends, 1)
	require.Equal(t, recorder.ErrorTypeBadRequest, rec.ends[0].ErrorType)
}

func TestForwardingHandlerProcessingPanic(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"Start", "End"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			rec := &lifecycleRecorder{}
			panicRecord := func(context.Context) { panic("recorder panic") }
			if stage == "Start" {
				rec.onStart = panicRecord
			} else {
				rec.onEnd = panicRecord
			}
			h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
			h.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("response"))}, nil
			})
			response := httptest.NewRecorder()
			h.serve(response, recordedRequest(t, nil), rec)
			require.NoError(t, h.inflight.Shutdown(codertestutil.Context(t, codertestutil.WaitLong)))
			require.Equal(t, http.StatusOK, response.Code)
			require.Equal(t, "response", response.Body.String())
			require.Len(t, rec.starts, 1)
			if stage == "Start" {
				require.Empty(t, rec.ends)
			} else {
				require.Len(t, rec.ends, 1)
			}
		})
	}
}

// TestForwardingHandlerRecordsStreamedRequest asserts that the start record
// keeps the request start time and takes body metadata from input that
// arrives after forwarding begins.
func TestForwardingHandlerRecordsStreamedRequest(t *testing.T) {
	t.Parallel()
	ctx := codertestutil.Context(t, codertestutil.WaitLong)
	input, output := io.Pipe()
	defer input.Close()
	defer output.Close()
	const prefix = `{"metadata":{"user_id":"user_hash_account_id_session_`
	const suffix = `session-one"}}`
	h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
	readPrefix := make(chan time.Time, 1)
	h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		chunk := make([]byte, len(prefix))
		if _, err := io.ReadFull(r.Body, chunk); err != nil {
			return nil, err
		}
		readPrefix <- time.Now()
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody}, nil
	})
	req := recordedRequest(t, input)
	req.Header.Set("User-Agent", "claude-code/1.0")
	rec := &lifecycleRecorder{}
	response := httptest.NewRecorder()
	served := make(chan struct{})
	before := time.Now()
	go func() {
		defer close(served)
		serveForwardingRequest(h, response, req, rec)
	}()
	_, err := io.WriteString(output, prefix)
	require.NoError(t, err)
	forwardedAt := codertestutil.RequireReceive(ctx, t, readPrefix)
	_, err = io.WriteString(output, suffix)
	require.NoError(t, err)
	require.NoError(t, output.Close())
	codertestutil.TryReceive(ctx, t, served)
	codertestutil.TryReceive(ctx, t, rec.done())
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, rec.starts, 1)
	require.Len(t, rec.ends, 1)
	require.Equal(t, new("session-one"), rec.starts[0].ClientSessionID)
	require.False(t, rec.starts[0].StartedAt.Before(before))
	require.False(t, rec.starts[0].StartedAt.After(forwardedAt))
	require.Empty(t, rec.ends[0].ErrorType)
}

func TestForwardingHandlerRecordsCircuitBreaker(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultCircuitBreaker()
	cfg.FailureThreshold = 1
	cfg.Timeout = time.Hour
	h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test", CircuitBreaker: &cfg}), nil)
	calls := 0
	h.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: http.NoBody}, nil
	})
	for range 2 {
		rec := &lifecycleRecorder{}
		response := httptest.NewRecorder()
		serveForwardingRequest(h, response, recordedRequest(t, nil), rec)
		codertestutil.TryReceive(codertestutil.Context(t, codertestutil.WaitLong), t, rec.done())
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Len(t, rec.starts, 1)
		require.Len(t, rec.ends, 1)
		require.Equal(t, recorder.ErrorTypeServerError, rec.ends[0].ErrorType)
	}
	require.Equal(t, 1, calls, "the open breaker must reject the second request")
}

// TestForwardingHandlerBodyFailureRecords asserts that failures reading the
// request body after admission are recorded, while preflight rejections are not.
func TestForwardingHandlerBodyFailureRecords(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"UnknownLengthOversize", "BodyReadFailure", "PreflightRejection"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			req := recordedRequest(t, nil)
			req.ContentLength = -1
			var status int
			switch name {
			case "UnknownLengthOversize":
				req.Body = io.NopCloser(io.LimitReader(readerFunc(func(p []byte) (int, error) { clear(p); return len(p), nil }), routing.MaxRequestBodyBytes+1))
				status = http.StatusRequestEntityTooLarge
			case "BodyReadFailure":
				req.Body = io.NopCloser(readerFunc(func([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }))
				status = http.StatusBadGateway
			case "PreflightRejection":
				req.Body = io.NopCloser(strings.NewReader("body"))
				req.Header.Set("X-Coder-Agent-Firewall-Session-Id", "invalid")
				status = http.StatusBadRequest
			}
			h := newTestForwardingHandler(t, provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"}), nil)
			h.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.NotEqual(t, "PreflightRejection", name, "unexpected upstream request")
				_, err := io.Copy(io.Discard, r.Body)
				return nil, err
			})
			rec := &lifecycleRecorder{}
			response := httptest.NewRecorder()
			serveForwardingRequest(h, response, req, rec)
			require.Equal(t, status, response.Code)
			if name == "PreflightRejection" {
				require.NoError(t, h.inflight.Shutdown(codertestutil.Context(t, codertestutil.WaitLong)))
				require.Empty(t, rec.starts)
				require.Empty(t, rec.ends)
				return
			}
			codertestutil.TryReceive(codertestutil.Context(t, codertestutil.WaitLong), t, rec.done())
			require.Len(t, rec.starts, 1)
			require.Len(t, rec.ends, 1)
			require.NotEmpty(t, rec.starts[0].ID)
			require.Nil(t, rec.starts[0].ClientSessionID)
			require.NotEmpty(t, rec.ends[0].ErrorType)
			if name == "UnknownLengthOversize" {
				require.Equal(t, recorder.ErrorTypeBadRequest, rec.ends[0].ErrorType)
			}
		})
	}
}

type unreadBody struct {
	read, closed bool
}

func (b *unreadBody) Read([]byte) (int, error) {
	b.read = true
	return 0, io.ErrUnexpectedEOF
}

func (b *unreadBody) Close() error {
	b.closed = true
	return nil
}

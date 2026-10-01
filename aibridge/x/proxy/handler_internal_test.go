package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"cdr.dev/slog/v3/sloggers/slogtest"
	"github.com/coder/coder/v2/aibridge/intercept"
	"github.com/coder/coder/v2/aibridge/internal/testutil"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
)

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

func TestForwardingHandlerPlaceholder(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("placeholder must not call upstream")
	}))
	t.Cleanup(upstream.Close)
	prov := &testutil.MockProvider{
		NameStr: "test", URL: upstream.URL,
		InterceptorFunc: func(http.ResponseWriter, *http.Request, trace.Tracer) (intercept.Interceptor, error) {
			t.Error("placeholder must not create an interceptor")
			return nil, io.ErrUnexpectedEOF
		},
	}
	for _, tc := range []struct {
		name    string
		prepare func(*http.Request)
		status  int
		message string
	}{
		{name: "UnparsedBody", status: http.StatusNotFound, message: "404 page not found"},
		{name: "WebSocket", prepare: func(r *http.Request) {
			r.Method = http.MethodGet
			r.Header.Set("Connection", "Upgrade")
			r.Header.Set("Upgrade", "websocket")
		}, status: http.StatusNotImplemented, message: "WebSocket transport is not supported, use HTTP"},
		{name: "InvalidFirewall", prepare: func(r *http.Request) {
			r.Header.Set("X-Coder-Agent-Firewall-Session-Id", "not-a-uuid")
			r.Header.Set("X-Coder-Agent-Firewall-Sequence-Number", "1")
		}, status: http.StatusBadRequest, message: "invalid agent firewall headers"},
		{name: "DeclaredOversize", prepare: func(r *http.Request) {
			r.ContentLength = routing.MaxRequestBodyBytes + 1
		}, status: http.StatusRequestEntityTooLarge, message: "Request body too large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &unreadBody{}
			req := httptest.NewRequest(http.MethodPost, "/test/messages", body)
			req.Header.Set("Content-Type", "application/json")
			if tc.prepare != nil {
				tc.prepare(req)
			}
			response := httptest.NewRecorder()
			newForwardingHandler(prov, slogtest.Make(t, nil), noop.NewTracerProvider().Tracer(t.Name()))(response, req)
			require.Equal(t, tc.status, response.Code)
			require.Contains(t, response.Body.String(), tc.message)
			require.False(t, body.read, "placeholder must not read the request body")
			if tc.status == http.StatusRequestEntityTooLarge {
				require.True(t, body.closed)
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

package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"cdr.dev/slog/v3"
	"github.com/coder/coder/v2/aibridge/client"
	"github.com/coder/coder/v2/aibridge/config"
	aibcontext "github.com/coder/coder/v2/aibridge/context"
	"github.com/coder/coder/v2/aibridge/credential"
	"github.com/coder/coder/v2/aibridge/keypool"
	"github.com/coder/coder/v2/aibridge/provider"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
	"github.com/coder/coder/v2/aibridge/utils"
	codertestutil "github.com/coder/coder/v2/testutil"
	"github.com/coder/quartz"
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
	for _, tc := range []struct {
		name       string
		prepare    func(*testing.T, *http.Request) *http.Request
		provider   func(*testing.T) provider.Provider
		noRecorder bool
		status     int
		message    string
		logLevel   slog.Level
		logMessage string
		bodyClosed bool
	}{
		{
			name: "WebSocket",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.Method = http.MethodGet
				r.Header.Set("Connection", "Upgrade")
				r.Header.Set("Upgrade", "websocket")
				return r
			},
			status: http.StatusNotImplemented, message: "WebSocket transport is not supported, use HTTP\n",
			logLevel: slog.LevelDebug, logMessage: "rejecting unsupported WebSocket upgrade",
		},
		{
			name: "MalformedFirewallSessionID",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("X-Coder-Agent-Firewall-Session-Id", "not-a-uuid")
				r.Header.Set("X-Coder-Agent-Firewall-Sequence-Number", "1")
				return r
			},
			status: http.StatusBadRequest, message: "invalid agent firewall headers\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting request with invalid agent firewall headers",
		},
		{
			name: "FirewallMissingSequence",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("X-Coder-Agent-Firewall-Session-Id", "e5f6a7b8-1234-5678-9abc-def012345678")
				return r
			},
			status: http.StatusBadRequest, message: "invalid agent firewall headers\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting request with invalid agent firewall headers",
		},
		{
			name: "DeclaredOversize",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.ContentLength = routing.MaxRequestBodyBytes + 1
				return r
			},
			status: http.StatusRequestEntityTooLarge, message: fmt.Sprintf("Request body too large. The maximum allowed request body size is %dMiB.\n", routing.MaxRequestBodyBytes>>20),
			logLevel: slog.LevelWarn, logMessage: "rejecting oversized request body", bodyClosed: true,
		},
		{
			name:    "MissingActor",
			prepare: func(t *testing.T, r *http.Request) *http.Request { return r.WithContext(t.Context()) },
			status:  http.StatusBadRequest, message: "no actor found\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting request without an actor",
		},
		{
			name: "MissingRecorder", noRecorder: true,
			status: http.StatusInternalServerError, message: "recorder unavailable\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting request without a recorder",
		},
		{
			name: "MissingCredential",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Authorization")
				return r
			},
			status: http.StatusBadGateway, message: "upstream authentication unavailable\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting request without an upstream credential",
		},
		{
			name: "UnsupportedSigning",
			prepare: func(_ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Authorization")
				return r
			},
			provider: func(t *testing.T) provider.Provider {
				prov, err := provider.NewAnthropic(t.Context(), config.Anthropic{BaseURL: "https://upstream.example.test"}, nil, &config.AWSClaudePlatform{Region: "us-west-2", WorkspaceID: "wrkspc_test"})
				require.NoError(t, err)
				return prov
			},
			status: http.StatusNotImplemented, message: "upstream authentication is not supported in proxy mode\n",
			logLevel: slog.LevelWarn, logMessage: "rejecting unsupported upstream credential",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var rec recorder.Recorder = &struct{ recorder.Recorder }{}
			if tc.noRecorder {
				rec = nil
			}
			body := &unreadBody{}
			req := recordedRequest(t, body)
			if tc.prepare != nil {
				req = tc.prepare(t, req)
			}
			var prov provider.Provider = provider.NewOpenAI(config.OpenAI{BaseURL: "https://upstream.example.test"})
			if tc.provider != nil {
				prov = tc.provider(t)
			}
			sink := codertestutil.NewFakeSink(t)
			h := &forwardingHandler{provider: prov, logger: sink.Logger()}
			response := httptest.NewRecorder()
			record, cred := h.checkRequest(response, req, rec)
			require.Nil(t, record)
			require.Nil(t, cred)
			require.Equal(t, tc.status, response.Code)
			require.Equal(t, tc.message, response.Body.String())
			require.False(t, body.read, "preflight must not read the request body")
			require.Equal(t, tc.bodyClosed, body.closed)
			entries := sink.Entries()
			require.Len(t, entries, 1)
			require.Equal(t, tc.logLevel, entries[0].Level)
			require.Equal(t, tc.logMessage, entries[0].Message)
			require.Contains(t, entries[0].Fields, slog.F("provider", prov.Name()))
			require.NotContains(t, fmt.Sprint(entries), "user-secret-key", "rejection logs must not include credentials")
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
			require.Equal(t, config.ProviderOpenAI, record.Provider)
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

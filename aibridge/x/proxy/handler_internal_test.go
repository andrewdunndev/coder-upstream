package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coder/coder/v2/aibridge/client"
	"github.com/coder/coder/v2/aibridge/config"
	aibcontext "github.com/coder/coder/v2/aibridge/context"
	"github.com/coder/coder/v2/aibridge/credential"
	"github.com/coder/coder/v2/aibridge/keypool"
	"github.com/coder/coder/v2/aibridge/provider"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
	"github.com/coder/coder/v2/aibridge/utils"
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

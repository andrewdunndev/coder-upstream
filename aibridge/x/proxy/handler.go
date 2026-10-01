package proxy

import (
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"cdr.dev/slog/v3"
	"github.com/coder/coder/v2/aibridge"
	aibclient "github.com/coder/coder/v2/aibridge/client"
	aibcontext "github.com/coder/coder/v2/aibridge/context"
	"github.com/coder/coder/v2/aibridge/credential"
	"github.com/coder/coder/v2/aibridge/headers"
	"github.com/coder/coder/v2/aibridge/provider"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
)

type forwardingHandler struct {
	provider provider.Provider
	logger   slog.Logger
	tracer   trace.Tracer
	inflight *aibridge.InflightGate
}

// newForwardingHandler prepares bridged requests before provider-specific handling.
func newForwardingHandler(prov provider.Provider, logger slog.Logger, tracer trace.Tracer, inflight *aibridge.InflightGate) *forwardingHandler {
	return &forwardingHandler{provider: prov, logger: logger, tracer: tracer, inflight: inflight}
}

func (h *forwardingHandler) serve(w http.ResponseWriter, r *http.Request, rec recorder.Recorder) {
	release, ok := h.inflight.Admit()
	if !ok {
		http.Error(w, "AI Gateway is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer release()
	ctx, cleanup := h.inflight.RequestContext(r.Context())
	defer cleanup()
	ctx, span := h.tracer.Start(ctx, "Proxy")
	defer span.End()
	if record, _ := h.checkRequest(w, r.WithContext(ctx), rec); record == nil {
		return
	}

	// TODO: connect forwarding with lifecycle recording.
	http.NotFound(w, r)
}

func (h *forwardingHandler) checkRequest(w http.ResponseWriter, r *http.Request, rec recorder.Recorder) (*recorder.InterceptionRecord, credential.Credential) {
	if headers.IsWebSocketUpgrade(r) {
		http.Error(w, "WebSocket transport is not supported, use HTTP", http.StatusNotImplemented)
		return nil, nil
	}
	firewallID, firewallSequence, err := headers.ExtractAgentFirewallHeaders(r)
	if err != nil {
		http.Error(w, "invalid agent firewall headers", http.StatusBadRequest)
		return nil, nil
	}
	if r.ContentLength > routing.MaxRequestBodyBytes {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		routing.WriteRequestBodyTooLarge(r.Context(), w)
		return nil, nil
	}
	actor := aibcontext.ActorFromContext(r.Context())
	if actor == nil {
		http.Error(w, "no actor found", http.StatusBadRequest)
		return nil, nil
	}
	if rec == nil {
		http.Error(w, "recorder unavailable", http.StatusInternalServerError)
		return nil, nil
	}
	cred, err := h.provider.ResolveCredential(r)
	if err != nil {
		http.Error(w, "upstream authentication unavailable", http.StatusBadGateway)
		return nil, nil
	}
	switch cred.(type) {
	case credential.BYOK, *credential.CentralizedPool:
	default:
		http.Error(w, "upstream authentication is not supported in proxy mode", http.StatusNotImplemented)
		return nil, nil
	}
	return &recorder.InterceptionRecord{
		InitiatorID:                 actor.ID,
		Metadata:                    actor.Metadata,
		Provider:                    h.provider.Type(),
		ProviderName:                h.provider.Name(),
		Client:                      string(aibclient.GuessClient(r)),
		UserAgent:                   r.UserAgent(),
		AgentFirewallSessionID:      firewallID,
		AgentFirewallSequenceNumber: firewallSequence,
		CredentialKind:              cred.Kind(),
		CredentialHint:              cred.Hint(),
	}, cred
}

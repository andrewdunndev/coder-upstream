package proxy

import (
	"fmt"
	"net/http"
	"strings"

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

// checkRequest validates a bridged request without reading its body and builds
// its interception record. On rejection it writes the error response and
// returns nil, nil, so the caller must not write to w. Otherwise it writes
// nothing and returns the record and the credential selected for upstream.
func (h *forwardingHandler) checkRequest(w http.ResponseWriter, r *http.Request, rec recorder.Recorder) (*recorder.InterceptionRecord, credential.Credential) {
	ctx := r.Context()
	logger := h.logger.With(slog.F("provider", h.provider.Name()))
	client := aibclient.GuessClient(r)
	if headers.IsWebSocketUpgrade(r) {
		logger.Debug(ctx, "rejecting unsupported WebSocket upgrade",
			slog.F("route", strings.TrimPrefix(r.URL.Path, h.provider.RoutePrefix())),
			slog.F("client", string(client)),
		)
		http.Error(w, "WebSocket transport is not supported, use HTTP", http.StatusNotImplemented)
		return nil, nil
	}
	firewallID, firewallSequence, err := headers.ExtractAgentFirewallHeaders(r)
	if err != nil {
		logger.Warn(ctx, "rejecting request with invalid agent firewall headers", slog.Error(err))
		http.Error(w, "invalid agent firewall headers", http.StatusBadRequest)
		return nil, nil
	}
	if r.ContentLength > routing.MaxRequestBodyBytes {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		logger.Warn(ctx, "rejecting oversized request body", slog.F("content_length", r.ContentLength))
		routing.WriteRequestBodyTooLarge(ctx, w)
		return nil, nil
	}
	actor := aibcontext.ActorFromContext(ctx)
	if actor == nil {
		logger.Warn(ctx, "rejecting request without an actor")
		http.Error(w, "no actor found", http.StatusBadRequest)
		return nil, nil
	}
	if rec == nil {
		logger.Warn(ctx, "rejecting request without a recorder")
		http.Error(w, "recorder unavailable", http.StatusInternalServerError)
		return nil, nil
	}
	cred, err := h.provider.ResolveCredential(r)
	if err != nil {
		logger.Warn(ctx, "rejecting request without an upstream credential", slog.Error(err))
		http.Error(w, "upstream authentication unavailable", http.StatusBadGateway)
		return nil, nil
	}
	switch cred.(type) {
	case credential.BYOK, *credential.CentralizedPool:
	default:
		// The type name identifies the credential kind; the value may hold secrets.
		logger.Warn(ctx, "rejecting unsupported upstream credential", slog.F("credential_type", fmt.Sprintf("%T", cred)))
		http.Error(w, "upstream authentication is not supported in proxy mode", http.StatusNotImplemented)
		return nil, nil
	}
	return &recorder.InterceptionRecord{
		InitiatorID:                 actor.ID,
		Metadata:                    actor.Metadata,
		Provider:                    h.provider.Type(),
		ProviderName:                h.provider.Name(),
		Client:                      string(client),
		UserAgent:                   r.UserAgent(),
		AgentFirewallSessionID:      firewallID,
		AgentFirewallSequenceNumber: firewallSequence,
		CredentialKind:              cred.Kind(),
		CredentialHint:              cred.Hint(),
	}, cred
}

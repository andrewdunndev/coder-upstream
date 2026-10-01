// Package proxy provides stateless AI Gateway request routing.
package proxy

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3"
	"github.com/coder/coder/v2/aibridge"
	"github.com/coder/coder/v2/aibridge/config"
	"github.com/coder/coder/v2/aibridge/keypool"
	"github.com/coder/coder/v2/aibridge/metrics"
	"github.com/coder/coder/v2/aibridge/provider"
	"github.com/coder/coder/v2/aibridge/recorder"
	"github.com/coder/coder/v2/aibridge/routing"
)

// Router serves the AI Gateway routes of an immutable snapshot of providers.
// It holds no per-user or per-request state, so a single instance serves every
// actor and is replaced wholesale when the provider configuration changes.
//
// Router has no concept of authentication or authorization; the caller is
// responsible for authenticating requests before they reach the router.
//
// Router is safe for concurrent use.
type Router struct {
	routes   []route
	fallback http.Handler

	// providers is an owned copy of the provider list, fixed at construction.
	providers  []provider.Provider
	transports []*http.Transport
}

type route struct {
	pattern string
	serve   func(http.ResponseWriter, *http.Request, recorder.Recorder)
}

// NewRouter creates a [*Router] for the given providers, whose
// names must be valid and unique.
//
// Each configured-but-disabled provider serves a 503 sentinel on every path
// under its name. Passthrough routes use the existing passthrough handler.
// Bridged routes require an actor on the request context and a recorder supplied
// to Serve. Bedrock bridged routes remain unsupported. All router snapshots
// must share the server's inflight gate so shutdown drains their requests.
func NewRouter(providers []provider.Provider, logger slog.Logger, m *metrics.Metrics, tracer trace.Tracer, inflight *aibridge.InflightGate) (_ *Router, outErr error) {
	if err := provider.ValidateProviders(providers); err != nil {
		return nil, err
	}

	snapshot := slices.Clone(providers)
	router := &Router{providers: snapshot, fallback: inflight.Middleware(routing.NewProviderMux(snapshot, logger))}
	defer func() {
		if outErr != nil {
			router.CloseIdleConnections()
		}
	}()
	for _, prov := range snapshot {
		if !prov.Enabled() {
			continue
		}

		notFound := inflight.Middleware(http.NotFoundHandler())
		bridged := func(w http.ResponseWriter, r *http.Request, _ recorder.Recorder) { notFound.ServeHTTP(w, r) }
		if prov.Type() != config.ProviderBedrock && len(prov.BridgedRoutes()) > 0 {
			handler, transport, err := newForwardingHandler(prov, logger, m, tracer, inflight)
			if err != nil {
				return nil, err
			}
			bridged = handler.serve
			router.transports = append(router.transports, transport)
		}
		for _, path := range prov.BridgedRoutes() {
			pattern, err := url.JoinPath(prov.RoutePrefix(), path)
			if err != nil {
				return nil, xerrors.Errorf("configure provider %q bridged route: %w", prov.Name(), err)
			}
			router.routes = append(router.routes, route{pattern: pattern, serve: bridged})
		}

		passthrough := inflight.Middleware(http.StripPrefix(prov.RoutePrefix(), aibridge.NewPassthroughRouter(prov, logger.Named(fmt.Sprintf("passthrough.%s", prov.Name())), m, tracer)))
		for _, path := range prov.PassthroughRoutes() {
			pattern, err := url.JoinPath(prov.RoutePrefix(), path)
			if err != nil {
				return nil, xerrors.Errorf("configure provider %q passthrough route: %w", prov.Name(), err)
			}
			router.routes = append(router.routes, route{pattern: pattern, serve: func(w http.ResponseWriter, r *http.Request, _ recorder.Recorder) {
				passthrough.ServeHTTP(w, r)
			}})
		}
	}

	// Detect conflicting route patterns at construction, not while serving.
	_ = router.requestMux(nil)
	return router, nil
}

// Serve dispatches a request with its authenticated recorder. Each handler owns
// its admission and processing lifetime. Passthrough routes do not need rec.
func (p *Router) Serve(rw http.ResponseWriter, r *http.Request, rec recorder.Recorder) {
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(rw, r.Body, routing.MaxRequestBodyBytes)
	}
	p.requestMux(rec).ServeHTTP(rw, r)
}

// requestMux binds the recorder without storing request state on shared handlers.
// Only the routing shell is request-local; provider handlers and transports are reused.
func (p *Router) requestMux(rec recorder.Recorder) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", p.fallback)
	for _, route := range p.routes {
		mux.HandleFunc(route.pattern, func(w http.ResponseWriter, r *http.Request) {
			route.serve(w, r, rec)
		})
	}
	return mux
}

// KeyPools returns the non-nil key pools from this router's provider snapshot.
func (p *Router) KeyPools() []*keypool.Pool {
	return provider.CollectKeyPools(p.providers)
}

// CloseIdleConnections releases idle connections owned by bridged forwarding.
// Active requests retain their connections until they finish.
func (p *Router) CloseIdleConnections() {
	for _, transport := range p.transports {
		transport.CloseIdleConnections()
	}
}

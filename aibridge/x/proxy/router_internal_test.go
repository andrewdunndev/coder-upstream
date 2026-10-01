package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	aibcontext "github.com/coder/coder/v2/aibridge/context"
)

func TestRouterDispatch(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/openai/v1/responses", func(w http.ResponseWriter, r *http.Request) {
		actor := aibcontext.ActorFromContext(r.Context())
		w.Header().Set("Actor-ID", actor.ID.String())
		w.Header().Set("API-Key-ID", actor.APIKeyID)
		w.Header().Set("Route-Pattern", r.Pattern)
		w.WriteHeader(http.StatusOK)
	})
	router := &Router{mux: mux}
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
			actor := aibcontext.Actor{ID: uuid.New(), APIKeyID: uuid.NewString()}
			req := httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
			if tc.path == "*" {
				req.URL.Path, req.RequestURI = tc.path, tc.path
			} else {
				req = httptest.NewRequest(http.MethodPost, tc.path, nil)
			}
			req = req.WithContext(aibcontext.AsActor(t.Context(), actor))
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, tc.status, response.Code)
			require.Same(t, mux, router.mux)
			if tc.status == http.StatusOK {
				require.Equal(t, actor.ID.String(), response.Header().Get("Actor-ID"))
				require.Equal(t, actor.APIKeyID, response.Header().Get("API-Key-ID"))
				require.Equal(t, "/openai/v1/responses", response.Header().Get("Route-Pattern"))
			} else {
				require.Empty(t, response.Header().Get("Actor-ID"))
				require.Empty(t, response.Header().Get("API-Key-ID"))
				require.Empty(t, response.Header().Get("Route-Pattern"))
			}
		})
	}
}

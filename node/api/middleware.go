package api

import (
	"net/http"
	"strings"
)

func (h *HTTPServer) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/values") {
			h.inflightCounter.Increment()
			defer h.inflightCounter.Decrement()
		}

		next.ServeHTTP(w, r)
	})
}

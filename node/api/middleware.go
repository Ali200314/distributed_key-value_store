package api

import (
	"net/http"
	"strings"
	"sync/atomic"
)

type InflightTracker struct {
	count atomic.Int64
}

func (i *InflightTracker) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/values") {
			i.count.Add(1)
			defer i.count.Add(-1)
		}

		next.ServeHTTP(w, r)
	})
}

func (i *InflightTracker) Count() int64 {
	return i.count.Load()
}

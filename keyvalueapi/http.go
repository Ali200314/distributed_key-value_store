package keyvalueapi

import (
	"net/http"
	"strconv"
	"time"

	"distributedkvstore/store"
)

type HTTPServer struct {
	store store.Store
}

func NewHTTPServer(s store.Store) *HTTPServer {
	return &HTTPServer{
		store: s,
	}
}

func (h *HTTPServer) Read(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	defer func() {
		latencyMs := time.Since(start).Milliseconds()
		h.store.RecordLatency(time.Duration(latencyMs))
	}()

	key := req.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val, err := h.store.Read(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write([]byte(strconv.Itoa(val)))
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) Create(w http.ResponseWriter, req *http.Request) {}

func (h *HTTPServer) Delete(w http.ResponseWriter, req *http.Request) {}

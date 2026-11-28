package api

import (
	"distributedkvstore/consistency"
	"encoding/json"
	"net/http"
	"time"

	"distributedkvstore/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var reads = promauto.NewCounter(prometheus.CounterOpts{
	Name: "read_requests",
})

type HTTPServer struct {
	store           store.Store
	latencyWindow   *consistency.Window
	inflightCounter *consistency.InflightCounter
}

func NewHTTPServer(store store.Store, latencyWindow *consistency.Window, inflightCounter *consistency.InflightCounter) *HTTPServer {
	return &HTTPServer{
		store:           store,
		latencyWindow:   latencyWindow,
		inflightCounter: inflightCounter,
	}
}

type body struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (h *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /values/{key}", h.Read)
	mux.HandleFunc("DELETE /values", h.Delete)
	mux.HandleFunc("POST /values", h.Create)
	mux.HandleFunc("GET /nodes", h.getNodes)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /leader", h.getLeader)
	return h.middleware(mux)
}

func (h *HTTPServer) Read(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	reads.Inc()
	defer func() {
		latencyMs := time.Since(start).Milliseconds()
		h.latencyWindow.RecordLatency(latencyMs)
	}()
	//time.Sleep(2 * time.Second)
	key := req.PathValue("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	val, cons, err := h.store.Read(key)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	consStr := "ANY_NODE_READ"
	if cons == store.LeaderRead {
		consStr = "LEADER_READ"
	}
	w.Header().Set("read-consistency", consStr)
	if val == "" {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if err := json.NewEncoder(w).Encode(val); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (h *HTTPServer) Create(w http.ResponseWriter, req *http.Request) {
	b := &body{}
	err := json.NewDecoder(req.Body).Decode(b)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}
	if err := h.store.Create(b.Key, b.Value); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPServer) Delete(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query().Get("key")
	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := h.store.Delete(key); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) Update(w http.ResponseWriter, req *http.Request) {
	b := &body{}
	err := json.NewDecoder(req.Body).Decode(b)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}
	if err := h.store.Update(b.Key, b.Value); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTPServer) getNodes(w http.ResponseWriter, req *http.Request) {
	addresses := h.store.NodeAddresses()
	if err := json.NewEncoder(w).Encode(addresses); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) getLeader(w http.ResponseWriter, req *http.Request) {
	if err := json.NewEncoder(w).Encode(h.store.LeaderAddress()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

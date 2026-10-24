package api

import (
	"distributedkvstore/consistency"
	"distributedkvstore/store"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type HTTPServer struct {
	store       store.Store
	consistency consistency.Consistency
}

func NewHTTPServer(s store.Store, c consistency.Consistency) *HTTPServer {
	return &HTTPServer{
		store:       s,
		consistency: c,
	}
}

type body struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (h *HTTPServer) Read(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	defer func() {
		latencyMs := time.Since(start).Milliseconds()
		h.consistency.RecordLatency(latencyMs)
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
	log.Printf("value: %s", val)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(val))
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

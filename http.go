package distributedkvstore

import (
	"net/http"

	"github.com/hashicorp/raft"
	"github.com/uber-go/tally/v4"
)

type HttpServer struct {
	raft  *raft.Raft
	scope tally.Scope
}

func NewHttpServer(raft *raft.Raft, scope tally.Scope) *HttpServer {
	return &HttpServer{
		raft:  raft,
		scope: scope,
	}
}

func (h *HttpServer) Read(w http.ResponseWriter, req *http.Request) {}

func (h *HttpServer) Create(w http.ResponseWriter, req *http.Request) {}

func (h *HttpServer) Delete(w http.ResponseWriter, req *http.Request) {}

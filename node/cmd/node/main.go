package main

import (
	"context"
	"distributedkvstore/api"
	"distributedkvstore/consistency"
	"distributedkvstore/store"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

var configuration = raft.Configuration{Servers: []raft.Server{
	{ID: raft.ServerID("1"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8081"))},
	{ID: raft.ServerID("2"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8082"))},
	{ID: raft.ServerID("3"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8083"))},
}}

func main() {
	id, grpcPort, httpPort := parseFlags()
	notifyCh := make(chan bool, 1)
	cfg := raft.DefaultConfig()
	cfg.NotifyCh = notifyCh
	//cfg.LogOutput = io.Discard
	cfg.LocalID = raft.ServerID(fmt.Sprint(id))
	addr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal("unable to resolve tcp address")
	}
	transport, err := raft.NewTCPTransport(addr, tcpAddr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("unable to create tcp transport: %s", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(fmt.Sprintf("./tmp/data-%d", id), 3, os.Stderr)
	if err != nil {
		log.Fatal("unable to create snapshot")
	}
	consistency := consistency.New(10, 10)
	go func() {
		for isLeader := range notifyCh {
			if isLeader {
				consistency.StartConsistencyHeartbeat(context.Background())
			}
		}
	}()
	fsm := store.NewFSM()
	raft, err := raft.NewRaft(
		cfg,
		fsm,
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		snapshots,
		transport,
	)
	if err := raft.BootstrapCluster(configuration).Error(); err != nil {
		log.Fatalf("unable to bootstrap cluster: %v", err)
	}
	store := store.New(raft, consistency, fsm)
	serv := api.NewHTTPServer(store, consistency)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/values", serv.Read)
	mux.HandleFunc("DELETE /api/values", serv.Delete)
	mux.HandleFunc("POST /api/values", serv.Create)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), mux); err != nil {
		log.Fatal("unable to start server")
	}
}

func parseFlags() (int, int, int) {
	id := flag.Int("id", 0, "Node ID")
	grpcPort := flag.Int("grpc-port", 0, "GRPC listening port")
	httpPort := flag.Int("http-port", 0, "HTTP listening port")

	flag.Parse()

	if *id == 0 {
		log.Fatal("No id specified")
	}
	if *grpcPort == 0 {
		log.Fatal("No GRPC port specified")
	}
	if *httpPort == 0 {
		log.Fatal("No HTTP port specified")
	}
	return *id, *grpcPort, *httpPort
}

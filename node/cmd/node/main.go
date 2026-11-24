package main

import (
	"context"
	"distributedkvstore/api"
	"distributedkvstore/consistency"
	latencyWindow "distributedkvstore/latency"
	"distributedkvstore/store"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	grpcTrans "github.com/Jille/raft-grpc-transport"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var configuration = raft.Configuration{Servers: []raft.Server{
	{ID: raft.ServerID("1"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8081"))},
	{ID: raft.ServerID("2"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8082"))},
	{ID: raft.ServerID("3"), Address: raft.ServerAddress(raft.ServerAddress("127.0.0.1:8083"))},
}}

func main() {
	id, grpcPort, httpPort, latency, inflight := parseFlags()
	notifyCh := make(chan bool, 1)
	cfg := raft.DefaultConfig()
	cfg.NotifyCh = notifyCh
	//cfg.LogOutput = io.Discard
	cfg.LocalID = raft.ServerID(fmt.Sprint(id))
	servAddr, err := serverAddressByID(configuration, id)
	if err != nil {
		panic(err)
	}

	dataDir := filepath.Join("tmp", fmt.Sprintf("data-%d", id))
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("unable to create data directory: %v", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		log.Fatal("unable to create snapshot")
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		log.Fatalf("unable to create bolt store: %v", err)
	}
	defer logStore.Close()
	inflightTracker := &api.InflightTracker{}
	latencyWindow := latencyWindow.New()
	fsm := store.NewFSM()
	s := store.New(fsm)
	c := consistency.New(latencyWindow, latency, int64(inflight), inflightTracker, id, s)
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(s.SetConsistencyInterceptor),
	}
	grpcServ := grpc.NewServer(grpc.UnaryInterceptor(s.HandleConsistencyInterceptor))
	tm := grpcTrans.New(servAddr, dialOpts)
	tm.Register(grpcServ)
	defer tm.Close()
	go func() {
		for isLeader := range notifyCh {
			if isLeader {
				c.StartHeartbeat(context.Background())
			}
		}
	}()
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		err = grpcServ.Serve(lis)
		if err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
	if err != nil {
		log.Fatalf("unable to check for existing raft state: %v", err)
	}

	r, err := raft.NewRaft(
		cfg,
		fsm,
		logStore,
		logStore,
		snapshots,
		tm.Transport(),
	)
	s.SetRaft(r)
	if err != nil {
		log.Fatalf("unable to create raft node: %v", err)
	}
	if !hasState {
		if err := r.BootstrapCluster(configuration).Error(); err != nil {
			log.Fatalf("unable to bootstrap cluster: %v", err)
		}
	}
	serv := api.NewHTTPServer(s, latencyWindow, inflightTracker)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), serv.Handler()); err != nil {
		log.Fatal("unable to start server")
	}
}

func parseFlags() (int, int, int, int64, float64) {
	id := flag.Int("id", 0, "Node ID")
	grpcPort := flag.Int("grpc-port", 0, "GRPC listening port")
	httpPort := flag.Int("http-port", 0, "HTTP listening port")
	latencyThreshold := flag.Int64("latency-threshold", 1000, "p99 latency read threshold in ms")
	cpuUsageThreshold := flag.Float64("cpu-usage-threshold", 0.5, "cpu usage threshold in percent")
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
	return *id, *grpcPort, *httpPort, *latencyThreshold, *cpuUsageThreshold
}

func serverAddressByID(cfg raft.Configuration, id int) (raft.ServerAddress, error) {
	targetID := raft.ServerID(strconv.Itoa(id))
	for _, srv := range cfg.Servers {
		if srv.ID == targetID {
			return srv.Address, nil
		}
	}
	return "", fmt.Errorf("server ID %q not found in configuration", targetID)
}

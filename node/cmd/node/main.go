package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

func main() {
	id, grpcPort, httpPort, peersRaw := parseFlags()

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatal("unable to resolve tcp address")
	}
	transport, err := raft.NewTCPTransport(fmt.Sprintf(":%d", grpcPort), addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatal("unable to create tcp transport")
	}
	snapshots, err := raft.NewFileSnapshotStore(fmt.Sprintf(":%d", grpcPort), 3, os.Stderr)
	if err != nil {
		log.Fatal("unable to create snapshot")
	}
	r,err:=raft.NewRaft(cfg,, fsm raft.FSM, logs raft.LogStore, stable raft.StableStore, snaps raft.SnapshotStore, trans raft.Transport)
}

func parseFlags() (int, int, int, string) {
	id := flag.Int("id", 0, "Node ID")
	grpcPort := flag.Int("grpc-port", 0, "GRPC listening port")
	httpPort := flag.Int("http-port", 0, "HTTP listening port")
	peersRaw := flag.String("peers", "", "Comma-separated peer addresses")

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
	if *peersRaw == "" {
		log.Fatal("No peers specified")
	}
	return *id, *grpcPort, *httpPort, *peersRaw
}

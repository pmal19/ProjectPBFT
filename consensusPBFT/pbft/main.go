package main

import (
	"ProjectPBFT/consensusPBFT/pbft/network"
	"os"
)

func main() {
	nodeID := os.Args[1]
	server := network.NewServer(nodeID)

	server.Start()
}

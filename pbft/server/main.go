package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
)

type KvStoreServer struct {
	Store map[string]string
}

func main() {
	var r *rand.Rand
	var seed int64
	var peers util.ArrayPeers
	var clientPort int
	var pbftPort int
	var client string
	var isByzantine bool

	flag.Int64Var(&seed, "seed", -1, "Seed for random number generator, values less than 0 result in use of time")
	flag.IntVar(&clientPort, "port", 3000, "Port on which server should listen to client requests")
	flag.IntVar(&pbftPort, "pbft", 3001, "Port on which server should listen to PBFT requests")
	flag.Var(&peers, "peer", "A peer for this process")
	flag.StringVar(&client, "client", "127.0.0.1:3005", "Pbft client")
	flag.BoolVar(&isByzantine, "byzantine", false, "Is a byzantine node?")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Is Byzantine - %v", isByzantine)
	// Initialize the random number generator
	if seed < 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		r = rand.New(rand.NewSource(seed))
	}

	// Get hostname
	name, err := os.Hostname()
	if err != nil {
		// Without a host name we can't really get an ID, so die.
		log.Fatalf("Could not get hostname")
	}

	id := fmt.Sprintf("%s:%d", name, pbftPort)
	log.Printf("Starting peer with ID %s", id)

	// This part might be redundant
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", clientPort)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	// Initialize KVStore
	store := util.KVStore{C: make(chan util.InputChannelType), Store: make(map[string]string)}

	kvs := KvStoreServer{Store: make(map[string]string)}

	go serve(&store, r, &peers, id, pbftPort, client, &kvs)

	// Tell GRPC that s will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in response.
	pb.RegisterKvStoreServer(s, &store)
	log.Printf("Going to listen on port %v", clientPort)
	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
	log.Printf("Done listening")
}

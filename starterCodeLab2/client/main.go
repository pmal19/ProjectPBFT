package main

import (
	"ProjectPBFT/pbft/pb"
	"flag"
	"fmt"
	"log"
	"os"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

// func connectToServers(server string) {
// 	backoffConfig := grpc.DefaultBackoffConfig
// 	// Choose an aggressive backoff strategy here.
// 	backoffConfig.MaxDelay = 500 * time.Millisecond
// 	conn, err := grpc.Dial(server, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
// 	// Ensure connection did not fail, which should not happen since this happens in the background
// 	if err != nil {
// 		return pb.NewRaftClient(nil), err
// 	}
// 	return pb.NewRaftClient(conn), nil
// }

func main() {

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Make client like a server - using ports // How???
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// var peers arrayPeers
	// var clientPort int
	// var raftPort int

	// flag.IntVar(&clientPort, "port", 3000, "Port on which server should listen to client requests")
	// flag.IntVar(&raftPort, "raft", 3001, "Port on which server should listen to Raft requests")
	// flag.Var(&peers, "peer", "A peer for this process")
	// flag.Parse()

	// // Initialize the random number generator
	// if seed < 0 {
	// 	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	// } else {
	// 	r = rand.New(rand.NewSource(seed))
	// }

	// // Get hostname
	// name, err := os.Hostname()
	// if err != nil {
	// 	// Without a host name we can't really get an ID, so die.
	// 	log.Fatalf("Could not get hostname")
	// }

	// id := fmt.Sprintf("%s:%d", name, raftPort)
	// log.Printf("Starting peer with ID %s", id)

	// // Convert port to a string form
	// portString := fmt.Sprintf(":%d", clientPort)
	// // Create socket that listens on the supplied port
	// c, err := net.Listen("tcp", portString)
	// if err != nil {
	// 	// Note the use of Fatalf which will exit the program after reporting the error.
	// 	log.Fatalf("Could not create listening socket %v", err)
	// }
	// // Create a new GRPC server
	// s := grpc.NewServer()

	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	endpoint := flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Could not clear")
	}

	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put error")
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	// Request value for hello
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}

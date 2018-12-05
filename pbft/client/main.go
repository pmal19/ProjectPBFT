package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"
)

// Is this needed??
type ClientResponse struct {
	ret  *pb.ClientResponse
	err  error
	node string
}

func callCommand(primary string, kvc *KvStoreClient, primaryConn pb.PbftClient, clientResponseChan chan ClientResponse) {
	log.Printf("callCommand - somthing")
	// call util.ClientRequestPBFT
	// do grpc call to primary - succ or redirect
	// while loop till f+1 correct responses from all nodes through some
	// channel using grpc
	// return with the result

	req := pb.ClientRequest{Operation: "Operation", Timestamp: 123, ClientID: "TheOneAndOnly"}
	ret, err := primaryConn.ClientRequestPBFT(context.Background(), &req)
	clientResponseChan <- ClientResponse{ret: ret, err: err, node: primary}
	// for p, c := range peerClients {
	// 	go func(c pb.RaftClient, p string) {
	// 		ret, err := c.ClientRequestPBFT(context.Background(), &req)
	// 		appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, lenOfEntries: int64(0)}
	// 	}(c, p)
	// 	log.Printf("iAmStillRunning %v Sending heartbeat from %v to %v currentTerm %v", iAmStillRunning, currentLeaderID, p, currentTerm)
	// }
}

func waitForSufficientResponses(primary string, kvc *KvStoreClient, primaryConn pb.PbftClient, clientResponseChan chan ClientResponse) {
	numberOfValidResponses := 0
	for numberOfValidResponses < 2 {
		pbftClr := <-clientResponseChan
		log.Printf("Recieved from %v", pbftClr.ret.Node)
		// Check something here and increment
	}
}

type KvStoreClient struct {
	Store map[string]string
}

func main() {

	// var clientPort int
	var pbftPort int
	var primary string

	// flag.IntVar(&clientPort, "port", 3000,
	// 	"Port on which server should listen to client requests")
	flag.IntVar(&pbftPort, "pbft", 3001,
		"Port on which client should listen to PBFT responses")

	flag.String(primary, "primary", "Primary")
	flag.Parse()

	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not get hostname")
	}
	id := fmt.Sprintf("%s:%d", name, pbftPort)
	log.Printf("Starting client with ID %s", id)

	// // Required??
	// portString := fmt.Sprintf(":%d", clientPort)
	// c, err := net.Listen("tcp", portString)
	// if err != nil {
	// 	log.Fatalf("Could not create listening socket %v", err)
	// }

	// Create a new GRPC server
	s := grpc.NewServer()
	pbft := util.Pbft{ClientRequestChan: make(chan util.ClientRequestInput), PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput)}
	// go util.RunPbftServer(&pbft, pbftPort)
	util.RunPbftServer(&pbft, pbftPort)
	primaryConn, err := util.ConnectToPeer(primary)
	// Initialize KVStore
	store := util.KVStore{C: make(chan util.InputChannelType), Store: make(map[string]string)}
	kvc := KvStoreClient{Store: make(map[string]string)}
	clientResponseChan := make(chan ClientResponse)

	// Tell GRPC that s will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in response.
	// Is this really? Am I really using KVStore as grpc??
	pb.RegisterKvStoreServer(s, &store)
	// log.Printf("Going to listen on port %v", clientPort)
	// Start serving, this will block this function and only return when done.
	// if err := s.Serve(c); err != nil {
	// 	log.Fatalf("Failed to serve %v", err)
	// }

	callCommand(primary, &kvc, primaryConn, clientResponseChan)
	req := pb.ClientRequest{Operation: "Operation", Timestamp: 123, ClientID: "TheOneAndOnly"}
	ret, err := primaryConn.ClientRequestPBFT(context.Background(), &req)
	clientResponseChan <- ClientResponse{ret: ret, err: err, node: primary}

	log.Printf("Done listening")
}

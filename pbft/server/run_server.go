package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"log"
	rand "math/rand"
	"time"
)

// type Pbft struct {
// 	ClientRequestChan chan ClientRequestInput
// 	PrePrepareMsgChan chan PrePrepareMsgInput
// 	PrepareMsgChan    chan PrepareMsgInput
// 	CommitMsgChan     chan CommitMsgInput
// }

func serve(s *util.KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int, client string) {
	pbft := util.Pbft{ClientRequestChan: make(chan util.ClientRequestInput), PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput)}
	go util.RunPbftServer(&pbft, port)
	peerClients := make(map[string]pb.PbftClient)
	for _, peer := range *peers {
		clientPeer, err := util.ConnectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerClients[peer] = clientPeer
		log.Printf("Connected to %v", peer)
	}
	clientConn, _ := util.ConnectToPeer(client)
	peerClients[client] = clientConn

	// Is this needed??
	// type ClientResponse struct {
	// 	ret  *pb.ClientResponse
	// 	err  error
	// 	peer string
	// }

	type PbftMsgAccepted struct {
		ret  *pb.PbftMsgAccepted
		err  error
		peer string
	}
	// clientResponseChan := make(chan ClientResponse)
	pbftMsgAcceptedChan := make(chan PbftMsgAccepted)

	// Create a timer and start running it
	timer := time.NewTimer(util.RandomDuration(r))

	for {
		select {
		case <-timer.C:
			log.Printf("Timeout")
			// for p, c := range peerClients {
			// 	// go func(c pb.PbftClient, p string) {
			// 	// 	ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: 1, CandidateID: id})
			// 	// 	voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
			// 	// }(c, p)
			// }
			util.RestartTimer(timer, r)
		case op := <-s.C:
			s.HandleCommand(op)
		// Should this be only in client?? pbftClr := <-pbft.ClientRequestChan
		case pbftClr := <-pbft.ClientRequestChan:
			log.Printf("Received ClientRequestChan %v", pbftClr.Arg.ClientID)
		case pbftPrePrep := <-pbft.PrePrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftPrePrep.Arg.Node)
		case pbftPre := <-pbft.PrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftPre.Arg.Node)
		case pbftCom := <-pbft.CommitMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftCom.Arg.Node)
		// Is this needed??
		// case clr := <-clientResponseChan:
		// 	log.Printf("Client Request Received")
		// 	// log.Printf("Client Request Received %v", clr.peer)
		case pbftMsg := <-pbftMsgAcceptedChan:
			// log.Printf("Some PBFT Msg Acceptance Received")
			log.Printf("Some PBFT Msg Acceptance Received %v, %v", pbftMsg.ret.TypeOfAccepted, pbftMsg.ret.Node)
		}
	}
	log.Printf("Strange to arrive here")
}

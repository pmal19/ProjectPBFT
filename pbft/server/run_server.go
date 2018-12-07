package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"context"
	"log"
	rand "math/rand"
	"strings"
	"time"
)

// type Pbft struct {
// 	ClientRequestChan chan ClientRequestInput
// 	PrePrepareMsgChan chan PrePrepareMsgInput
// 	PrepareMsgChan    chan PrepareMsgInput
// 	CommitMsgChan     chan CommitMsgInput
// }

func printClientRequest(cr pb.ClientRequest, view int64, seq int64) {
	log.Printf("Received client request in view %v and seq %v", view, seq)
	log.Printf("ClientRequest - operation : %v || timestamp : %v || clientId : %v", cr.Operation, cr.Timestamp, cr.ClientID)
}

func printClientResponse(cr pb.ClientResponse, view int64, seq int64) {
	log.Printf("Sending client response in view %v and seq %v", view, seq)
	log.Printf("ClientResponse - viewId : %v || timestamp : %v || clientId : %v || node : %v || nodeResult : %v", cr.ViewId, cr.Timestamp, cr.ClientID, cr.Node, cr.NodeResult)
}

func printPrePrepareMsg(ppm pb.PrePrepareMsg, view int64, seq int64) {
	log.Printf("Received pre-prepare request in view %v and seq %v", view, seq)
	log.Printf("PrePrepareMsg - viewId : %v || sequenceID : %v || digest : %v || request : %v || node : %v", ppm.ViewId, ppm.SequenceID, ppm.Digest, ppm.Request, ppm.Node)
}

func printPrepareMsg(pm pb.PrepareMsg, view int64, seq int64) {
	log.Printf("Received prepare request in view %v and seq %v", view, seq)
	log.Printf("PrepareMsg - viewId : %v || sequenceID : %v || digest : %v || node : %v", pm.ViewId, pm.SequenceID, pm.Digest, pm.Node)
}

func printCommitMsg(cm pb.PrepareMsg, view int64, seq int64) {
	log.Printf("Received prepare request in view %v and seq %v", view, seq)
	log.Printf("CommitMsg - viewId : %v || sequenceID : %v || digest : %v || node : %v", cm.ViewId, cm.SequenceID, cm.Digest, cm.Node)
}

func printPbftMsgAccepted(pma pb.PbftMsgAccepted, view int64, seq int64) {
	log.Printf("Received msg acc in view %v and seq %v", view, seq)
	log.Printf("PbftMsgAccepted - viewId : %v || sequenceID : %v || success : %v || typeOfAccepted : %v || node : %v", pma.ViewId, pma.SequenceID, pma.Success, pma.TypeOfAccepted, pma.Node)
}

func serve(s *util.KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int, client string, kvs *KvStoreServer) {
	pbft := util.Pbft{ClientRequestChan: make(chan util.ClientRequestInput), PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput)}
	go util.RunPbftServer(&pbft, port)
	peerClients := make(map[string]pb.PbftClient)

	log.Printf("client address - %v", client)
	clientConn, e := util.ConnectToPeer(client)
	if e != nil {
		log.Fatal("Failed to connect to client's GRPC - %v", e)
	}
	// peerClients[client] = clientConn
	log.Printf("Connected to client : clientConn - %v", clientConn)

	for _, peer := range *peers {
		log.Printf("peer address - %v", peer)
		clientPeer, err := util.ConnectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerClients[peer] = clientPeer
		log.Printf("Connected to %v", peer)
	}

	currentView := int64(1)
	seqId := int64(0)

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
			// log.Printf("Timeout")
			// for p, c := range peerClients {
			// 	// go func(c pb.PbftClient, p string) {
			// 	// 	ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: 1, CandidateID: id})
			// 	// 	voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
			// 	// }(c, p)
			// }
			log.Printf("My KVStore - %v", kvs.Store)
			util.RestartTimer(timer, r)
		// case op := <-s.C:
		// 	s.HandleCommand(op)
		// Should this be only in client?? pbftClr := <-pbft.ClientRequestChan
		case pbftClr := <-pbft.ClientRequestChan:
			log.Printf("Received ClientRequestChan %v", pbftClr.Arg.ClientID)
			// Do something here as primary to return initially to client
			printClientRequest(*pbftClr.Arg, currentView, seqId)
			clientId := pbftClr.Arg.ClientID
			op := strings.Split(pbftClr.Arg.Operation, ":")
			timestamp := pbftClr.Arg.Timestamp
			operation := op[0]
			key := op[1]
			val := op[2]
			res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
			if operation == "set" {
				kvs.Store[key] = val
				// reset res
				res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			} else if operation == "get" {
				// reset res
				val = kvs.Store[key]
				res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			}
			// log.Printf("Received pbft client (%v) req for %v at %v", clientId, operation, timestamp)

			digest := "message digest pre-prepare"
			prePrepareMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: seqId, Digest: digest, Request: pbftClr.Arg, Node: id}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					ret, err := c.PrePreparePBFT(context.Background(), &prePrepareMsg)
					pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
				}(c, p)
			}

			responseBack := pb.ClientResponse{ViewId: currentView, Timestamp: timestamp, ClientID: clientId, Node: id, NodeResult: &res}
			log.Printf("Sending back responseBack - %v", responseBack)
			pbftClr.Response <- responseBack
		case pbftPrePrep := <-pbft.PrePrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v from primary %v", pbftPrePrep.Arg, pbftPrePrep.Arg.Node)
			printPrePrepareMsg(*pbftPrePrep.Arg, currentView, seqId)

			digest := "message digest prepare"
			prepareMsg := pb.PrepareMsg{ViewId: currentView, SequenceID: seqId, Digest: digest, Node: id}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					ret, err := c.PreparePBFT(context.Background(), &prepareMsg)
					pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
				}(c, p)
			}

			responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: true, TypeOfAccepted: "pre-prepare", Node: id}
			pbftPrePrep.Response <- responseBack
		case pbftPre := <-pbft.PrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftPre.Arg.Node)
			printPrepareMsg(*pbftPre.Arg, currentView, seqId)

			responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: true, TypeOfAccepted: "prepare", Node: id}
			pbftPre.Response <- responseBack

		case pbftCom := <-pbft.CommitMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftCom.Arg.Node)
		// Is this needed??
		// case clr := <-clientResponseChan:
		// 	log.Printf("Client Request Received")
		// 	// log.Printf("Client Request Received %v", clr.peer)
		case pbftMsg := <-pbftMsgAcceptedChan:
			// log.Printf("Some PBFT Msg Acceptance Received")
			log.Printf("%v PBFT Msg Acceptance Received from %v", pbftMsg.ret.TypeOfAccepted, pbftMsg.ret.Node)
			printPbftMsgAccepted(*pbftMsg.ret, currentView, seqId)
		}
	}
	log.Printf("Strange to arrive here")
}

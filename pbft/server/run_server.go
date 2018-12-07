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

func printCommitMsg(cm pb.CommitMsg, view int64, seq int64) {
	log.Printf("Received prepare request in view %v and seq %v", view, seq)
	log.Printf("CommitMsg - viewId : %v || sequenceID : %v || digest : %v || node : %v", cm.ViewId, cm.SequenceID, cm.Digest, cm.Node)
}

func printPbftMsgAccepted(pma pb.PbftMsgAccepted, view int64, seq int64) {
	log.Printf("Received msg acc in view %v and seq %v", view, seq)
	log.Printf("PbftMsgAccepted - viewId : %v || sequenceID : %v || success : %v || typeOfAccepted : %v || node : %v", pma.ViewId, pma.SequenceID, pma.Success, pma.TypeOfAccepted, pma.Node)
}

type logEntry struct {
	viewId     int64             //`json:"group,omitempty" bson:",omitempty"`
	sequenceID int64             //`json:"group,omitempty" bson:",omitempty"`
	clientReq  *pb.ClientRequest //`json:"group,omitempty" bson:",omitempty"`
	prePrep    *pb.PrePrepareMsg //`json:"group,omitempty" bson:",omitempty"`
	pre        []*pb.PrepareMsg  //`json:"group,omitempty" bson:",omitempty"`
	com        []*pb.CommitMsg   //`json:"group,omitempty" bson:",omitempty"`
	// pMsg       []*pb.PbftMsgAccepted //`json:"group,omitempty" bson:",omitempty"`
	// clientRes      *pb.ClientResponse    //`json:"group,omitempty" bson:",omitempty"`
	prepared       bool //`json:"group,omitempty" bson:",omitempty"`
	committed      bool //`json:"group,omitempty" bson:",omitempty"`
	committedLocal bool //`json:"group,omitempty" bson:",omitempty"`
}

func verifyPrePrepare(prePrepareMsg *pb.PrePrepareMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {
	if prePrepareMsg.ViewId != viewId {
		return false
	}
	if sequenceID != -1 {
		if sequenceID >= prePrepareMsg.SequenceID {
			return false
		}
	}
	// if _, ok := logEntries[prePrepareMsg.SequenceID]; ok {
	// 	return false
	// }
	digest := util.Digest(prePrepareMsg.Request)
	if digest != prePrepareMsg.Digest {
		return false
	}
	return true
}

func verifyPrepare(prepareMsg *pb.PrepareMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {
	if prepareMsg.ViewId != viewId {
		return false
	}
	if sequenceID != -1 {
		if sequenceID >= prepareMsg.SequenceID {
			return false
		}
	}
	digest := logEntries[prepareMsg.SequenceID].prePrep.Digest
	if digest != prepareMsg.Digest {
		return false
	}
	return true
}

func verifyCommit(commitMsg *pb.CommitMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {
	if commitMsg.ViewId != viewId {
		return false
	}
	if sequenceID != -1 {
		if sequenceID >= commitMsg.SequenceID {
			return false
		}
	}
	digest := logEntries[commitMsg.SequenceID].prePrep.Digest
	if digest != commitMsg.Digest {
		return false
	}
	return true
}

func isPrepared(entry logEntry) bool {
	return len(entry.pre) >= 2
}

func isCommitted(entry logEntry) bool {
	return len(entry.com) >= 2
}

func isCommittedLocal(entry logEntry) bool {
	return len(entry.com) >= 3
}

func printMyStoreAndLog(logEntries []logEntry, kvs *KvStoreServer) {
	log.Printf("My KVStore - %v", kvs.Store)
	log.Printf("My Logs - %v", logEntries)
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
	seqId := int64(-1)
	var logEntries []logEntry
	// maxLogSize := 10000000
	maxMsgLogsSize := 10
	// logEntries := make(map[int64]logEntry)

	type ClientResponse struct {
		ret  *pb.ClientResponse
		err  error
		node string
	}

	type PbftMsgAccepted struct {
		ret  *pb.PbftMsgAccepted
		err  error
		peer string
	}
	clientResponseChan := make(chan ClientResponse)
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
			printMyStoreAndLog(logEntries, kvs)
			// log.Printf("My KVStore - %v", kvs.Store)
			// log.Printf("My Logs - %v", logEntries)
			util.RestartTimer(timer, r)
		// case op := <-s.C:
		// 	s.HandleCommand(op)
		// Should this be only in client?? pbftClr := <-pbft.ClientRequestChan
		case pbftClr := <-pbft.ClientRequestChan:
			clientReq := pbftClr.Arg
			log.Printf("Received ClientRequestChan %v", clientReq.ClientID)
			printClientRequest(*clientReq, currentView, seqId)
			// Do something here as primary to return initially to client
			seqId += 1
			clientId := clientReq.ClientID
			timestamp := clientReq.Timestamp
			// op := strings.Split(clientReq.Operation, ":")
			// operation := op[0]
			// key := op[1]
			// val := op[2]
			res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
			// if operation == "set" {
			// 	kvs.Store[key] = val
			// 	res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			// } else if operation == "get" {
			// 	val = kvs.Store[key]
			// 	res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			// }
			// log.Printf("Received pbft client (%v) req for %v at %v", clientId, operation, timestamp)

			// digest := "message digest pre-prepare"
			digest := util.Digest(clientReq)
			prePrepareMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: seqId, Digest: digest, Request: clientReq, Node: id}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					ret, err := c.PrePreparePBFT(context.Background(), &prePrepareMsg)
					pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
				}(c, p)
			}

			newEntry := logEntry{viewId: currentView, sequenceID: seqId, clientReq: clientReq, prePrep: &prePrepareMsg, pre: make([]*pb.PrepareMsg, maxMsgLogsSize), com: make([]*pb.CommitMsg, maxMsgLogsSize), prepared: false, committed: false, committedLocal: false}
			// logEntries[newEntry.sequenceID] = newEntry
			logEntries = append(logEntries, newEntry)

			responseBack := pb.ClientResponse{ViewId: currentView, Timestamp: timestamp, ClientID: clientId, Node: id, NodeResult: &res}
			log.Printf("Sending back responseBack - %v", responseBack)
			printMyStoreAndLog(logEntries, kvs)
			pbftClr.Response <- responseBack
		case pbftPrePrep := <-pbft.PrePrepareMsgChan:
			prePrepareMsg := pbftPrePrep.Arg
			log.Printf("Received PrePrepareMsgChan %v from primary %v", pbftPrePrep.Arg, pbftPrePrep.Arg.Node)
			printPrePrepareMsg(*prePrepareMsg, currentView, seqId)

			verified := verifyPrePrepare(prePrepareMsg, currentView, seqId, logEntries)
			if verified {
				digest := prePrepareMsg.Digest
				prepareMsg := pb.PrepareMsg{ViewId: prePrepareMsg.ViewId, SequenceID: prePrepareMsg.SequenceID, Digest: digest, Node: id}
				for p, c := range peerClients {
					go func(c pb.PbftClient, p string) {
						ret, err := c.PreparePBFT(context.Background(), &prepareMsg)
						pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
					}(c, p)
				}

				newEntry := logEntry{viewId: prePrepareMsg.ViewId, sequenceID: prePrepareMsg.SequenceID, clientReq: prePrepareMsg.Request, prePrep: prePrepareMsg, pre: make([]*pb.PrepareMsg, maxMsgLogsSize), com: make([]*pb.CommitMsg, maxMsgLogsSize), prepared: false, committed: false, committedLocal: false}

				oldPrepares := newEntry.pre
				oldPrepares = append(oldPrepares, &prepareMsg)
				newEntry.pre = oldPrepares

				logEntries = append(logEntries, newEntry)

				// oldEntry, ok := logEntries[prePrepareMsg.SequenceID]
				// if ok {
				// 	oldEntry.prePrep = prePrepareMsg
				// 	logEntries[prePrepareMsg.SequenceID] = oldEntry
				// } else {
				// 	newEntry := logEntry{viewId: prePrepareMsg.ViewId, sequenceID: prePrepareMsg.SequenceID, clientReq: prePrepareMsg.Request, prePrep: prePrepareMsg}
				// 	// logEntries[newEntry.sequenceID] = newEntry
				// 	logEntries = append(logEntries, newEntry)
				// }
			}
			responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "pre-prepare", Node: id}
			printMyStoreAndLog(logEntries, kvs)
			pbftPrePrep.Response <- responseBack
		case pbftPre := <-pbft.PrepareMsgChan:
			prepareMsg := pbftPre.Arg
			log.Printf("Received PrePrepareMsgChan %v", prepareMsg)
			printPrepareMsg(*prepareMsg, currentView, seqId)

			verified := verifyPrepare(prepareMsg, currentView, seqId, logEntries)
			if verified {
				oldEntry := logEntries[prepareMsg.SequenceID]
				oldPrepares := oldEntry.pre
				oldPrepares = append(oldPrepares, prepareMsg)
				oldEntry.pre = oldPrepares
				logEntries[prepareMsg.SequenceID] = oldEntry
				prepared := isPrepared(oldEntry)
				oldEntry.prepared = prepared
				logEntries[prepareMsg.SequenceID] = oldEntry
				if prepared {

					commitMsg := pb.CommitMsg{ViewId: prepareMsg.ViewId, SequenceID: prepareMsg.SequenceID, Digest: prepareMsg.Digest, Node: id}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							ret, err := c.CommitPBFT(context.Background(), &commitMsg)
							pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
						}(c, p)
					}

					oldCommits := oldEntry.com
					oldCommits = append(oldCommits, &commitMsg)
					oldEntry.com = oldCommits
					logEntries[prepareMsg.SequenceID] = oldEntry

				}
			}

			responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "prepare", Node: id}
			printMyStoreAndLog(logEntries, kvs)
			pbftPre.Response <- responseBack
		case pbftCom := <-pbft.CommitMsgChan:
			commitMsg := pbftCom.Arg
			log.Printf("Received CommitMsgChan %v", pbftCom.Arg.Node)
			printCommitMsg(*commitMsg, currentView, seqId)

			verified := verifyCommit(commitMsg, currentView, seqId, logEntries)
			if verified {
				oldEntry := logEntries[commitMsg.SequenceID]
				oldCommits := oldEntry.com
				oldCommits = append(oldCommits, commitMsg)
				oldEntry.com = oldCommits
				logEntries[commitMsg.SequenceID] = oldEntry
				committed := isCommitted(oldEntry)
				oldEntry.committed = committed
				committedLocal := isCommittedLocal(oldEntry)
				oldEntry.committedLocal = committedLocal
				logEntries[commitMsg.SequenceID] = oldEntry
				if committedLocal {
					// Execute and finally send back to client to aggregate
					clr := oldEntry.clientReq
					op := strings.Split(clr.Operation, ":")
					operation := op[0]
					key := op[1]
					val := op[2]
					res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
					if operation == "set" {
						kvs.Store[key] = val
						res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
					} else if operation == "get" {
						val = kvs.Store[key]
						res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
					}
					e := pb.Entry{Term: int64(12), Index: int64(34)}
					log.Printf("res %v e %v", res, e)
					clr.NodeResult = &res
					go func(c pb.PbftClient) {
						ret, err := c.ClientRequestPBFT(context.Background(), clr)
						clientResponseChan <- ClientResponse{ret: ret, err: err, node: id}
					}(clientConn)
				}
			}

			responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "commit", Node: id}
			printMyStoreAndLog(logEntries, kvs)
			pbftCom.Response <- responseBack
		// Is this needed??
		case clr := <-clientResponseChan:
			log.Printf("Client Response Received for committedLocal and executed state %v", clr)
		// 	// log.Printf("Client Request Received %v", clr.peer)
		case pbftMsg := <-pbftMsgAcceptedChan:
			// log.Printf("Some PBFT Msg Acceptance Received")
			log.Printf("PBFT Msg Acceptance Received - %v", pbftMsg)

			// log.Printf("%v PBFT Msg Acceptance Received from %v", pbftMsg.ret.TypeOfAccepted, pbftMsg.ret.Node)
			// printPbftMsgAccepted(*pbftMsg.ret, currentView, seqId)
		}
	}
	log.Printf("Strange to arrive here")
}

package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"context"
	"log"
	rand "math/rand"
	"strconv"
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
	log.Printf("Received commit request in view %v and seq %v", view, seq)
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
		if sequenceID > prePrepareMsg.SequenceID {
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
		if sequenceID > prepareMsg.SequenceID {
			return false
		}
	}
	if prepareMsg.SequenceID+1 > int64(len(logEntries)) {
		return false
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
		if sequenceID > commitMsg.SequenceID {
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
	prePrepareMsg := entry.prePrep
	if prePrepareMsg != nil {
		validPrepares := 0
		for i := 0; i < len(entry.pre); i++ {
			prepareMsg := entry.pre[i]
			if prepareMsg.ViewId == prePrepareMsg.ViewId && prepareMsg.SequenceID == prePrepareMsg.SequenceID && prepareMsg.Digest == prePrepareMsg.Digest {
				validPrepares += 1
			}
		}
		return validPrepares >= 2
	}
	return false
}

func isCommitted(entry logEntry) bool {
	prepared := isPrepared(entry)
	if prepared {
		prePrepareMsg := entry.prePrep
		validCommits := 0
		for i := 0; i < len(entry.com); i++ {
			commitMsg := entry.com[i]
			if commitMsg.ViewId == prePrepareMsg.ViewId && commitMsg.SequenceID == prePrepareMsg.SequenceID && commitMsg.Digest == prePrepareMsg.Digest {
				validCommits += 1
			}
		}
		return validCommits >= 2
	}
	return false
}

func isCommittedLocal(entry logEntry) bool {
	committed := isCommitted(entry)
	if committed {
		prePrepareMsg := entry.prePrep
		validCommits := 0
		for i := 0; i < len(entry.com); i++ {
			commitMsg := entry.com[i]
			if commitMsg.ViewId == prePrepareMsg.ViewId && commitMsg.SequenceID == prePrepareMsg.SequenceID && commitMsg.Digest == prePrepareMsg.Digest {
				validCommits += 1
			}
		}
		return validCommits >= 3
	}
	return false
}

func printMyStoreAndLog(logEntries []logEntry, kvs *KvStoreServer, currentView int64, seqId int64) {
	log.Printf("currentView - %v || seqId - %v", currentView, seqId)
	log.Printf("My KVStore - %v", kvs.Store)
	log.Printf("My Logs - %v", logEntries)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func tamper(digest string) string {
	return RandStringRunes(len(digest))
}

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

func serve(s *util.KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int, client string, kvs *KvStoreServer, isByzantine bool) {
	pbft := util.Pbft{ClientRequestChan: make(chan util.ClientRequestInput), PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput), ViewChangeMsgChan: make(chan util.ViewChangeMsgInput)}
	go util.RunPbftServer(&pbft, port)
	peerClients := make(map[string]pb.PbftClient)

	log.Printf("client address - %v", client)
	clientConn, e := util.ConnectToPeer(client)
	if e != nil {
		log.Fatal("Failed to connect to client's GRPC - %v", e)
	}
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

	clientResponseChan := make(chan ClientResponse)
	pbftMsgAcceptedChan := make(chan PbftMsgAccepted)

	currentView := int64(0)
	seqId := int64(-1)
	var logEntries []logEntry
	maxMsgLogsSize := 0
	myId := int64(port) % 3001
	timer := time.NewTimer(util.RandomDuration(r))
	viewChangeTimer := util.NewSecondsTimer(util.RandomDuration(r))
	viewChangeTimer.Stop()

	transitionPhase := false
	numberOfVotes := 0

	for {
		select {
		case <-timer.C:
			printMyStoreAndLog(logEntries, kvs, currentView, seqId)
			util.RestartTimer(timer, r)
		case <-viewChangeTimer.Timer.C:
			newView := (currentView + 1) % 4
			log.Printf("Timeout - initiate view change. New view - %v", newView)
			transitionPhase = true
			viewChange := pb.ViewChangeMsg{Type: "view-change", NewView: newView, LastSequenceID: seqId - 1, Node: id}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					ret, err := c.ViewChangePBFT(context.Background(), &viewChange)
					pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
				}(c, p)
			}
			printMyStoreAndLog(logEntries, kvs, currentView, seqId)

		case pbftVc := <-pbft.ViewChangeMsgChan:
			// Got request for vote change
			newView := pbftVc.Arg.NewView
			if pbftVc.Arg.Type == "new-view" {
				log.Printf("Switching to new view - %v || %v", newView, pbftVc)
				// Should send back redirect here for commands that weren't committed
				iWasLeader := currentView == myId
				currentView = newView
				transitionPhase = false
				numberOfVotes = 0
				seqId = pbftVc.Arg.LastSequenceID
				if iWasLeader {
					redirectMsg := pb.ClientRequest{Operation: "redirect", Timestamp: time.Now().UnixNano(), ClientID: strconv.FormatInt(newView+3001, 10)}
					log.Printf("redirectMsg - %v || redirectMsg.ClientID", redirectMsg, strconv.FormatInt(newView+3001, 10))
					go func(c pb.PbftClient) {
						ret, err := c.ClientRequestPBFT(context.Background(), &redirectMsg)
						clientResponseChan <- ClientResponse{ret: ret, err: err, node: strconv.FormatInt(myId+3001, 10)}
					}(clientConn)
					log.Printf("Send Back Redirect message - View Change")
				}
			} else {
				if newView == myId {
					log.Printf("Received vote from %v", pbftVc.Arg.Node)
					numberOfVotes += 1
				} else {
					log.Printf("Received view change request - %v", pbftVc)
				}
			}
			// New Primary
			if numberOfVotes >= 2 {
				viewChangeTimer.Stop()
				log.Printf("Switching to new view - %v and taking on as primary", newView)
				viewChange := pb.ViewChangeMsg{Type: "new-view", NewView: newView, LastSequenceID: seqId - 1, Node: strconv.FormatInt(myId+3001, 10)}
				for p, c := range peerClients {
					go func(c pb.PbftClient, p string) {
						ret, err := c.ViewChangePBFT(context.Background(), &viewChange)
						pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
					}(c, p)
				}
				transitionPhase = false
				currentView = newView
				numberOfVotes = 0
				seqId = pbftVc.Arg.LastSequenceID
			}
		case pbftClr := <-pbft.ClientRequestChan:
			clientReq := pbftClr.Arg
			if !transitionPhase {
				iAmLeader := currentView == myId
				if iAmLeader {
					log.Printf("Received ClientRequestChan %v", clientReq.ClientID)
					printClientRequest(*clientReq, currentView, seqId)
					seqId += 1
					clientId := clientReq.ClientID
					timestamp := clientReq.Timestamp
					res := pb.Result{Result: &pb.Result_S{S: &pb.Success{IsSuccessful: true}}}
					digest := util.Digest(clientReq)
					if isByzantine {
						digest = tamper(digest)
					}
					prePrepareMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: seqId, Digest: digest, Request: clientReq, Node: strconv.FormatInt(myId+3001, 10)}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							ret, err := c.PrePreparePBFT(context.Background(), &prePrepareMsg)
							pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
						}(c, p)
					}
					newEntry := logEntry{viewId: currentView, sequenceID: seqId, clientReq: clientReq, prePrep: &prePrepareMsg, pre: make([]*pb.PrepareMsg, maxMsgLogsSize), com: make([]*pb.CommitMsg, maxMsgLogsSize), prepared: false, committed: false, committedLocal: false}
					if int64(len(logEntries)) >= seqId+1 {
						logEntries[seqId] = newEntry
					} else {
						logEntries = append(logEntries, newEntry)
					}
					responseBack := pb.ClientResponse{ViewId: currentView, Timestamp: timestamp, ClientID: clientId, Node: strconv.FormatInt(myId+3001, 10), NodeResult: &res, SequenceID: seqId}
					log.Printf("Sending back responseBack - %v", responseBack)
					printMyStoreAndLog(logEntries, kvs, currentView, seqId)
					pbftClr.Response <- responseBack
				} else {
					// Need to send some kind of redirect message
					res := pb.Result{Result: &pb.Result_S{S: &pb.Success{IsSuccessful: false}}}
					responseBack := pb.ClientResponse{ViewId: currentView, Timestamp: clientReq.Timestamp, ClientID: clientReq.ClientID, Node: strconv.FormatInt(myId+3001, 10), NodeResult: &res, SequenceID: seqId}
					pbftClr.Response <- responseBack
					redirectMsg := pb.ClientRequest{Operation: "redirect", Timestamp: clientReq.Timestamp, ClientID: strconv.FormatInt(currentView+3001, 10)}
					go func(c pb.PbftClient) {
						ret, err := c.ClientRequestPBFT(context.Background(), &redirectMsg)
						clientResponseChan <- ClientResponse{ret: ret, err: err, node: strconv.FormatInt(myId+3001, 10)}
					}(clientConn)
					log.Printf("Send Back Redirect message")
				}
			} else {
				log.Printf("Received ClientRequestChan %v", clientReq.ClientID)
				log.Printf("But.....Requested View Change")
				// log.Printf("Send Back Redirect message - View Change")
			}
		case pbftPrePrep := <-pbft.PrePrepareMsgChan:
			prePrepareMsg := pbftPrePrep.Arg
			seqId = prePrepareMsg.SequenceID
			if !transitionPhase {
				if viewChangeTimer.TimeRemaining() < 100*time.Millisecond {
					dur := util.RandomDuration(r)
					log.Printf("Resetting timer for duration - %v", dur)
					viewChangeTimer.Reset(dur)
				}
				log.Printf("Received PrePrepareMsgChan %v from primary %v", pbftPrePrep.Arg, pbftPrePrep.Arg.Node)
				printPrePrepareMsg(*prePrepareMsg, currentView, seqId)
				verified := verifyPrePrepare(prePrepareMsg, currentView, seqId, logEntries)
				if verified {
					digest := prePrepareMsg.Digest
					if isByzantine {
						digest = tamper(digest)
					}
					prepareMsg := pb.PrepareMsg{ViewId: prePrepareMsg.ViewId, SequenceID: prePrepareMsg.SequenceID, Digest: digest, Node: strconv.FormatInt(myId+3001, 10)}
					if prePrepareMsg.SequenceID+1 <= int64(len(logEntries)) {
						log.Printf("Had received a prepare msg before, so writing on previous seqId - %v", prePrepareMsg.SequenceID)
						oldEntry := logEntries[prePrepareMsg.SequenceID]
						oldEntry.prePrep = prePrepareMsg
						oldEntry.clientReq = prePrepareMsg.Request
						oldPrepares := oldEntry.pre
						oldPrepares = append(oldPrepares, &prepareMsg)
						oldEntry.pre = oldPrepares
						logEntries[prePrepareMsg.SequenceID] = oldEntry
					} else {
						log.Printf("Appending new entry to log")
						newEntry := logEntry{viewId: prePrepareMsg.ViewId, sequenceID: prePrepareMsg.SequenceID, clientReq: prePrepareMsg.Request, prePrep: prePrepareMsg, pre: make([]*pb.PrepareMsg, maxMsgLogsSize), com: make([]*pb.CommitMsg, maxMsgLogsSize), prepared: false, committed: false, committedLocal: false}
						oldPrepares := newEntry.pre
						oldPrepares = append(oldPrepares, &prepareMsg)
						newEntry.pre = oldPrepares
						logEntries = append(logEntries, newEntry)
					}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							// time.Sleep(10 * time.Millisecond)
							ret, err := c.PreparePBFT(context.Background(), &prepareMsg)
							pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
						}(c, p)
					}
				}
				responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "pre-prepare", Node: strconv.FormatInt(myId+3001, 10)}
				printMyStoreAndLog(logEntries, kvs, currentView, seqId)
				pbftPrePrep.Response <- responseBack
			} else {
				log.Printf("Received PrePrepareMsgChan %v from primary %v", pbftPrePrep.Arg, pbftPrePrep.Arg.Node)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		case pbftPre := <-pbft.PrepareMsgChan:
			prepareMsg := pbftPre.Arg
			if !transitionPhase {
				log.Printf("Received PrepareMsgChan %v", prepareMsg)
				printPrepareMsg(*prepareMsg, currentView, seqId)
				verified := verifyPrepare(prepareMsg, currentView, seqId, logEntries)
				if verified {
					if viewChangeTimer.TimeRemaining() < 100*time.Millisecond {
						dur := util.RandomDuration(r)
						log.Printf("Resetting timer for duration - %v", dur)
						viewChangeTimer.Reset(dur)
					}
					prepared := false
					if prepareMsg.SequenceID+1 <= int64(len(logEntries)) {
						log.Printf("Normal case received pre-prepare before prepare - writing to entry in logs at - %v", prepareMsg.SequenceID)
						oldEntry := logEntries[prepareMsg.SequenceID]
						oldPrepares := oldEntry.pre
						oldPrepares = append(oldPrepares, prepareMsg)
						oldEntry.pre = oldPrepares
						logEntries[prepareMsg.SequenceID] = oldEntry
						prepared = isPrepared(oldEntry)
						oldEntry.prepared = prepared
						logEntries[prepareMsg.SequenceID] = oldEntry
					} else {
						log.Printf("Have received prepare before pre-prepare - appending new entry to logs")
						newEntry := logEntry{viewId: prepareMsg.ViewId, sequenceID: prepareMsg.SequenceID, pre: make([]*pb.PrepareMsg, maxMsgLogsSize), com: make([]*pb.CommitMsg, maxMsgLogsSize), prepared: false, committed: false, committedLocal: false}
						oldPrepares := newEntry.pre
						oldPrepares = append(oldPrepares, prepareMsg)
						newEntry.pre = oldPrepares
						logEntries = append(logEntries, newEntry)
					}
					if prepared {
						digest := prepareMsg.Digest
						if isByzantine {
							digest = tamper(digest)
						}
						commitMsg := pb.CommitMsg{ViewId: prepareMsg.ViewId, SequenceID: prepareMsg.SequenceID, Digest: prepareMsg.Digest, Node: strconv.FormatInt(myId+3001, 10)}
						for p, c := range peerClients {
							go func(c pb.PbftClient, p string) {
								// time.Sleep(100 * time.Millisecond)
								ret, err := c.CommitPBFT(context.Background(), &commitMsg)
								pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
							}(c, p)
						}
						oldEntry := logEntries[prepareMsg.SequenceID]
						oldCommits := oldEntry.com
						oldCommits = append(oldCommits, &commitMsg)
						oldEntry.com = oldCommits
						logEntries[prepareMsg.SequenceID] = oldEntry
					}
				}
				responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "prepare", Node: strconv.FormatInt(myId+3001, 10)}
				printMyStoreAndLog(logEntries, kvs, currentView, seqId)
				pbftPre.Response <- responseBack
			} else {
				log.Printf("Received PrepareMsgChan %v", prepareMsg)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		case pbftCom := <-pbft.CommitMsgChan:
			if !transitionPhase {
				commitMsg := pbftCom.Arg
				log.Printf("Received CommitMsgChan %v", pbftCom.Arg.Node)
				printCommitMsg(*commitMsg, currentView, seqId)
				verified := verifyCommit(commitMsg, currentView, seqId, logEntries)
				if verified {
					if viewChangeTimer.TimeRemaining() < 100*time.Millisecond {
						dur := util.RandomDuration(r)
						log.Printf("Resetting timer for duration - %v", dur)
						viewChangeTimer.Reset(dur)
					}
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
						viewChangeTimer.Stop()
						// Execute and finally send back to client to aggregate
						clr := oldEntry.clientReq
						op := strings.Split(clr.Operation, ":")
						operation := op[0]
						key := op[1]
						val := op[2]
						res := pb.Result{Result: &pb.Result_S{S: &pb.Success{IsSuccessful: true}}}
						if operation == "set" {
							kvs.Store[key] = val
							res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
						} else if operation == "get" {
							val = kvs.Store[key]
							res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
						}
						clr.NodeResult = &res
						clr.ClientID = strconv.FormatInt(myId+3001, 10)
						clr.SequenceID = commitMsg.SequenceID
						go func(c pb.PbftClient) {
							ret, err := c.ClientRequestPBFT(context.Background(), clr)
							clientResponseChan <- ClientResponse{ret: ret, err: err, node: strconv.FormatInt(myId+3001, 10)}
						}(clientConn)
					}
				}
				responseBack := pb.PbftMsgAccepted{ViewId: currentView, SequenceID: seqId, Success: verified, TypeOfAccepted: "commit", Node: strconv.FormatInt(myId+3001, 10)}
				printMyStoreAndLog(logEntries, kvs, currentView, seqId)
				pbftCom.Response <- responseBack
			} else {
				log.Printf("Received CommitMsgChan %v", pbftCom.Arg.Node)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		case clr := <-clientResponseChan:
			log.Printf("Client Response Received for committedLocal and executed state %v", clr)
		case pbftMsg := <-pbftMsgAcceptedChan:
			log.Printf("PBFT Msg Acceptance Received - %v", pbftMsg)
		}
	}
	log.Printf("Strange to arrive here")
}

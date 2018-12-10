package main

import (
	"ProjectPBFT/pbft/pb"
	"context"
	"log"
)

func startViewChange(newView int64, peerClients map[string]pb.PbftClient, seqId int64, node string, pbftMsgAcceptedChan chan PbftMsgAccepted, logEntries []logEntry, kvs *KvStoreServer, currentView int64) {
	log.Printf("Starting View Change")
	viewChange := pb.ViewChangeMsg{Type: "view-change", NewView: newView, LastSequenceID: seqId, Node: node}
	for p, c := range peerClients {
		go func(c pb.PbftClient, p string) {
			ret, err := c.ViewChangePBFT(context.Background(), &viewChange)
			pbftMsgAcceptedChan <- PbftMsgAccepted{ret: ret, err: err, peer: p}
		}(c, p)
	}
	printMyStoreAndLog(logEntries, kvs, currentView)
}

// func

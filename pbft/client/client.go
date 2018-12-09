package main

import (
	"ProjectPBFT/pbft/pb"
	"ProjectPBFT/pbft/util"
	"context"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
)

type Welcome struct {
	Name        string
	Time        string
	ReturnValue string
}

type ClientResponse struct {
	ret  *pb.ClientResponse
	err  error
	node string
}

// type PbftMsgAccepted struct {
// 	ret  *pb.PbftMsgAccepted
// 	err  error
// 	peer string
// }

func convertKeyValueToString(kv pb.KeyValue) string {
	key := kv.Key
	val := kv.Value
	return key + " = " + val
}

// call util.ClientRequestPBFT
// do grpc call to primary - succ or redirect
// while loop till f+1 correct responses from all nodes through some
// channel using grpc
// return with the result
func callCommand(primary string, kvc *KvStoreClient, primaryConn pb.PbftClient, clientResponseChan chan ClientResponse, command string, key string, value string) {
	log.Printf("callCommand for " + command + ":" + key + ":" + value)
	req := pb.ClientRequest{Operation: command + ":" + key + ":" + value, Timestamp: time.Now().UnixNano(), ClientID: "TheOneAndOnly"}
	log.Printf("req %v || reqOp %v reqT %v reqC %v", req, req.Operation, req.Timestamp, req.ClientID)
	ret, err := primaryConn.ClientRequestPBFT(context.Background(), &req)
	log.Printf("ret %v || err %v", ret, err)
	clientResponseChan <- ClientResponse{ret: ret, err: err, node: primary}
}

func waitForSufficientResponses(primary string, kvc *KvStoreClient, primaryConn pb.PbftClient, clientResponseChan chan ClientResponse, command string, key string, value string, pbft util.Pbft) string {
	log.Printf("waiting for " + command + ":" + key + ":" + value + " - commit from nodes")
	numberOfValidResponses := 0
	succ := <-clientResponseChan // Initial success message
	log.Printf("Initial succ recieved - %v sequenceID - %v", succ, succ.ret.SequenceID)
	ret := "Empty Response"
	for numberOfValidResponses < 2 {
		// pbftClr := <-clientResponseChan
		pbftClr := <-pbft.ClientRequestChan // This should be it
		if pbftClr.Arg.SequenceID == succ.ret.SequenceID {
			// Check if the request is for this particular seqId
			log.Printf("Recieved from %v", pbftClr.Arg.ClientID)
			ret = fmt.Sprintf("%v", pbftClr.Arg.NodeResult)
			// ret = convertKeyValueToString(pbftClr.Arg.NodeResult)
			numberOfValidResponses += 1
		}
		// Check something here and increment
		res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
		responseBack := pb.ClientResponse{ViewId: 0, Timestamp: time.Now().UnixNano(), ClientID: "TheOneAndOnly", Node: "TheOneAndOnly", NodeResult: &res, SequenceID: pbftClr.Arg.SequenceID}
		pbftClr.Response <- responseBack
	}
	return ret + " " + string(numberOfValidResponses)
}

type KvStoreClient struct {
	Store map[string]string
}

func main() {
	// func clientMain() {

	// var clientPort int
	var pbftPort int
	var primary string

	flag.IntVar(&pbftPort, "pbft", 3005, "Port on which client should listen to PBFT responses")
	flag.StringVar(&primary, "primary", "127.0.0.1:3001", "Pbft Primary")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	go util.RunPbftServer(&pbft, pbftPort)

	log.Printf("primary address - %v", primary)
	primaryConn, e := util.ConnectToPeer(primary)
	if e != nil {
		log.Fatal("Failed to connect to primary's GRPC - %v", e)
	}
	log.Printf("Connected to primary : primaryConn - %v", primaryConn)
	// for _, peer := range primaries {
	// 	primaryConn, err = util.ConnectToPeer(peer)
	// 	if err != nil {
	// 		log.Fatalf("Failed to connect to GRPC server %v", err)
	// 	}
	// 	log.Printf("Connected to %v", peer)
	// }

	// Initialize KVStore
	store := util.KVStore{C: make(chan util.InputChannelType), Store: make(map[string]string)}
	kvc := KvStoreClient{Store: make(map[string]string)}
	clientResponseChan := make(chan ClientResponse)
	// pbftMsgAcceptedChan := make(chan PbftMsgAccepted)

	// Is this really? Am I really using KVStore as grpc??
	pb.RegisterKvStoreServer(s, &store)
	// log.Printf("Going to listen on port %v", clientPort)
	// Start serving, this will block this function and only return when done.
	// if err := s.Serve(c); err != nil {
	// 	log.Fatalf("Failed to serve %v", err)
	// }

	welcome := Welcome{"Anonymous", time.Now().Format(time.Stamp), ""}
	templates := template.Must(template.ParseFiles("templates/welcome-template.html"))

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if name := r.FormValue("name"); name != "" {
			welcome.Name = name
		}
		log.Printf("Browser request - %v", r)
		command := r.FormValue("req")
		key := r.FormValue("key")
		value := r.FormValue("value")

		if command != "" && key != "" {
			// callCommand should be async
			go callCommand(primary, &kvc, primaryConn, clientResponseChan, command, key, value)

			log.Printf("waitForSufficientResponses")
			// waitForSufficientResponses should be sychronous
			stringToDisplay := waitForSufficientResponses(primary, &kvc, primaryConn, clientResponseChan, command, key, value, pbft)
			welcome.ReturnValue = stringToDisplay
			log.Printf("Should be committed - return to browser")
		}
		if err := templates.ExecuteTemplate(w, "welcome-template.html", welcome); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	fmt.Println("Listening on port 8080")
	fmt.Println(http.ListenAndServe(":8080", nil))
}

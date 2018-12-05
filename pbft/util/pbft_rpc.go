package util

import (
	"ProjectPBFT/pbft/pb"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ClientRequestInput struct {
	Arg      *pb.ClientRequest
	Response chan pb.ClientResponse
}

type PrePrepareMsgInput struct {
	Arg      *pb.PrePrepareMsg
	Response chan pb.PbftMsgAccepted
}

type PrepareMsgInput struct {
	Arg      *pb.PrepareMsg
	Response chan pb.PbftMsgAccepted
}

type CommitMsgInput struct {
	Arg      *pb.CommitMsg
	Response chan pb.PbftMsgAccepted
}

type Pbft struct {
	ClientRequestChan chan ClientRequestInput
	PrePrepareMsgChan chan PrePrepareMsgInput
	PrepareMsgChan    chan PrepareMsgInput
	CommitMsgChan     chan CommitMsgInput
}

func (r *Pbft) ClientRequestPBFT(ctx context.Context, arg *pb.ClientRequest) (*pb.ClientResponse, error) {
	c := make(chan pb.ClientResponse)
	r.ClientRequestChan <- ClientRequestInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) PrePreparePBFT(ctx context.Context, arg *pb.PrePrepareMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.PrePrepareMsgChan <- PrePrepareMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) PreparePBFT(ctx context.Context, arg *pb.PrepareMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.PrepareMsgChan <- PrepareMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) CommitPBFT(ctx context.Context, arg *pb.CommitMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.CommitMsgChan <- CommitMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func RandomDuration(r *rand.Rand) time.Duration {
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

func RestartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	if !stopped {
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
	timer.Reset(RandomDuration(r))
}

func RunPbftServer(r *Pbft, port int) {
	portString := fmt.Sprintf(":%d", port)
	c, err := net.Listen("tcp", portString)
	if err != nil {
		log.Fatalf("Could not create listening socket %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPbftServer(s, r)
	log.Printf("Going to listen on port %v", port)
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func ConnectToPeer(peer string) (pb.PbftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	if err != nil {
		return pb.NewPbftClient(nil), err
	}
	return pb.NewPbftClient(conn), nil
}

package util

import (
	"ProjectPBFT/pbft/pb"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type ViewChangeMsgInput struct {
	Arg      *pb.ViewChangeMsg
	Response chan pb.PbftMsgAccepted
}

type Pbft struct {
	ClientRequestChan chan ClientRequestInput
	PrePrepareMsgChan chan PrePrepareMsgInput
	PrepareMsgChan    chan PrepareMsgInput
	CommitMsgChan     chan CommitMsgInput
	ViewChangeMsgChan chan ViewChangeMsgInput
}

func (r *Pbft) ClientRequestPBFT(ctx context.Context, arg *pb.ClientRequest) (*pb.ClientResponse, error) {
	// log.Printf("Inside ClientRequestPBFT arg %v ", arg)
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

func (r *Pbft) ViewChangePBFT(ctx context.Context, arg *pb.ViewChangeMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.ViewChangeMsgChan <- ViewChangeMsgInput{Arg: arg, Response: c}
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

func StopTimer(timer *time.Timer) {
	stopped := timer.Stop()
	if !stopped {
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
}

type SecondsTimer struct {
	Timer *time.Timer
	End   time.Time
}

func NewSecondsTimer(t time.Duration) *SecondsTimer {
	return &SecondsTimer{time.NewTimer(t), time.Now().Add(t)}
}

func (s *SecondsTimer) Reset(t time.Duration) {
	stopped := s.Timer.Stop()
	if !stopped {
		for len(s.Timer.C) > 0 {
			<-s.Timer.C
		}
	}
	s.Timer.Reset(t)
	s.End = time.Now().Add(t)
}

func (s *SecondsTimer) Stop() {
	stopped := s.Timer.Stop()
	if !stopped {
		for len(s.Timer.C) > 0 {
			<-s.Timer.C
		}
	}
	s.Timer.Stop()
}

func (s *SecondsTimer) TimeRemaining() time.Duration {
	return s.End.Sub(time.Now())
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
	log.Printf("ConnectToPeer peer %v", peer)
	log.Printf("ConnectToPeer err %v", err)
	log.Printf("ConnectToPeer conn %v", conn)
	if err != nil {
		log.Printf("ConnectToPeer err %v", err)
		return pb.NewPbftClient(nil), err
	}
	return pb.NewPbftClient(conn), nil
}

func Hash(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

func Digest(object interface{}) string {
	msg, err := json.Marshal(object)
	if err != nil {
		// return "", err
		log.Fatal("Cannot make digest")
	}
	return Hash(msg)
}
